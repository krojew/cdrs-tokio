use crate::cluster::topology::{KeyspaceMetadata, Node, NodeDistance, ReplicationStrategy};
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::token::Murmur3Token;
use derivative::Derivative;
use fxhash::{FxHashMap, FxHashSet};
use itertools::Itertools;
use rand::prelude::*;
use std::cmp::Ordering as CmpOrdering;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Topology-aware load balancing strategy. Depends on up-to-date topology information, which is
/// constantly monitored in the background by a control connection. For best results, a
/// topology-aware [`NodeDistanceEvaluator`](crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator) (e.g.
/// [`TopologyAwareNodeDistanceEvaluator`](crate::load_balancing::node_distance_evaluator::TopologyAwareNodeDistanceEvaluator))
/// should also be used.
///
/// This implementation prioritizes replica nodes over non-replica ones; if more than one replica
/// is available, the replicas will be shuffled. Non-replica nodes will be included in a round-robin
/// fashion. If local nodes are present in the cluster, only those will be used, unless a remote
/// failover dc is allowed.
///
/// Note: if a referenced keyspace doesn't use `NetworkTopologyStrategy`, replica nodes will be
/// chosen ignoring distance information.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct TopologyAwareLoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    max_nodes_per_remote_dc: Option<usize>,
    allow_dc_failover_for_local_cl: bool,
    prev_idx: AtomicUsize,
    #[derivative(Debug = "ignore")]
    _transport: PhantomData<T>,
    #[derivative(Debug = "ignore")]
    _connection_manager: PhantomData<CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for TopologyAwareLoadBalancingStrategy<T, CM>
{
    fn query_plan(
        &self,
        request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        if let Some(request) = request {
            self.replicas_for_request(request, cluster)
        } else {
            self.round_robin_unignored_local_nodes(cluster)
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> TopologyAwareLoadBalancingStrategy<T, CM> {
    /// Creates new strategy. The parameters determine if remote non-replica nodes should be
    /// considered in addition to replica ones, if `NetworkTopologyStrategy` is used for queried
    /// keyspace. `allow_dc_failover_for_local_cl` determines if remote non-replicas should be
    /// used to local-only consistency.
    pub fn new(
        max_nodes_per_remote_dc: Option<usize>,
        allow_dc_failover_for_local_cl: bool,
    ) -> Self {
        TopologyAwareLoadBalancingStrategy {
            max_nodes_per_remote_dc,
            allow_dc_failover_for_local_cl,
            prev_idx: AtomicUsize::new(0),
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }

    fn replicas_for_request(
        &self,
        request: Request,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        let token = request
            .token
            .or_else(|| request.routing_key.map(Murmur3Token::generate));

        if let Some(token) = token {
            self.replicas_for_token(token, request.keyspace, request.consistency, cluster)
        } else {
            self.round_robin_unignored_local_nodes(cluster)
        }
    }

    fn replicas_for_token(
        &self,
        token: Murmur3Token,
        keyspace: Option<&str>,
        consistency: Option<Consistency>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        keyspace
            .and_then(|keyspace| cluster.keyspace(keyspace))
            .map(|keyspace| self.replicas_for_keyspace(token, keyspace, consistency, cluster))
            .unwrap_or_else(|| self.round_robin_unignored_local_nodes(cluster))
    }

    fn replicas_for_keyspace(
        &self,
        token: Murmur3Token,
        keyspace: &KeyspaceMetadata,
        consistency: Option<Consistency>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        match &keyspace.replication_strategy {
            ReplicationStrategy::SimpleStrategy { replication_factor } => {
                self.simple_strategy_replicas(token, *replication_factor, cluster)
            }
            ReplicationStrategy::NetworkTopologyStrategy {
                datacenter_replication_factor,
            } => self.network_topology_strategy_replicas(
                token,
                datacenter_replication_factor.clone(),
                consistency,
                cluster,
            ),
            ReplicationStrategy::Other => self.simple_strategy_replicas(token, 1, cluster),
        }
    }

    fn network_topology_strategy_replicas(
        &self,
        token: Murmur3Token,
        mut datacenter_replication_factor: FxHashMap<String, usize>,
        consistency: Option<Consistency>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        // similar to Datastax BasicLoadBalancingPolicy:
        // 1. fetch all replicas
        // 2. extract unignored local
        // 3. shuffle replicas
        // 4. append round-robin unignored local non-replicas
        // 5. optionally, add shuffled remote unignored non-replicas

        let replicas = cluster.token_map().nodes_for_token(token).collect_vec();

        let desired_replica_count = datacenter_replication_factor.values().sum();
        let mut same_rack_replicas: FxHashMap<String, usize> = datacenter_replication_factor
            .iter()
            .map(|(dc, replication_factor)| {
                let rack_count = cluster.datacenter(dc).map(|dc| dc.rack_count).unwrap_or(0);
                (dc.into(), replication_factor.saturating_sub(rack_count))
            })
            .collect();

        let mut result = Vec::with_capacity(desired_replica_count);
        let mut used_dc_racks: FxHashSet<(&str, &str)> = Default::default();

        for replica in &replicas {
            if let Some(datacenter_replication_factor) =
                datacenter_replication_factor.get_mut(replica.datacenter())
            {
                if *datacenter_replication_factor == 0 {
                    // found enough nodes in this datacenter
                    continue;
                }

                let current_node_dc = replica.datacenter();
                let current_node_rack = replica.rack();

                if used_dc_racks.contains(&(current_node_dc, current_node_rack)) {
                    // check if we need to put nodes from the same rack multiple times to meet
                    // the replication factor
                    if let Some(same_rack_replicas) = same_rack_replicas.get_mut(current_node_dc) {
                        if *same_rack_replicas > 0 {
                            *same_rack_replicas -= 1;
                            *datacenter_replication_factor -= 1;
                            result.push(replica.clone());
                        }
                    }
                } else {
                    *datacenter_replication_factor -= 1;

                    used_dc_racks.insert((current_node_dc, current_node_rack));
                    result.push(replica.clone());
                }

                if result.len() == desired_replica_count {
                    break;
                }
            }
        }

        // result now contains mixed local/remote and ignored/unignored nodes - put local in front
        result.sort_unstable_by(|a, b| {
            let a_distance = a.distance();
            let b_distance = b.distance();

            if a_distance == b_distance {
                return CmpOrdering::Equal;
            }

            if a_distance == Some(NodeDistance::Local) {
                return CmpOrdering::Less;
            }
            if b_distance == Some(NodeDistance::Local) {
                return CmpOrdering::Greater;
            }

            CmpOrdering::Equal
        });

        // remove ignored
        result.retain(|node| !node.is_ignored());

        let mut rng = thread_rng();

        // find now many local nodes we have
        let local_count = result.iter().position(|node| node.is_remote()).unwrap_or(0);
        if local_count > 0 {
            result[..local_count].shuffle(&mut rng);
        }

        // add unignored non-replicas
        let unignored_nodes = self.round_robin_unignored_local_nodes(cluster);
        let replicas = result.into_iter().chain(unignored_nodes.into_iter());

        // now the result contains (in order): local replicas, remote replicas, local non-replicas
        if let Some(max_nodes_per_remote_dc) = self.max_nodes_per_remote_dc {
            if let Some(consistency) = consistency {
                if !self.allow_dc_failover_for_local_cl && consistency.is_dc_local() {
                    return replicas.collect();
                }
            }

            let mut remote_nodes = cluster.unignored_remote_nodes_capped(max_nodes_per_remote_dc);
            remote_nodes.shuffle(&mut rng);

            replicas
                .chain(remote_nodes.into_iter())
                .unique_by(|node| node.broadcast_rpc_address())
                .collect()
        } else {
            replicas
                .unique_by(|node| node.broadcast_rpc_address())
                .collect()
        }
    }

    fn simple_strategy_replicas(
        &self,
        token: Murmur3Token,
        replica_count: usize,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        let mut replicas = cluster
            .token_map()
            .nodes_for_token_capped(token, replica_count)
            .filter(|node| !node.is_ignored())
            .collect_vec();

        replicas.shuffle(&mut thread_rng());

        let unignored_nodes = self.round_robin_unignored_nodes(cluster);
        replicas
            .into_iter()
            .chain(unignored_nodes.into_iter())
            .unique_by(|node| node.broadcast_rpc_address())
            .collect()
    }

    fn round_robin_unignored_nodes(
        &self,
        cluster: &ClusterMetadata<T, CM>,
    ) -> Vec<Arc<Node<T, CM>>> {
        let mut nodes = cluster.unignored_nodes();
        if nodes.is_empty() {
            return nodes;
        }

        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst) % nodes.len();

        nodes.rotate_left(cur_idx);
        nodes
    }

    fn round_robin_unignored_local_nodes(
        &self,
        cluster: &ClusterMetadata<T, CM>,
    ) -> Vec<Arc<Node<T, CM>>> {
        let mut nodes = cluster.unignored_local_nodes();
        if nodes.is_empty() {
            return nodes;
        }

        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst) % nodes.len();

        nodes.rotate_left(cur_idx);
        nodes
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::token::Murmur3Token;
    use fxhash::FxHashMap;
    use lazy_static::lazy_static;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::{
        KeyspaceMetadata, Node, NodeDistance, NodeState, ReplicationStrategy,
    };
    use crate::cluster::ClusterMetadata;
    use crate::load_balancing::{
        LoadBalancingStrategy, Request, TopologyAwareLoadBalancingStrategy,
    };
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;

    lazy_static! {
        static ref HOST_ID_1: Uuid = Uuid::new_v4();
        static ref HOST_ID_2: Uuid = Uuid::new_v4();
        static ref HOST_ID_3: Uuid = Uuid::new_v4();
        static ref HOST_ID_4: Uuid = Uuid::new_v4();
        static ref HOST_ID_5: Uuid = Uuid::new_v4();
    }

    fn create_cluster(
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        let reconnection_policy = MockReconnectionPolicy::new();
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(reconnection_policy),
        ));

        let mut nodes = FxHashMap::default();
        nodes.insert(
            *HOST_ID_1,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 1),
                None,
                Some(*HOST_ID_1),
                Some(NodeDistance::Local),
                NodeState::Up,
                vec![Murmur3Token::new(1), Murmur3Token::new(2)],
                "r1".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_2,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 2),
                None,
                Some(*HOST_ID_2),
                Some(NodeDistance::Local),
                NodeState::Up,
                vec![Murmur3Token::new(3), Murmur3Token::new(4)],
                "r1".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_3,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 3),
                None,
                Some(*HOST_ID_3),
                Some(NodeDistance::Local),
                NodeState::Up,
                vec![Murmur3Token::new(7)],
                "r2".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 4),
                None,
                None,
                None,
                NodeState::Up,
                vec![Murmur3Token::new(8)],
                "r2".into(),
                "dc1".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_4,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 5),
                None,
                Some(*HOST_ID_4),
                Some(NodeDistance::Remote),
                NodeState::Up,
                vec![Murmur3Token::new(5), Murmur3Token::new(6)],
                "r1".into(),
                "dc2".into(),
            )),
        );
        nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6),
                None,
                None,
                None,
                NodeState::Up,
                vec![Murmur3Token::new(9)],
                "r1".into(),
                "dc2".into(),
            )),
        );
        nodes.insert(
            *HOST_ID_5,
            Arc::new(Node::new_with_state(
                connection_pool_factory,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 7),
                None,
                Some(*HOST_ID_5),
                Some(NodeDistance::Remote),
                NodeState::Up,
                vec![Murmur3Token::new(0)],
                "r2".into(),
                "dc2".into(),
            )),
        );

        let mut datacenter_replication_factor_2 = FxHashMap::default();
        datacenter_replication_factor_2.insert("dc1".into(), 3);
        datacenter_replication_factor_2.insert("dc2".into(), 1);

        let mut datacenter_replication_factor_4 = FxHashMap::default();
        datacenter_replication_factor_4.insert("dc1".into(), 2);
        datacenter_replication_factor_4.insert("dc2".into(), 1);

        let mut keyspaces = FxHashMap::default();
        keyspaces.insert(
            "k1".into(),
            KeyspaceMetadata::new(ReplicationStrategy::SimpleStrategy {
                replication_factor: 2,
            }),
        );
        keyspaces.insert(
            "k2".into(),
            KeyspaceMetadata::new(ReplicationStrategy::NetworkTopologyStrategy {
                datacenter_replication_factor: datacenter_replication_factor_2,
            }),
        );
        keyspaces.insert(
            "k3".into(),
            KeyspaceMetadata::new(ReplicationStrategy::Other),
        );
        keyspaces.insert(
            "k4".into(),
            KeyspaceMetadata::new(ReplicationStrategy::NetworkTopologyStrategy {
                datacenter_replication_factor: datacenter_replication_factor_4,
            }),
        );

        ClusterMetadata::new(nodes, keyspaces)
    }

    #[test]
    fn should_return_nodes_without_request() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(None, &cluster);
        assert_eq!(query_plan.len(), 3);

        for node in &query_plan {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_local_nodes_without_request() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(None, &cluster);
        assert_eq!(query_plan.len(), 3);

        for node in &query_plan {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_local_nodes_without_token() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(Some(Request::new(None, None, None, None)), &cluster);
        assert_eq!(query_plan.len(), 3);

        for node in &query_plan {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_local_nodes_without_keyspace() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(
            Some(Request::new(None, Some(Murmur3Token::new(4)), None, None)),
            &cluster,
        );
        assert_eq!(query_plan.len(), 3);

        for node in &query_plan {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_all_nodes_with_unknown_strategy() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k3"),
                Some(Murmur3Token::new(4)),
                None,
                None,
            )),
            &cluster,
        );
        assert_eq!(query_plan.len(), 5); // 1 replica + 4 unignored
        assert_eq!(query_plan[0].host_id().unwrap(), *HOST_ID_2);
    }

    #[test]
    fn should_return_replica_nodes_with_simple_strategy() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k1"),
                Some(Murmur3Token::new(4)),
                None,
                None,
            )),
            &cluster,
        );

        assert_eq!(query_plan.len(), 5); // 2 replicas + 3 unignored
        assert!(
            query_plan[0].host_id().unwrap() == *HOST_ID_2
                || query_plan[0].host_id().unwrap() == *HOST_ID_4
        );
        assert!(
            query_plan[1].host_id().unwrap() == *HOST_ID_2
                || query_plan[1].host_id().unwrap() == *HOST_ID_4
        );
        assert!(query_plan.iter().all(|node| !node.is_ignored()));
    }

    #[test]
    fn should_return_topology_aware_nodes_with_network_topology_strategy_with_repeated_racks() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k2"),
                Some(Murmur3Token::new(2)),
                None,
                None,
            )),
            &cluster,
        );
        assert_eq!(query_plan.len(), 4);
        assert!(
            query_plan[0].host_id().unwrap() == *HOST_ID_1
                || query_plan[0].host_id().unwrap() == *HOST_ID_2
                || query_plan[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan[1].host_id().unwrap() == *HOST_ID_1
                || query_plan[1].host_id().unwrap() == *HOST_ID_2
                || query_plan[1].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan[2].host_id().unwrap() == *HOST_ID_1
                || query_plan[2].host_id().unwrap() == *HOST_ID_2
                || query_plan[2].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan[3].host_id().unwrap(), *HOST_ID_4);
    }

    #[test]
    fn should_return_topology_aware_nodes_with_network_topology_strategy() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k4"),
                Some(Murmur3Token::new(2)),
                None,
                None,
            )),
            &cluster,
        );

        assert_eq!(query_plan.len(), 4);
        assert!(
            query_plan[0].host_id().unwrap() == *HOST_ID_1
                || query_plan[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan[1].host_id().unwrap() == *HOST_ID_1
                || query_plan[1].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan[2].host_id().unwrap(), *HOST_ID_4);
        assert_eq!(query_plan[3].host_id().unwrap(), *HOST_ID_2);
    }

    #[test]
    fn should_return_topology_aware_nodes_with_network_topology_strategy_with_remote() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(Some(5), false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k4"),
                Some(Murmur3Token::new(2)),
                None,
                None,
            )),
            &cluster,
        );

        assert_eq!(query_plan.len(), 5);
        assert!(
            query_plan[0].host_id().unwrap() == *HOST_ID_1
                || query_plan[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan[1].host_id().unwrap() == *HOST_ID_1
                || query_plan[1].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan[2].host_id().unwrap(), *HOST_ID_4);
        assert_eq!(query_plan[3].host_id().unwrap(), *HOST_ID_2);
        assert_eq!(query_plan[4].host_id().unwrap(), *HOST_ID_5);
    }
}
