use crate::cluster::topology::{KeyspaceMetadata, Node, ReplicationStrategy};
use crate::cluster::Murmur3Token;
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;
use cassandra_protocol::consistency::Consistency;
use derivative::Derivative;
use fxhash::{FxHashMap, FxHashSet};
use itertools::Itertools;
use rand::prelude::*;
use rand::rng;
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
/// fashion. Local nodes are always placed before remote ones. If local nodes are present in the
/// cluster, only those will be used, unless a remote failover dc is allowed.
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

// Replicas for the given token in ring order, including ignored nodes.
fn network_topology_strategy_replica_nodes<T: CdrsTransport, CM: ConnectionManager<T>>(
    token: Murmur3Token,
    datacenter_replication_factor: &FxHashMap<String, usize>,
    cluster: &ClusterMetadata<T, CM>,
) -> Vec<Arc<Node<T, CM>>> {
    let mut remaining_replicas: FxHashMap<&str, usize> = datacenter_replication_factor
        .iter()
        .map(|(dc, replication_factor)| (dc.as_str(), *replication_factor))
        .collect();

    // replicas are never found in unknown datacenters, so don't wait for them before stopping
    let desired_replica_count = datacenter_replication_factor
        .iter()
        .filter(|(dc, _)| cluster.datacenter(dc).is_some())
        .map(|(_, replication_factor)| replication_factor)
        .sum();
    if desired_replica_count == 0 {
        return vec![];
    }

    let mut same_rack_replicas: FxHashMap<&str, usize> = datacenter_replication_factor
        .iter()
        .map(|(dc, replication_factor)| {
            let rack_count = cluster.datacenter(dc).map(|dc| dc.rack_count).unwrap_or(0);
            (dc.as_str(), replication_factor.saturating_sub(rack_count))
        })
        .collect();

    let mut result = Vec::with_capacity(desired_replica_count);
    let mut used_dc_racks: FxHashSet<(&str, &str)> = Default::default();

    for replica in cluster.token_map().distinct_nodes_for_token(token) {
        if let Some(datacenter_replication_factor) =
            remaining_replicas.get_mut(replica.datacenter())
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

    result
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
            QueryPlan::new(self.round_robin_unignored_local_nodes(cluster))
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> TopologyAwareLoadBalancingStrategy<T, CM> {
    /// Creates new strategy. The parameters determine if remote non-replica nodes should be
    /// considered in addition to replica ones, if `NetworkTopologyStrategy` is used for queried
    /// keyspace. `allow_dc_failover_for_local_cl` determines if remote nodes should be used for
    /// local-only consistency.
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
            QueryPlan::new(self.round_robin_unignored_local_nodes(cluster))
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
            .unwrap_or_else(|| QueryPlan::new(self.round_robin_unignored_local_nodes(cluster)))
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
                datacenter_replication_factor,
                consistency,
                cluster,
            ),
            ReplicationStrategy::Other => self.simple_strategy_replicas(token, 1, cluster),
        }
    }

    fn network_topology_strategy_replicas(
        &self,
        token: Murmur3Token,
        datacenter_replication_factor: &FxHashMap<String, usize>,
        consistency: Option<Consistency>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        // similar to Datastax BasicLoadBalancingPolicy:
        // 1. fetch all replicas
        // 2. extract unignored local
        // 3. shuffle replicas
        // 4. append round-robin unignored local non-replicas
        // 5. unless dc-local consistency is used without failover, append unignored remote
        //    replicas and, optionally, shuffled remote unignored non-replicas

        // Replica placement doesn't depend on node state, so ignored nodes are
        // removed only after placement - otherwise a non-replica would take
        // the place of an ignored replica.
        let replicas =
            network_topology_strategy_replica_nodes(token, datacenter_replication_factor, cluster);

        // unignored nodes always have a distance
        let (mut local_replicas, remote_replicas): (Vec<_>, Vec<_>) = replicas
            .into_iter()
            .filter(|node| !node.is_ignored())
            .partition(|node| node.is_local());

        let mut rng = rng();
        local_replicas.shuffle(&mut rng);

        let local_nodes = local_replicas
            .into_iter()
            .chain(self.round_robin_unignored_local_nodes(cluster));

        let is_dc_local = consistency
            .map(|consistency| consistency.is_dc_local())
            .unwrap_or(false);
        if is_dc_local && !self.allow_dc_failover_for_local_cl {
            return QueryPlan::new(
                local_nodes
                    .unique_by(|node| node.broadcast_rpc_address())
                    .collect(),
            );
        }

        let mut remote_nodes = self
            .max_nodes_per_remote_dc
            .map(|max_nodes_per_remote_dc| {
                cluster.unignored_remote_nodes_capped(max_nodes_per_remote_dc)
            })
            .unwrap_or_default();
        remote_nodes.shuffle(&mut rng);

        QueryPlan::new(
            local_nodes
                .chain(remote_replicas)
                .chain(remote_nodes)
                .unique_by(|node| node.broadcast_rpc_address())
                .collect(),
        )
    }

    fn simple_strategy_replicas(
        &self,
        token: Murmur3Token,
        replica_count: usize,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        // ignored replicas are skipped, not replaced by the next nodes on the ring, which aren't
        // replicas
        let mut replicas = cluster
            .token_map()
            .nodes_for_token_capped(token, replica_count)
            .filter(|node| !node.is_ignored())
            .collect_vec();

        replicas.shuffle(&mut rng());

        let unignored_nodes = self.round_robin_unignored_nodes(cluster);
        QueryPlan::new(
            replicas
                .into_iter()
                .chain(unignored_nodes)
                .unique_by(|node| node.broadcast_rpc_address())
                .collect(),
        )
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
    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::frame::Version;
    use fxhash::{FxHashMap, FxHashSet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, LazyLock};
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::{
        KeyspaceMetadata, Node, NodeDistance, NodeState, ReplicationStrategy,
    };
    use crate::cluster::ClusterMetadata;
    use crate::cluster::Murmur3Token;
    use crate::load_balancing::topology_aware::network_topology_strategy_replica_nodes;
    use crate::load_balancing::{
        LoadBalancingStrategy, Request, TopologyAwareLoadBalancingStrategy,
    };
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;
    use itertools::Itertools;

    static HOST_ID_1: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_2: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_3: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_4: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
    static HOST_ID_5: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

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

    type TestNode = Arc<Node<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>>;

    fn create_connection_pool_factory(
    ) -> Arc<ConnectionPoolFactory<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>>
    {
        let (_, keyspace_receiver) = watch::channel(None);
        Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            MockConnectionManager::<MockCdrsTransport>::new(),
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        ))
    }

    // the node is local if it's in dc1
    fn create_node(port: u16, state: NodeState, tokens: &[i64], rack: &str, dc: &str) -> TestNode {
        Arc::new(Node::new_with_state(
            create_connection_pool_factory(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            None,
            Some(Uuid::new_v4()),
            Some(if dc == "dc1" {
                NodeDistance::Local
            } else {
                NodeDistance::Remote
            }),
            state,
            tokens
                .iter()
                .map(|token| Murmur3Token::new(*token))
                .collect(),
            rack.into(),
            dc.into(),
        ))
    }

    fn create_nts_cluster(
        nodes: &[TestNode],
        datacenter_replication_factor: &[(&str, usize)],
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        create_test_cluster(
            nodes,
            ReplicationStrategy::NetworkTopologyStrategy {
                datacenter_replication_factor: datacenter_replication_factor
                    .iter()
                    .map(|(dc, replication_factor)| (dc.to_string(), *replication_factor))
                    .collect(),
            },
        )
    }

    fn create_test_cluster(
        nodes: &[TestNode],
        replication_strategy: ReplicationStrategy,
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        let mut keyspaces = FxHashMap::default();
        keyspaces.insert("ks".into(), KeyspaceMetadata::new(replication_strategy));

        ClusterMetadata::new(
            nodes
                .iter()
                .map(|node| (node.host_id().unwrap(), node.clone()))
                .collect(),
            keyspaces,
        )
    }

    fn query_plan_ports(
        lb: &TopologyAwareLoadBalancingStrategy<
            MockCdrsTransport,
            MockConnectionManager<MockCdrsTransport>,
        >,
        cluster: &ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>,
        token: i64,
        consistency: Option<Consistency>,
    ) -> Vec<u16> {
        lb.query_plan(
            Some(Request::new(
                Some("ks"),
                Some(Murmur3Token::new(token)),
                None,
                consistency,
            )),
            cluster,
        )
        .nodes
        .iter()
        .map(|node| node.broadcast_rpc_address().port())
        .collect()
    }

    #[test]
    fn should_shuffle_local_replicas_without_remote_ones() {
        let nodes = [
            create_node(1, NodeState::Up, &[0], "r1", "dc1"),
            create_node(2, NodeState::Up, &[10], "r2", "dc1"),
            create_node(3, NodeState::Up, &[20], "r3", "dc1"),
        ];
        let cluster = create_nts_cluster(&nodes, &[("dc1", 3)]);
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let first_nodes: FxHashSet<_> = (0..50)
            .map(|_| query_plan_ports(&lb, &cluster, 5, None)[0])
            .collect();

        assert!(first_nodes.len() > 1, "replicas were not shuffled");
    }

    fn create_multi_dc_vnode_cluster(
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        // deterministic pseudo-random tokens
        let mut state = 7u64;
        let mut next_tokens = move || {
            (0..16)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    state as i64
                })
                .collect_vec()
        };

        let mut nodes = vec![];
        let mut port = 0;
        for (dc, racks, node_count) in [
            ("dc1", &["r1", "r2", "r3"][..], 6),
            ("dc2", &["r1", "r2"][..], 4),
            ("dc3", &["r1"][..], 2),
        ]
        .iter()
        {
            for index in 0..*node_count {
                port += 1;
                let state = if port % 5 == 0 {
                    NodeState::Down
                } else {
                    NodeState::Up
                };
                nodes.push(create_node(
                    port,
                    state,
                    &next_tokens(),
                    racks[index % racks.len()],
                    dc,
                ));
            }
        }

        create_test_cluster(&nodes, ReplicationStrategy::Other)
    }

    fn reference_nts_replica_ports(
        token: Murmur3Token,
        datacenter_replication_factor: &[(&str, usize)],
        cluster: &ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>,
    ) -> Vec<u16> {
        let mut datacenter_replication_factor: FxHashMap<String, usize> =
            datacenter_replication_factor
                .iter()
                .map(|(dc, replication_factor)| (dc.to_string(), *replication_factor))
                .collect();

        let ring = cluster
            .nodes()
            .values()
            .flat_map(|node| node.tokens().iter().map(move |token| (*token, node)))
            .sorted_by_key(|(token, _)| *token)
            .collect_vec();
        let start = ring
            .iter()
            .position(|(ring_token, _)| *ring_token >= token)
            .unwrap_or(ring.len());
        let replicas = ring[start..]
            .iter()
            .chain(ring[..start].iter())
            .map(|(_, node)| *node)
            .unique_by(|node| node.broadcast_rpc_address())
            .collect_vec();

        let desired_replica_count = datacenter_replication_factor.values().sum();
        let mut same_rack_replicas: FxHashMap<String, usize> = datacenter_replication_factor
            .iter()
            .map(|(dc, replication_factor)| {
                let rack_count = cluster.datacenter(dc).map(|dc| dc.rack_count).unwrap_or(0);
                (dc.into(), replication_factor.saturating_sub(rack_count))
            })
            .collect();

        let mut result: Vec<&TestNode> = Vec::with_capacity(desired_replica_count);
        let mut used_dc_racks: FxHashSet<(&str, &str)> = Default::default();

        for replica in &replicas {
            if let Some(datacenter_replication_factor) =
                datacenter_replication_factor.get_mut(replica.datacenter())
            {
                if *datacenter_replication_factor == 0 {
                    continue;
                }

                let current_node_dc = replica.datacenter();
                let current_node_rack = replica.rack();

                if used_dc_racks.contains(&(current_node_dc, current_node_rack)) {
                    if let Some(same_rack_replicas) = same_rack_replicas.get_mut(current_node_dc) {
                        if *same_rack_replicas > 0 {
                            *same_rack_replicas -= 1;
                            *datacenter_replication_factor -= 1;
                            result.push(replica);
                        }
                    }
                } else {
                    *datacenter_replication_factor -= 1;

                    used_dc_racks.insert((current_node_dc, current_node_rack));
                    result.push(replica);
                }

                if result.len() == desired_replica_count {
                    break;
                }
            }
        }

        result
            .iter()
            .map(|node| node.broadcast_rpc_address().port())
            .collect()
    }

    #[test]
    fn should_place_nts_replicas_on_multi_dc_vnode_ring() {
        let cluster = create_multi_dc_vnode_cluster();

        let tokens = cluster
            .nodes()
            .values()
            .flat_map(|node| node.tokens().iter().copied())
            .chain((-10..10).map(|i| Murmur3Token::new(i * (i64::MAX / 10))))
            .collect_vec();

        let replication_factors: &[&[(&str, usize)]] = &[
            &[("dc1", 3), ("dc2", 2)],
            &[("dc1", 3), ("dc2", 3), ("dc3", 1)],
            &[("dc1", 2)],
            &[("dc1", 5)],
            &[("dc1", 7)],
            &[("dc3", 3)],
            &[("dc4", 1)],
            &[("dc1", 0), ("dc2", 1)],
            &[],
        ];

        for datacenter_replication_factor in replication_factors {
            let replication_factor_map = datacenter_replication_factor
                .iter()
                .map(|(dc, replication_factor)| (dc.to_string(), *replication_factor))
                .collect();

            for token in &tokens {
                let replicas = network_topology_strategy_replica_nodes(
                    *token,
                    &replication_factor_map,
                    &cluster,
                );

                assert_eq!(
                    replicas
                        .iter()
                        .map(|node| node.broadcast_rpc_address().port())
                        .collect_vec(),
                    reference_nts_replica_ports(*token, datacenter_replication_factor, &cluster),
                    "token: {:?}, replication: {:?}",
                    token,
                    datacenter_replication_factor
                );
            }
        }
    }

    // nodes 2 and 3 are the remaining replicas, node 4 is not a replica
    fn create_cluster_with_down_replica(
        replication_strategy: ReplicationStrategy,
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        create_test_cluster(
            &[
                create_node(1, NodeState::Down, &[0], "r1", "dc1"),
                create_node(2, NodeState::Up, &[10], "r2", "dc1"),
                create_node(3, NodeState::Up, &[20], "r3", "dc1"),
                create_node(4, NodeState::Up, &[30], "r4", "dc1"),
            ],
            replication_strategy,
        )
    }

    // node 1 is the dc1 replica, node 3 is the dc2 replica
    fn create_cluster_with_down_local_replica(
    ) -> ClusterMetadata<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>> {
        create_nts_cluster(
            &[
                create_node(1, NodeState::Down, &[0], "r1", "dc1"),
                create_node(2, NodeState::Up, &[10], "r1", "dc1"),
                create_node(3, NodeState::Up, &[5], "r1", "dc2"),
                create_node(4, NodeState::Up, &[15], "r1", "dc2"),
            ],
            &[("dc1", 1), ("dc2", 1)],
        )
    }

    #[test]
    fn should_not_use_remote_nodes_for_dc_local_consistency() {
        let cluster = create_cluster_with_down_local_replica();

        for consistency in [Consistency::LocalOne, Consistency::LocalQuorum].iter() {
            let lb = TopologyAwareLoadBalancingStrategy::new(Some(2), false);
            assert_eq!(
                query_plan_ports(&lb, &cluster, 0, Some(*consistency)),
                vec![2]
            );

            let lb = TopologyAwareLoadBalancingStrategy::new(None, false);
            assert_eq!(
                query_plan_ports(&lb, &cluster, 0, Some(*consistency)),
                vec![2]
            );
        }
    }

    #[test]
    fn should_prefer_local_non_replicas_over_remote_replicas() {
        let cluster = create_cluster_with_down_local_replica();

        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);
        assert_eq!(query_plan_ports(&lb, &cluster, 0, None), vec![2, 3]);

        let lb = TopologyAwareLoadBalancingStrategy::new(Some(2), true);
        assert_eq!(
            query_plan_ports(&lb, &cluster, 0, Some(Consistency::LocalOne)),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn should_not_return_duplicates_for_dc_local_consistency() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(Some(5), false);

        let query_plan = lb.query_plan(
            Some(Request::new(
                Some("k4"),
                Some(Murmur3Token::new(2)),
                None,
                Some(Consistency::LocalQuorum),
            )),
            &cluster,
        );

        let unique_addresses: FxHashSet<_> = query_plan
            .nodes
            .iter()
            .map(|node| node.broadcast_rpc_address())
            .collect();
        assert_eq!(unique_addresses.len(), query_plan.nodes.len());
    }

    #[test]
    fn should_not_promote_non_replica_in_place_of_down_nts_replica() {
        let cluster =
            create_cluster_with_down_replica(ReplicationStrategy::NetworkTopologyStrategy {
                datacenter_replication_factor: std::iter::once(("dc1".to_string(), 3)).collect(),
            });
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        for _ in 0..30 {
            assert_eq!(query_plan_ports(&lb, &cluster, 0, None)[2], 4);
        }
    }

    #[test]
    fn should_not_promote_non_replica_in_place_of_down_simple_strategy_replica() {
        let cluster = create_cluster_with_down_replica(ReplicationStrategy::SimpleStrategy {
            replication_factor: 3,
        });
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        for _ in 0..30 {
            assert_eq!(query_plan_ports(&lb, &cluster, 0, None)[2], 4);
        }
    }

    #[test]
    fn should_return_nodes_without_request() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(None, &cluster);
        assert_eq!(query_plan.nodes.len(), 3);

        for node in &query_plan.nodes {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_local_nodes_without_request() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(None, &cluster);
        assert_eq!(query_plan.nodes.len(), 3);

        for node in &query_plan.nodes {
            assert!(node.is_local());
        }
    }

    #[test]
    fn should_return_local_nodes_without_token() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let query_plan = lb.query_plan(Some(Request::new(None, None, None, None)), &cluster);
        assert_eq!(query_plan.nodes.len(), 3);

        for node in &query_plan.nodes {
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
        assert_eq!(query_plan.nodes.len(), 3);

        for node in &query_plan.nodes {
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
        assert_eq!(query_plan.nodes.len(), 5); // 1 replica + 4 unignored
        assert_eq!(query_plan.nodes[0].host_id().unwrap(), *HOST_ID_2);
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

        assert_eq!(query_plan.nodes.len(), 5); // 2 replicas + 3 unignored
        assert!(
            query_plan.nodes[0].host_id().unwrap() == *HOST_ID_2
                || query_plan.nodes[0].host_id().unwrap() == *HOST_ID_4
        );
        assert!(
            query_plan.nodes[1].host_id().unwrap() == *HOST_ID_2
                || query_plan.nodes[1].host_id().unwrap() == *HOST_ID_4
        );
        assert!(query_plan.nodes.iter().all(|node| !node.is_ignored()));
    }

    // The test cluster has two ignored nodes at tokens 8 and 9, which are the
    // only replicas for token 8 with RF=2 (keyspace k1). The next nodes on the
    // ring aren't replicas, so the plan is exactly the round-robin order of the
    // unignored nodes, with no node promoted to the front.
    #[test]
    fn simple_strategy_skips_ignored_replicas() {
        let cluster = create_cluster();
        let lb = TopologyAwareLoadBalancingStrategy::new(None, false);

        let unignored_nodes = cluster
            .unignored_nodes()
            .iter()
            .map(|node| node.broadcast_rpc_address())
            .collect_vec();
        assert_eq!(unignored_nodes.len(), 5);

        for offset in 0..unignored_nodes.len() {
            let query_plan = lb.query_plan(
                Some(Request::new(
                    Some("k1"), // SimpleStrategy with replication_factor: 2
                    Some(Murmur3Token::new(8)),
                    None,
                    None,
                )),
                &cluster,
            );

            let mut expected = unignored_nodes.clone();
            expected.rotate_left(offset);
            assert_eq!(
                query_plan
                    .nodes
                    .iter()
                    .map(|node| node.broadcast_rpc_address())
                    .collect_vec(),
                expected
            );
        }
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
        assert_eq!(query_plan.nodes.len(), 4);
        assert!(
            query_plan.nodes[0].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[0].host_id().unwrap() == *HOST_ID_2
                || query_plan.nodes[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan.nodes[1].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[1].host_id().unwrap() == *HOST_ID_2
                || query_plan.nodes[1].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan.nodes[2].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[2].host_id().unwrap() == *HOST_ID_2
                || query_plan.nodes[2].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan.nodes[3].host_id().unwrap(), *HOST_ID_4);
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

        assert_eq!(query_plan.nodes.len(), 4);
        assert!(
            query_plan.nodes[0].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan.nodes[1].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[1].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan.nodes[2].host_id().unwrap(), *HOST_ID_2);
        assert_eq!(query_plan.nodes[3].host_id().unwrap(), *HOST_ID_4);
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

        assert_eq!(query_plan.nodes.len(), 5);
        assert!(
            query_plan.nodes[0].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[0].host_id().unwrap() == *HOST_ID_3
        );
        assert!(
            query_plan.nodes[1].host_id().unwrap() == *HOST_ID_1
                || query_plan.nodes[1].host_id().unwrap() == *HOST_ID_3
        );
        assert_eq!(query_plan.nodes[2].host_id().unwrap(), *HOST_ID_2);
        assert_eq!(query_plan.nodes[3].host_id().unwrap(), *HOST_ID_4);
        assert_eq!(query_plan.nodes[4].host_id().unwrap(), *HOST_ID_5);
    }
}
