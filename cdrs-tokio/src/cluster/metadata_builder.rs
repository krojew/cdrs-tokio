use fxhash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tracing::*;

use crate::cluster::connection_pool::ConnectionPoolFactory;
use crate::cluster::topology::{KeyspaceMetadata, Node, NodeState};
use crate::cluster::{ClusterMetadata, ConnectionManager, NodeInfo};
use crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use crate::transport::CdrsTransport;

pub(crate) fn build_initial_metadata<T: CdrsTransport, CM: ConnectionManager<T>>(
    node_infos: Vec<NodeInfo>,
    keyspaces: FxHashMap<String, KeyspaceMetadata>,
    contact_points: &[Arc<Node<T, CM>>],
    old_metadata: Option<&ClusterMetadata<T, CM>>,
    connection_pool_factory: &Arc<ConnectionPoolFactory<T, CM>>,
    node_distance_evaluator: &(dyn NodeDistanceEvaluator + Send + Sync),
) -> ClusterMetadata<T, CM> {
    let mut nodes = FxHashMap::with_capacity_and_hasher(node_infos.len(), Default::default());
    for node_info in node_infos {
        if let Entry::Vacant(entry) = nodes.entry(node_info.host_id) {
            let distance = node_distance_evaluator.compute_distance(&node_info);

            // A known node keeps its connectivity if its address and distance, which determines
            // the pool size, are unchanged.
            let old_node = old_metadata
                .and_then(|old_metadata| old_metadata.nodes().get(&node_info.host_id))
                .filter(|old_node| {
                    old_node.broadcast_rpc_address() == node_info.broadcast_rpc_address
                        && old_node.distance() == distance
                });

            let node = match old_node {
                // Listed in the peers table, so assumed to be up. A forced down node cannot leave
                // that state in place, so it gets fresh connectivity instead. The caller may retry
                // building inside an rcu closure, and setting the state is idempotent.
                Some(old_node) if old_node.try_set_state(NodeState::Up) => {
                    debug!(?node_info, "Reusing old node.");
                    Arc::new(old_node.clone_with_node_info(node_info, node_distance_evaluator))
                }
                _ => {
                    let contact_point = contact_points.iter().find(|contact_point| {
                        contact_point.broadcast_rpc_address() == node_info.broadcast_rpc_address
                    });

                    if let Some(contact_point) = contact_point {
                        debug!(?node_info, "Copying contact point.");
                        Arc::new(
                            contact_point
                                .clone_as_contact_point(node_info, node_distance_evaluator),
                        )
                    } else {
                        debug!(?node_info, "Adding new node.");
                        Arc::new(Node::new_with_state(
                            connection_pool_factory.clone(),
                            node_info.broadcast_rpc_address,
                            node_info.broadcast_address,
                            Some(node_info.host_id),
                            distance,
                            NodeState::Up,
                            node_info.tokens,
                            node_info.rack,
                            node_info.datacenter,
                        ))
                    }
                }
            };

            entry.insert(node);
        } else {
            warn!(
                host_id = %node_info.host_id,
                "Found duplicate peer entries - keeping only the first one."
            );
        }
    }

    ClusterMetadata::new(nodes, keyspaces)
}

pub(crate) fn refresh_metadata<T: CdrsTransport, CM: ConnectionManager<T>>(
    node_infos: &[NodeInfo],
    keyspaces: FxHashMap<String, KeyspaceMetadata>,
    old_metadata: &ClusterMetadata<T, CM>,
    connection_pool_factory: &Arc<ConnectionPoolFactory<T, CM>>,
    node_distance_evaluator: &dyn NodeDistanceEvaluator,
) -> ClusterMetadata<T, CM> {
    let old_nodes = old_metadata.nodes();

    let mut seen_hosts = FxHashSet::default();
    let mut added_or_updated = FxHashMap::default();

    for node_info in node_infos {
        if seen_hosts.contains(&node_info.host_id) {
            warn!(
                host_id = %node_info.host_id,
                "Found duplicate peer entries - keeping only the first one."
            );
        } else {
            seen_hosts.insert(node_info.host_id);

            let old_node = old_nodes.get(&node_info.host_id);
            if let Some(old_node) = old_node {
                debug!(?node_info, "Updating old node.");
                added_or_updated.insert(
                    node_info.host_id,
                    Arc::new(
                        old_node.clone_with_node_info(node_info.clone(), node_distance_evaluator),
                    ),
                );
            } else {
                debug!(?node_info, "Adding new node.");

                // Present in the peers table, so assume it's up, as in the initial metadata. An UP
                // event, which would otherwise promote it, isn't sent for a node that is already up.
                let node = Arc::new(Node::new_with_state(
                    connection_pool_factory.clone(),
                    node_info.broadcast_rpc_address,
                    node_info.broadcast_address,
                    Some(node_info.host_id),
                    node_distance_evaluator.compute_distance(node_info),
                    NodeState::Up,
                    node_info.tokens.clone(),
                    node_info.rack.clone(),
                    node_info.datacenter.clone(),
                ));

                added_or_updated.insert(node_info.host_id, node);
            }
        }
    }

    ClusterMetadata::new(added_or_updated, keyspaces)
}

/// Returns `None` if the metadata doesn't need to change.
pub(crate) fn add_new_node<T: CdrsTransport, CM: ConnectionManager<T>>(
    node_info: NodeInfo,
    old_metadata: &ClusterMetadata<T, CM>,
    connection_pool_factory: &Arc<ConnectionPoolFactory<T, CM>>,
    node_distance_evaluator: &dyn NodeDistanceEvaluator,
    state: NodeState,
) -> Option<ClusterMetadata<T, CM>> {
    let old_node = old_metadata.find_node_by_host_id(&node_info.host_id);
    if let Some(old_node) = old_node {
        // If a node is restarted after changing its broadcast RPC address, Cassandra considers that
        // an addition, even though the host_id hasn't changed :(
        if old_node.broadcast_rpc_address() == node_info.broadcast_rpc_address {
            // A refresh may have added the node, as up, while its info was being fetched. The
            // event's state is applied only when it promotes the node to up, so an up node is never
            // demoted and ignored.
            if state != NodeState::Up || old_node.state() == NodeState::Up {
                debug!(?old_node, "Ignoring adding an existing node.");
                return None;
            }

            // The state changes in place, which every node sharing its connectivity sees. Only a
            // forced down node needs fresh connectivity.
            if old_node.try_set_state(NodeState::Up) {
                debug!(?old_node, "Set existing node state to up.");
                return None;
            }

            debug!(?old_node, "Recreating forced down node as up.");
            return Some(
                old_metadata.clone_with_node(old_node.clone_with_node_state(NodeState::Up)),
            );
        }

        debug!(?old_node, "Updating old node with new info.");
        return Some(
            old_metadata.clone_with_node(old_node.clone_with_node_info_and_state(
                node_info,
                node_distance_evaluator,
                state,
            )),
        );
    }

    Some(old_metadata.clone_with_node(Node::new_with_state(
        connection_pool_factory.clone(),
        node_info.broadcast_rpc_address,
        node_info.broadcast_address,
        Some(node_info.host_id),
        node_distance_evaluator.compute_distance(&node_info),
        state,
        node_info.tokens,
        node_info.rack,
        node_info.datacenter,
    )))
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use cassandra_protocol::error::Error;
    use cassandra_protocol::frame::Version;
    use futures::future::ready;
    use futures::FutureExt;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};
    use tokio::time::timeout;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::metadata_builder::{
        add_new_node, build_initial_metadata, refresh_metadata,
    };
    use crate::cluster::test_support::{
        self, node_address, void_response, TestConnectionManager, TestNode, TestTransport,
    };
    use crate::cluster::topology::NodeMap;
    use crate::cluster::topology::{
        KeyspaceMetadata, Node, NodeDistance, NodeState, ReplicationStrategy,
    };
    use crate::cluster::{ClusterMetadata, NodeInfo};
    use crate::load_balancing::node_distance_evaluator::MockNodeDistanceEvaluator;
    use crate::retry::{ConstantReconnectionPolicy, MockReconnectionPolicy};
    use crate::transport::MockCdrsTransport;
    use fxhash::FxHashMap;

    // upper bound for awaiting outcomes which are expected to happen right away
    const TEST_GUARD_TIMEOUT: Duration = Duration::from_secs(5);

    fn create_connection_pool_factory(
    ) -> Arc<ConnectionPoolFactory<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>>
    {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        let reconnection_policy = MockReconnectionPolicy::new();
        let connection_pool_factory = ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            Arc::new(reconnection_policy),
        );

        Arc::new(connection_pool_factory)
    }

    #[test]
    fn should_create_initial_metadata_from_all_new_nodes() {
        let node_infos = vec![NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(None);

        let metadata = build_initial_metadata(
            node_infos.clone(),
            Default::default(),
            &[],
            None,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_infos[0].host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_infos[0].broadcast_rpc_address
        );
    }

    #[test]
    fn should_copy_old_node() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Local));

        let node_infos = vec![NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let contact_points = [Arc::new(
            Node::new(
                connection_pool_factory.clone(),
                node_infos[0].broadcast_rpc_address,
                node_infos[0].broadcast_address,
                Some(node_infos[0].host_id),
                None,
                Default::default(),
                "".into(),
                "".into(),
            )
            .clone_with_node_state(NodeState::Up),
        )];

        let metadata = build_initial_metadata(
            node_infos.clone(),
            Default::default(),
            &contact_points,
            None,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes.get(&node_infos[0].host_id).unwrap().state(),
            NodeState::Up
        );
    }

    #[test]
    fn should_compute_contact_point_distance() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Remote));

        let node_infos = vec![NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
            None,
            "dc2".into(),
            Default::default(),
            "".into(),
        )];

        let contact_points = [Arc::new(Node::new_with_state(
            connection_pool_factory.clone(),
            node_infos[0].broadcast_rpc_address,
            None,
            None,
            Some(NodeDistance::Local),
            NodeState::Up,
            Default::default(),
            "".into(),
            "".into(),
        ))];

        let metadata = build_initial_metadata(
            node_infos.clone(),
            Default::default(),
            &contact_points,
            None,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let node = metadata
            .nodes()
            .get(&node_infos[0].host_id)
            .unwrap()
            .clone();
        assert_eq!(node.distance(), Some(NodeDistance::Remote));
        assert_eq!(node.state(), NodeState::Up);
    }

    #[test]
    fn should_replace_old_metadata_nodes_with_new() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(None);

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let old_host_id = Uuid::new_v4();

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            old_host_id,
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                Some(old_host_id),
                None,
                Default::default(),
                "".into(),
                "".into(),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let metadata = refresh_metadata(
            &node_infos,
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_infos[0].host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_infos[0].broadcast_rpc_address
        );
    }

    #[test]
    fn should_use_fetched_keyspaces_on_refresh() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_distance_evaluator = MockNodeDistanceEvaluator::new();

        let mut old_keyspaces = FxHashMap::default();
        old_keyspaces.insert(
            "dropped".to_string(),
            KeyspaceMetadata::new(ReplicationStrategy::Other),
        );
        let old_metadata = ClusterMetadata::new(Default::default(), old_keyspaces);

        let mut keyspaces = FxHashMap::default();
        keyspaces.insert(
            "created".to_string(),
            KeyspaceMetadata::new(ReplicationStrategy::Other),
        );

        let metadata = refresh_metadata(
            &[],
            keyspaces,
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        assert!(metadata.keyspace("created").is_some());
        assert!(metadata.keyspace("dropped").is_none());
    }

    #[test]
    fn should_add_new_nodes_as_up_on_refresh() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Local));

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let old_metadata = ClusterMetadata::new(Default::default(), Default::default());

        let metadata = refresh_metadata(
            &node_infos,
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let node = metadata
            .nodes()
            .get(&node_infos[0].host_id)
            .unwrap()
            .clone();
        assert_eq!(node.state(), NodeState::Up);
        assert!(!node.is_ignored());
    }

    #[test]
    fn should_update_old_metadata_nodes_with_new_info() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Local));

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            node_infos[0].host_id,
            Arc::new(Node::new(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
                None,
                Some(node_infos[0].host_id),
                None,
                Default::default(),
                "".into(),
                "".into(),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let metadata = refresh_metadata(
            &node_infos,
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_infos[0].host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_infos[0].broadcast_rpc_address
        );
    }

    #[test]
    fn should_keep_node_state_when_rpc_address_is_unchanged() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_distance_evaluator = MockNodeDistanceEvaluator::new();

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                7000,
            )),
            "".into(),
            Default::default(),
            "".into(),
        )];

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            node_infos[0].host_id,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                node_infos[0].broadcast_rpc_address,
                node_infos[0].broadcast_address,
                Some(node_infos[0].host_id),
                Some(NodeDistance::Local),
                NodeState::Up,
                Default::default(),
                "".into(),
                "".into(),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let metadata = refresh_metadata(
            &node_infos,
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let node = metadata
            .nodes()
            .get(&node_infos[0].host_id)
            .unwrap()
            .clone();
        assert_eq!(node.state(), NodeState::Up);
        assert_eq!(node.distance(), Some(NodeDistance::Local));
    }

    #[test]
    fn should_mark_node_up_when_rpc_address_changes() {
        let connection_pool_factory = create_connection_pool_factory();

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Remote));

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
            None,
            "".into(),
            Default::default(),
            "".into(),
        )];

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            node_infos[0].host_id,
            Arc::new(Node::new_with_state(
                connection_pool_factory.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9042),
                None,
                Some(node_infos[0].host_id),
                Some(NodeDistance::Local),
                NodeState::Up,
                Default::default(),
                "".into(),
                "".into(),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let metadata = refresh_metadata(
            &node_infos,
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let node = metadata
            .nodes()
            .get(&node_infos[0].host_id)
            .unwrap()
            .clone();
        assert_eq!(
            node.broadcast_rpc_address(),
            node_infos[0].broadcast_rpc_address
        );
        assert_eq!(node.state(), NodeState::Up);
        assert_eq!(node.distance(), Some(NodeDistance::Remote));
        assert!(!node.is_ignored());
    }

    #[test]
    fn should_not_add_already_existing_node() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_info = NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        );

        let old_node = Node::with_distance(
            connection_pool_factory.clone(),
            node_info.broadcast_rpc_address,
            None,
            Some(node_info.host_id),
            NodeDistance::Local,
        );

        assert_eq!(old_node.state(), NodeState::Unknown);

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(node_info.host_id, Arc::new(old_node));

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let node_distance_evaluator = MockNodeDistanceEvaluator::new();

        // the state changes in place
        let metadata = add_new_node(
            node_info.clone(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
            NodeState::Up,
        );
        assert!(metadata.is_none());

        let nodes = old_metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_info.host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_info.broadcast_rpc_address
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().state(),
            NodeState::Up
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().distance().unwrap(),
            NodeDistance::Local
        );
    }

    #[test]
    fn should_not_demote_existing_up_node() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_info = NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
            None,
            "".into(),
            Default::default(),
            "".into(),
        );

        let old_node = Arc::new(Node::new_with_state(
            connection_pool_factory.clone(),
            node_info.broadcast_rpc_address,
            None,
            Some(node_info.host_id),
            Some(NodeDistance::Local),
            NodeState::Up,
            Default::default(),
            "".into(),
            "".into(),
        ));

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(node_info.host_id, old_node.clone());

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let node_distance_evaluator = MockNodeDistanceEvaluator::new();

        let metadata = add_new_node(
            node_info.clone(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
            NodeState::Unknown,
        )
        .unwrap_or(old_metadata);

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert_eq!(node.state(), NodeState::Up);
        assert!(!node.is_ignored());
        assert!(Arc::ptr_eq(&node, &old_node));
    }

    #[test]
    fn should_update_existing_node() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_info = NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        );

        let old_node = Node::with_distance(
            connection_pool_factory.clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
            None,
            Some(node_info.host_id),
            NodeDistance::Local,
        );

        assert_eq!(old_node.state(), NodeState::Unknown);

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(node_info.host_id, Arc::new(old_node));

        let old_metadata = ClusterMetadata::new(old_nodes, Default::default());

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Remote));

        let metadata = add_new_node(
            node_info.clone(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
            NodeState::Up,
        )
        .unwrap();

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_info.host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_info.broadcast_rpc_address
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().state(),
            NodeState::Up
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().distance(),
            Some(NodeDistance::Remote)
        );
        assert!(!nodes.get(&node_info.host_id).unwrap().is_ignored());
    }

    #[test]
    fn should_add_new_node() {
        let connection_pool_factory = create_connection_pool_factory();

        let node_info = NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            None,
            "".into(),
            Default::default(),
            "".into(),
        );

        let old_metadata = ClusterMetadata::new(Default::default(), Default::default());

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Local));

        let metadata = add_new_node(
            node_info.clone(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
            NodeState::Up,
        )
        .unwrap();

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes
                .get(&node_info.host_id)
                .unwrap()
                .broadcast_rpc_address(),
            node_info.broadcast_rpc_address
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().state(),
            NodeState::Up
        );
        assert_eq!(
            nodes.get(&node_info.host_id).unwrap().distance(),
            Some(NodeDistance::Local)
        );
        assert!(!nodes.get(&node_info.host_id).unwrap().is_ignored());
    }

    fn counting_connection_pool_factory(
        connect_count: Arc<AtomicUsize>,
    ) -> Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>> {
        test_support::create_connection_pool_factory(
            TestConnectionManager::new(|_, _| ready(Ok(void_response(Version::V4))).boxed())
                .with_connect_handler(move |_| {
                    connect_count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }),
            Version::V4,
        )
    }

    fn test_node_info(broadcast_rpc_address: SocketAddr) -> NodeInfo {
        NodeInfo::new(
            Uuid::new_v4(),
            broadcast_rpc_address,
            None,
            "".into(),
            Default::default(),
            "".into(),
        )
    }

    fn create_up_node(
        connection_pool_factory: &Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>>,
        node_info: &NodeInfo,
    ) -> Arc<TestNode> {
        Arc::new(Node::new_with_state(
            connection_pool_factory.clone(),
            node_info.broadcast_rpc_address,
            node_info.broadcast_address,
            Some(node_info.host_id),
            Some(NodeDistance::Local),
            NodeState::Up,
            node_info.tokens.clone(),
            node_info.rack.clone(),
            node_info.datacenter.clone(),
        ))
    }

    fn metadata_with_node(
        node: Arc<TestNode>,
    ) -> ClusterMetadata<TestTransport, TestConnectionManager> {
        let mut nodes = NodeMap::default();
        nodes.insert(node.host_id().unwrap(), node);
        ClusterMetadata::new(nodes, Default::default())
    }

    #[tokio::test]
    async fn should_reuse_connection_pool_when_rpc_address_is_unchanged() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connection_pool_factory = counting_connection_pool_factory(connect_count.clone());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        old_node.persistent_connection().await.unwrap();
        let old_metadata = metadata_with_node(old_node.clone());

        let metadata = refresh_metadata(
            &[node_info.clone()],
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &MockNodeDistanceEvaluator::new(),
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        node.persistent_connection().await.unwrap();

        assert_eq!(connect_count.load(Ordering::Relaxed), 1);
        assert!(node.shares_connectivity_with(&old_node));
    }

    #[tokio::test]
    async fn should_create_new_connection_pool_when_rpc_address_changes() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connection_pool_factory = counting_connection_pool_factory(connect_count.clone());

        let old_node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &old_node_info);
        old_node.persistent_connection().await.unwrap();
        let old_pool = Arc::downgrade(&old_node.connection_pool().unwrap());
        let old_metadata = metadata_with_node(old_node.clone());

        let mut node_info = old_node_info.clone();
        node_info.broadcast_rpc_address = node_address(2);

        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(NodeDistance::Local));

        let metadata = refresh_metadata(
            &[node_info.clone()],
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &node_distance_evaluator,
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        node.persistent_connection().await.unwrap();

        assert_eq!(connect_count.load(Ordering::Relaxed), 2);
        assert!(!node.shares_connectivity_with(&old_node));

        drop(old_metadata);
        drop(old_node);
        assert!(old_pool.upgrade().is_none());
    }

    #[tokio::test]
    async fn should_close_connection_pool_of_removed_node() {
        let connection_pool_factory = counting_connection_pool_factory(Default::default());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        old_node.persistent_connection().await.unwrap();
        let old_pool = Arc::downgrade(&old_node.connection_pool().unwrap());
        let old_metadata = metadata_with_node(old_node);

        let metadata = refresh_metadata(
            &[],
            Default::default(),
            &old_metadata,
            &connection_pool_factory,
            &MockNodeDistanceEvaluator::new(),
        );
        assert!(!metadata.has_nodes());

        drop(old_metadata);
        assert!(old_pool.upgrade().is_none());
    }

    #[tokio::test]
    async fn should_keep_connection_pool_when_adding_existing_node() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connection_pool_factory = counting_connection_pool_factory(connect_count.clone());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        old_node.persistent_connection().await.unwrap();
        old_node.shared().mark_down();
        let old_metadata = metadata_with_node(old_node.clone());

        let metadata = add_new_node(
            node_info.clone(),
            &old_metadata,
            &connection_pool_factory,
            &MockNodeDistanceEvaluator::new(),
            NodeState::Up,
        )
        .unwrap_or(old_metadata);

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert_eq!(node.state(), NodeState::Up);
        assert_eq!(old_node.state(), NodeState::Up);

        node.persistent_connection().await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 1);
        assert!(node.shares_connectivity_with(&old_node));
    }

    fn distance_evaluator(distance: NodeDistance) -> MockNodeDistanceEvaluator {
        let mut node_distance_evaluator = MockNodeDistanceEvaluator::new();
        node_distance_evaluator
            .expect_compute_distance()
            .return_const(Some(distance));
        node_distance_evaluator
    }

    #[tokio::test]
    async fn should_reuse_connection_pool_on_full_refresh() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connection_pool_factory = counting_connection_pool_factory(connect_count.clone());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        old_node.persistent_connection().await.unwrap();
        old_node.shared().mark_down();
        let old_metadata = metadata_with_node(old_node.clone());

        let metadata = build_initial_metadata(
            vec![node_info.clone()],
            Default::default(),
            &[],
            Some(&old_metadata),
            &connection_pool_factory,
            &distance_evaluator(NodeDistance::Local),
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert_eq!(node.state(), NodeState::Up);

        node.persistent_connection().await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 1);
        assert!(node.shares_connectivity_with(&old_node));
    }

    #[tokio::test]
    async fn should_recreate_forced_down_node_on_full_refresh() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connection_pool_factory = counting_connection_pool_factory(connect_count.clone());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        old_node.persistent_connection().await.unwrap();
        old_node.force_down();
        let old_metadata = metadata_with_node(old_node.clone());

        let metadata = build_initial_metadata(
            vec![node_info.clone()],
            Default::default(),
            &[],
            Some(&old_metadata),
            &connection_pool_factory,
            &distance_evaluator(NodeDistance::Local),
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert_eq!(node.state(), NodeState::Up);
        assert_eq!(old_node.state(), NodeState::ForcedDown);
        assert!(!node.shares_connectivity_with(&old_node));

        node.persistent_connection().await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn should_recreate_node_with_changed_distance_on_full_refresh() {
        let connection_pool_factory = counting_connection_pool_factory(Default::default());

        let node_info = test_node_info(node_address(1));
        let old_node = create_up_node(&connection_pool_factory, &node_info);
        let old_metadata = metadata_with_node(old_node.clone());

        let metadata = build_initial_metadata(
            vec![node_info.clone()],
            Default::default(),
            &[],
            Some(&old_metadata),
            &connection_pool_factory,
            &distance_evaluator(NodeDistance::Remote),
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert_eq!(node.distance(), Some(NodeDistance::Remote));
        assert!(!node.shares_connectivity_with(&old_node));
    }

    #[test]
    fn should_recreate_node_with_new_host_id_on_full_refresh() {
        let connection_pool_factory = counting_connection_pool_factory(Default::default());

        let old_node = create_up_node(&connection_pool_factory, &test_node_info(node_address(1)));
        let old_metadata = metadata_with_node(old_node.clone());

        let node_info = test_node_info(node_address(1));
        let metadata = build_initial_metadata(
            vec![node_info.clone()],
            Default::default(),
            &[],
            Some(&old_metadata),
            &connection_pool_factory,
            &distance_evaluator(NodeDistance::Local),
        );

        assert_eq!(metadata.nodes().len(), 1);
        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        assert!(!node.shares_connectivity_with(&old_node));
    }

    #[tokio::test]
    async fn contact_point_clone_gets_own_connection_pool() {
        let broken: Arc<Mutex<Vec<Arc<AtomicBool>>>> = Default::default();
        let error_handlers: Arc<Mutex<Vec<mpsc::Sender<Error>>>> = Default::default();

        let mut connection_manager = MockConnectionManager::<MockCdrsTransport>::new();
        connection_manager.expect_connection().returning({
            let broken = broken.clone();
            let error_handlers = error_handlers.clone();
            move |_, error_handler, _| {
                let connection_broken = Arc::new(AtomicBool::new(false));
                broken.lock().unwrap().push(connection_broken.clone());
                error_handlers.lock().unwrap().push(error_handler.unwrap());

                let mut transport = MockCdrsTransport::new();
                transport
                    .expect_is_broken()
                    .returning(move || connection_broken.load(Ordering::Relaxed));
                Box::pin(async move { Ok(transport) })
            }
        });

        let (_, keyspace_receiver) = watch::channel(None);
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            // keeps the node down for the duration of the test
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_secs(3600))),
        ));

        let node_info = test_node_info(node_address(1));
        let contact_point = Arc::new(Node::new_with_state(
            connection_pool_factory.clone(),
            node_info.broadcast_rpc_address,
            None,
            None,
            Some(NodeDistance::Local),
            NodeState::Up,
            Default::default(),
            "".into(),
            "".into(),
        ));
        contact_point.persistent_connection().await.unwrap();

        let metadata = build_initial_metadata(
            vec![node_info.clone()],
            Default::default(),
            &[contact_point.clone()],
            None,
            &connection_pool_factory,
            &distance_evaluator(NodeDistance::Local),
        );

        let node = metadata.nodes().get(&node_info.host_id).unwrap().clone();
        node.persistent_connection().await.unwrap();

        assert_eq!(broken.lock().unwrap().len(), 2);
        assert!(!node.shares_connectivity_with(&contact_point));
        assert!(!Arc::ptr_eq(
            &node.connection_pool().unwrap(),
            &contact_point.connection_pool().unwrap()
        ));

        broken.lock().unwrap()[1].store(true, Ordering::Relaxed);
        error_handlers.lock().unwrap()[1]
            .try_send(Error::General("connection broken".into()))
            .unwrap();

        timeout(TEST_GUARD_TIMEOUT, async {
            while node.state() != NodeState::Down {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("monitor did not mark the metadata node down");
        assert_eq!(contact_point.state(), NodeState::Up);
    }
}
