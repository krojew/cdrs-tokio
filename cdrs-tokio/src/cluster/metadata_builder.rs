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
    connection_pool_factory: &Arc<ConnectionPoolFactory<T, CM>>,
    node_distance_evaluator: &(dyn NodeDistanceEvaluator + Send + Sync),
) -> ClusterMetadata<T, CM> {
    let mut nodes = FxHashMap::with_capacity_and_hasher(node_infos.len(), Default::default());
    for node_info in node_infos {
        if let Entry::Vacant(entry) = nodes.entry(node_info.host_id) {
            let contact_point = contact_points.iter().find(|contact_point| {
                contact_point.broadcast_rpc_address() == node_info.broadcast_rpc_address
            });

            let node = if let Some(contact_point) = contact_point {
                debug!(?node_info, "Copying contact point.");
                Arc::new(contact_point.clone_as_contact_point(node_info, node_distance_evaluator))
            } else {
                debug!(?node_info, "Adding new node.");
                Arc::new(Node::new_with_state(
                    connection_pool_factory.clone(),
                    node_info.broadcast_rpc_address,
                    node_info.broadcast_address,
                    Some(node_info.host_id),
                    node_distance_evaluator.compute_distance(&node_info),
                    NodeState::Up,
                    node_info.tokens.clone(),
                    node_info.rack,
                    node_info.datacenter,
                ))
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

            debug!(?old_node, "Setting existing node state to up.");
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
    use cassandra_protocol::frame::Version;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::watch;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::metadata_builder::{
        add_new_node, build_initial_metadata, refresh_metadata,
    };
    use crate::cluster::topology::NodeMap;
    use crate::cluster::topology::{
        KeyspaceMetadata, Node, NodeDistance, NodeState, ReplicationStrategy,
    };
    use crate::cluster::{ClusterMetadata, NodeInfo};
    use crate::load_balancing::node_distance_evaluator::MockNodeDistanceEvaluator;
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;
    use fxhash::FxHashMap;

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
}
