use fxhash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use tracing::*;

use crate::cluster::topology::Node;
use crate::cluster::{ClusterMetadata, ConnectionManager, NodeInfo};
use crate::transport::CdrsTransport;

pub fn build_initial_metadata<T: CdrsTransport, CM: ConnectionManager<T>>(
    node_infos: &[NodeInfo],
    contact_points: &[Arc<Node<T, CM>>],
    connection_manager: &Arc<CM>,
) -> ClusterMetadata<T, CM> {
    let mut nodes = FxHashMap::with_capacity_and_hasher(node_infos.len(), Default::default());
    for node_info in node_infos {
        if nodes.contains_key(&node_info.host_id) {
            warn!(
                host_id = %node_info.host_id,
                "Found duplicate peer entries - keeping only the first one."
            );
        } else {
            let contact_point = contact_points.iter().find(|contact_point| {
                contact_point.broadcast_rpc_address() == node_info.broadcast_rpc_address
            });

            let node = if let Some(contact_point) = contact_point {
                debug!(?node_info, "Copying contact point.");
                contact_point.clone()
            } else {
                debug!(?node_info, "Adding new node.");
                Arc::new(Node::new(
                    connection_manager.clone(),
                    node_info.broadcast_rpc_address,
                ))
            };

            nodes.insert(node_info.host_id, node);
        }
    }

    ClusterMetadata::new(nodes)
}

pub fn refresh_metadata<T: CdrsTransport, CM: ConnectionManager<T>>(
    node_infos: &[NodeInfo],
    old_metadata: &ClusterMetadata<T, CM>,
    connection_manager: &Arc<CM>,
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
                    Arc::new(old_node.clone_with_node_info(node_info)),
                );
            } else {
                debug!(?node_info, "Adding new node.");

                let node = Arc::new(Node::new(
                    connection_manager.clone(),
                    node_info.broadcast_rpc_address,
                ));

                added_or_updated.insert(node_info.host_id, node);
            }
        }
    }

    old_metadata.clone_with_nodes(added_or_updated)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::metadata_builder::{build_initial_metadata, refresh_metadata};
    use crate::cluster::topology::cluster_metadata::NodeMap;
    use crate::cluster::topology::Node;
    use crate::cluster::{ClusterMetadata, NodeInfo};
    use crate::transport::MockCdrsTransport;

    #[test]
    fn should_create_initial_metadata_from_all_new_nodes() {
        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        )];

        let connection_manager = MockConnectionManager::<MockCdrsTransport>::new();

        let metadata = build_initial_metadata(&node_infos, &[], &Arc::new(connection_manager));

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
        let connection_manager = Arc::new(MockConnectionManager::<MockCdrsTransport>::new());

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        )];
        let contact_points = [Arc::new(Node::new(
            connection_manager.clone(),
            node_infos[0].broadcast_rpc_address,
        ))];

        let metadata = build_initial_metadata(&node_infos, &contact_points, &connection_manager);

        let nodes = metadata.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            Arc::as_ptr(nodes.get(&node_infos[0].host_id).unwrap()),
            Arc::as_ptr(&contact_points[0])
        );
    }

    #[test]
    fn should_replace_old_metadata_nodes_with_new() {
        let connection_manager = Arc::new(MockConnectionManager::<MockCdrsTransport>::new());

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        )];

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            Uuid::new_v4(),
            Arc::new(Node::new(
                connection_manager.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes);

        let metadata = refresh_metadata(&node_infos, &old_metadata, &connection_manager);

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
    fn should_update_old_metadata_nodes_with_new_info() {
        let connection_manager = Arc::new(MockConnectionManager::<MockCdrsTransport>::new());

        let node_infos = [NodeInfo::new(
            Uuid::new_v4(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        )];

        let mut old_nodes = NodeMap::default();
        old_nodes.insert(
            node_infos[0].host_id,
            Arc::new(Node::new(
                connection_manager.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8080),
            )),
        );

        let old_metadata = ClusterMetadata::new(old_nodes);

        let metadata = refresh_metadata(&node_infos, &old_metadata, &connection_manager);

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
}
