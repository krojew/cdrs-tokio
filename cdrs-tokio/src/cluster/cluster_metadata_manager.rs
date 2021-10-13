use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use itertools::Itertools;
use tokio::sync::broadcast::Receiver;
use tracing::*;

use crate::cluster::metadata_builder::{build_initial_metadata, refresh_metadata};
use crate::cluster::topology::Node;
use crate::cluster::NodeInfo;
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::error::Error;
use crate::error::Result;
use crate::events::ServerEvent;
use crate::frame::frame_error::{AdditionalErrorInfo, CdrsError};
use crate::frame::Frame;
use crate::query::utils::prepare_flags;
use crate::query::{Query, QueryParamsBuilder};
use crate::transport::CdrsTransport;
use crate::types::rows::Row;
use crate::types::{ByName, IntoRustByName};

pub struct ClusterMetadataManager<T: CdrsTransport, CM: ConnectionManager<T>> {
    // TODO: apply topology changes
    #[allow(dead_code)]
    event_receiver: Receiver<ServerEvent>,
    metadata: ArcSwap<ClusterMetadata<T, CM>>,
    contact_points: Vec<Arc<Node<T, CM>>>,
    connection_manager: Arc<CM>,
    did_initial_refresh: AtomicBool,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadataManager<T, CM> {
    pub fn new(
        event_receiver: Receiver<ServerEvent>,
        contact_points: Vec<Arc<Node<T, CM>>>,
        connection_manager: Arc<CM>,
    ) -> Self {
        ClusterMetadataManager {
            event_receiver,
            metadata: ArcSwap::from_pointee(ClusterMetadata::default()),
            contact_points,
            connection_manager,
            did_initial_refresh: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.metadata.load().clone()
    }

    // Refreshes stored metadata. Note: it is expected to be called by the control connection.
    pub async fn refresh_metadata(&self, control_transport: &T) -> Result<()> {
        let node_infos = Self::build_node_infos(control_transport).await?;

        if self
            .did_initial_refresh
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.metadata.store(Arc::new(build_initial_metadata(
                &node_infos,
                &self.contact_points,
                &self.connection_manager,
            )));
        } else {
            self.metadata.rcu(|old_metadata| {
                refresh_metadata(&node_infos, old_metadata.as_ref(), &self.connection_manager)
            });
        };

        Ok(())
    }

    async fn build_node_infos(control_transport: &T) -> Result<Vec<NodeInfo>> {
        let control_addr = control_transport.addr();
        let local = Self::send_query("SELECT * FROM system.local", control_transport)
            .await?
            .ok_or_else(|| format!("Node {} failed to return info about itself!", control_addr))?;

        let local = local
            .get(0)
            .ok_or_else(|| format!("Node {} failed to return info about itself!", control_addr))?;

        let local_broadcast_rpc_address = Self::broadcast_rpc_address_from_row(local, control_addr);
        let local_broadcast_rpc_address = Self::build_node_broadcast_rpc_address(
            local,
            local_broadcast_rpc_address,
            control_addr,
        );

        let mut node_infos = vec![Self::build_node_info(local, local_broadcast_rpc_address)?];

        let peers = Self::query_peers(control_transport).await?;
        if let Some(peers) = peers {
            node_infos.reserve(peers.len());
            node_infos = peers
                .iter()
                .filter_map(|row| {
                    if !Self::is_peer_row_valid(row) {
                        return None;
                    }

                    Self::broadcast_rpc_address_from_row(row, control_addr).map(
                        |broadcast_rpc_address| Self::build_node_info(row, broadcast_rpc_address),
                    )
                })
                .fold_ok(node_infos, |mut node_infos, node_info| {
                    node_infos.push(node_info);
                    node_infos
                })?;
        }

        Ok(node_infos)
    }

    async fn query_peers(transport: &T) -> Result<Option<Vec<Row>>> {
        let peers_v2_result = Self::send_query("SELECT * FROM system.peers_v2", transport).await;
        match peers_v2_result {
            Ok(result) => Ok(result),
            // peers_v2 does not exist
            Err(Error::Server(CdrsError {
                additional_info: AdditionalErrorInfo::Invalid,
                ..
            })) => Self::send_query("SELECT * FROM system.peers", transport).await,
            Err(error) => Err(error),
        }
    }

    async fn send_query(query: &str, transport: &T) -> Result<Option<Vec<Row>>> {
        let query_params = QueryParamsBuilder::new().idempotent(true).finalize();

        let query = Query {
            query: query.to_string(),
            params: query_params,
        };

        let flags = prepare_flags(false, false);
        let frame = Frame::new_query(query, flags);

        transport
            .write_frame(&frame)
            .await
            .and_then(|frame| frame.body())
            .map(|body| body.into_rows())
    }

    fn build_node_info(row: &Row, broadcast_rpc_address: SocketAddr) -> Result<NodeInfo> {
        row.get_r_by_name("host_id")
            .map(move |host_id| NodeInfo::new(host_id, broadcast_rpc_address))
    }

    fn build_node_broadcast_rpc_address(
        row: &Row,
        broadcast_rpc_address: Option<SocketAddr>,
        control_addr: SocketAddr,
    ) -> SocketAddr {
        if row.contains_column("peer") {
            // this can only happen when a misconfigured local node thinks it's also a peer
            broadcast_rpc_address.unwrap_or(control_addr)
        } else {
            // Don't rely on system.local.rpc_address for the control node, because it mistakenly
            // reports the normal RPC address instead of the broadcast one (CASSANDRA-11181). We
            // already know the endpoint anyway since we've just used it to query.
            control_addr
        }
    }

    fn broadcast_rpc_address_from_row(row: &Row, control_addr: SocketAddr) -> Option<SocketAddr> {
        // in system.peers or system.local
        let rpc_address: Result<Option<IpAddr>> = row.by_name("rpc_address").or_else(|_| {
            // in system.peers_v2 (Cassandra >= 4.0)
            row.by_name("native_address")
        });

        let rpc_address = match rpc_address {
            Ok(Some(rpc_address)) => rpc_address,
            Ok(None) => return None,
            Err(error) => {
                // this could only happen if system tables are corrupted, but handle gracefully
                warn!(%error, "Error getting rpc address.");
                return None;
            }
        };

        // system.local for Cassandra >= 4.0
        let rpc_port: i32 = row
            .get_by_name("rpc_port")
            .or_else(|_| {
                // system.peers_v2
                row.get_by_name("native_port")
            })
            // use the default port if no port information was found in the row
            .map(|port| port.unwrap_or_else(|| control_addr.port() as i32))
            .unwrap_or_else(|_| control_addr.port() as i32);

        let rpc_address = SocketAddr::new(rpc_address, rpc_port as u16);

        // if the peer is actually the control node, ignore that peer as it is likely a
        // misconfiguration problem
        if rpc_address == control_addr && row.contains_column("peer") {
            warn!(
                node = %rpc_address,
                control = %control_addr,
                "Control node has itself as a peer, thus will be ignored. This is likely due to a \
                misconfiguration; please verify your rpc_address configuration in cassandra.yaml \
                on all nodes in your cluster."
            );

            None
        } else {
            Some(rpc_address)
        }
    }

    fn is_peer_row_valid(row: &Row) -> bool {
        let has_peers_rpc_address = !row.is_empty_by_name("rpc_address");
        let has_peers_v_2_rpc_address =
            !row.is_empty_by_name("native_address") && !row.is_empty_by_name("native_port");
        let has_rpc_address = has_peers_rpc_address || has_peers_v_2_rpc_address;

        has_rpc_address
            && !row.is_empty_by_name("host_id")
            && !row.is_empty_by_name("data_center")
            && !row.is_empty_by_name("rack")
            && !row.is_empty_by_name("tokens")
            && !row.is_empty_by_name("schema_version")
    }
}
