use arc_swap::ArcSwap;
use fxhash::FxHashMap;
use itertools::Itertools;
use serde_json::{Map, Value as JsonValue};
use std::convert::TryInto;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tracing::*;

use crate::cluster::metadata_builder::{add_new_node, build_initial_metadata, refresh_metadata};
use crate::cluster::topology::{KeyspaceMetadata, Node, NodeState, ReplicationStrategy};
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::cluster::{NodeInfo, SessionContext};
use crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use crate::transport::CdrsTransport;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::events::{SchemaChange, ServerEvent};
use cassandra_protocol::frame::events::{
    SchemaChangeOptions, SchemaChangeType, StatusChange, StatusChangeType, TopologyChange,
    TopologyChangeType,
};
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, CdrsError};
use cassandra_protocol::frame::{Frame, Version};
use cassandra_protocol::query::utils::prepare_flags;
use cassandra_protocol::query::{Query, QueryParams, QueryParamsBuilder, QueryValues};
use cassandra_protocol::types::list::List;
use cassandra_protocol::types::rows::Row;
use cassandra_protocol::types::{AsRustType, ByName, IntoRustByName};

fn find_in_peers(
    peers: &[Row],
    broadcast_rpc_address: SocketAddr,
    control_addr: SocketAddr,
) -> Result<Option<NodeInfo>> {
    peers
        .iter()
        .find_map(|peer| {
            broadcast_rpc_address_from_row(peer, control_addr)
                .filter(|peer_address| {
                    *peer_address == broadcast_rpc_address && is_peer_row_valid(peer)
                })
                .map(|peer_address| build_node_info(peer, peer_address))
        })
        .transpose()
}

async fn send_query<T: CdrsTransport>(
    query: &str,
    transport: &T,
    version: Version,
) -> Result<Option<Vec<Row>>> {
    let query_params = QueryParamsBuilder::new().idempotent(true).finalize();
    send_query_with_params(query, query_params, transport, version).await
}

async fn send_query_with_values<T: CdrsTransport, V: Into<QueryValues>>(
    query: &str,
    values: V,
    transport: &T,
    version: Version,
) -> Result<Option<Vec<Row>>> {
    let query_params = QueryParamsBuilder::new()
        .idempotent(true)
        .values(values.into())
        .finalize();
    send_query_with_params(query, query_params, transport, version).await
}

async fn send_query_with_params<T: CdrsTransport>(
    query: &str,
    query_params: QueryParams,
    transport: &T,
    version: Version,
) -> Result<Option<Vec<Row>>> {
    let query = Query {
        query: query.to_string(),
        params: query_params,
    };

    let flags = prepare_flags(false, false);
    let frame = Frame::new_query(query, flags, version);

    transport
        .write_frame(&frame)
        .await
        .and_then(|frame| frame.body())
        .map(|body| body.into_rows())
}

fn build_node_info(row: &Row, broadcast_rpc_address: SocketAddr) -> Result<NodeInfo> {
    row.get_r_by_name("host_id").and_then(move |host_id| {
        let broadcast_address: Option<IpAddr> = row
            .get_by_name("broadcast_address")
            .or_else(|_| row.get_by_name("peer"))?;

        let broadcast_address = if let Some(broadcast_address) = broadcast_address {
            let port: Option<i32> = if row.contains_column("broadcast_port") {
                // system.local for Cassandra >= 4.0
                row.get_by_name("broadcast_port")?
            } else if row.contains_column("peer_port") {
                // system.peers_v2
                row.get_by_name("peer_port")?
            } else {
                None
            };

            port.map(|port| SocketAddr::new(broadcast_address, port as u16))
        } else {
            None
        };

        let datacenter = row.get_r_by_name("data_center")?;
        let rack = row.get_r_by_name("rack")?;
        let tokens: List = row.get_r_by_name("tokens")?;
        let tokens: Vec<String> = tokens.as_r_type()?;

        Ok(NodeInfo::new(
            host_id,
            broadcast_rpc_address,
            broadcast_address,
            datacenter,
            tokens
                .into_iter()
                .filter_map(|token| token.try_into().ok()) // ignore unsupported tokens
                .collect(),
            rack,
        ))
    })
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

async fn fetch_control_connection_info<T: CdrsTransport>(
    control_transport: &T,
    control_addr: &SocketAddr,
    version: Version,
) -> Result<Row> {
    send_query("SELECT * FROM system.local", control_transport, version)
        .await?
        .and_then(|mut rows| rows.pop())
        .ok_or_else(|| format!("Node {} failed to return info about itself!", control_addr).into())
}

fn build_keyspace(row: &Row) -> Result<(String, KeyspaceMetadata)> {
    let keyspace_name = row.get_r_by_name("keyspace_name")?;

    let replication: String = row.get_r_by_name("replication")?;
    let replication: JsonValue = serde_json::from_str(&replication).map_err(|error| {
        Error::General(format!(
            "Error parsing replication for {}: {}",
            keyspace_name, error
        ))
    })?;

    let replication_strategy = match replication {
        JsonValue::Object(properties) => build_replication_strategy(properties)?,
        _ => return Err(format!("Invalid replication format for: {}", keyspace_name).into()),
    };

    Ok((keyspace_name, KeyspaceMetadata::new(replication_strategy)))
}

fn build_replication_strategy(properties: Map<String, JsonValue>) -> Result<ReplicationStrategy> {
    match properties.get("class") {
        Some(JsonValue::String(class)) => Ok(match class.as_str() {
            "org.apache.cassandra.locator.SimpleStrategy" | "SimpleStrategy" => {
                ReplicationStrategy::SimpleStrategy {
                    replication_factor: extract_replication_factor(
                        properties.get("replication_factor"),
                    )?,
                }
            }
            "org.apache.cassandra.locator.NetworkTopologyStrategy" | "NetworkTopologyStrategy" => {
                ReplicationStrategy::NetworkTopologyStrategy {
                    datacenter_replication_factor: extract_datacenter_replication_factor(
                        properties,
                    )?,
                }
            }
            _ => ReplicationStrategy::Other,
        }),
        _ => Err("Missing replication strategy class!".into()),
    }
}

fn extract_datacenter_replication_factor(
    properties: Map<String, JsonValue>,
) -> Result<FxHashMap<String, usize>> {
    properties
        .into_iter()
        .map(|(key, replication_factor)| {
            extract_replication_factor(Some(&replication_factor))
                .map(move |replication_factor| (key, replication_factor))
        })
        .try_collect()
}

fn extract_replication_factor(value: Option<&JsonValue>) -> Result<usize> {
    match value {
        Some(JsonValue::String(replication_factor)) => {
            let result = if let Some(slash) = replication_factor.find('/') {
                usize::from_str(&replication_factor[..slash])
            } else {
                usize::from_str(replication_factor)
            };

            result.map_err(|error| format!("Error parsing replication factor: {}", error).into())
        }
        _ => Err("Missing replication factor!".into()),
    }
}

pub struct ClusterMetadataManager<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    metadata: ArcSwap<ClusterMetadata<T, CM>>,
    contact_points: Vec<Arc<Node<T, CM>>>,
    connection_manager: Arc<CM>,
    did_initial_refresh: AtomicBool,
    is_schema_v2: AtomicBool,
    session_context: Arc<SessionContext<T>>,
    node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    version: Version,
}

impl<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> ClusterMetadataManager<T, CM> {
    pub fn new(
        contact_points: Vec<Arc<Node<T, CM>>>,
        connection_manager: Arc<CM>,
        session_context: Arc<SessionContext<T>>,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
        version: Version,
    ) -> Self {
        ClusterMetadataManager {
            metadata: ArcSwap::from_pointee(ClusterMetadata::default()),
            contact_points,
            connection_manager,
            did_initial_refresh: AtomicBool::new(false),
            is_schema_v2: AtomicBool::new(true),
            session_context,
            node_distance_evaluator,
            version,
        }
    }

    pub fn listen_to_events(self: Arc<Self>, mut event_receiver: Receiver<ServerEvent>) {
        tokio::spawn(async move {
            loop {
                let event = event_receiver.recv().await;
                match event {
                    Ok(event) => self.process_event(event).await,
                    Err(RecvError::Lagged(n)) => {
                        warn!("Skipped {} events.", n);
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    async fn process_event(&self, event: ServerEvent) {
        trace!(?event);

        match event {
            ServerEvent::TopologyChange(event) => self.process_topology_event(event).await,
            ServerEvent::StatusChange(event) => self.process_status_event(event).await,
            ServerEvent::SchemaChange(event) => self.process_schema_event(event).await,
        }
    }

    async fn process_schema_event(&self, event: SchemaChange) {
        if let SchemaChangeOptions::Keyspace(keyspace) = &event.options {
            match event.change_type {
                SchemaChangeType::Created | SchemaChangeType::Updated => {
                    self.refresh_keyspace(keyspace).await
                }
                SchemaChangeType::Dropped => {
                    self.remove_keyspace(keyspace);
                }
            }
        }
    }

    async fn process_topology_event(&self, event: TopologyChange) {
        let metadata = self.metadata.load().clone();
        match event.change_type {
            TopologyChangeType::NewNode => {
                if metadata.has_node_by_rpc_address(event.addr.addr) {
                    debug!(
                        broadcast_rpc_address = %event.addr.addr,
                        "Trying to add already existing node - ignoring."
                    );
                } else {
                    self.add_new_node(event.addr.addr, NodeState::Unknown, metadata)
                        .await;
                }
            }
            TopologyChangeType::RemovedNode => {
                if metadata.has_node_by_rpc_address(event.addr.addr) {
                    debug!(broadcast_rpc_address = %event.addr.addr, "Removing node from cluster.");

                    self.metadata
                        .store(Arc::new(metadata.clone_without_node(event.addr.addr)));
                } else {
                    debug!(
                        broadcast_rpc_address = %event.addr.addr,
                        "Trying to remove a node outside the cluster."
                    );
                }
            }
        }
    }

    async fn process_status_event(&self, event: StatusChange) {
        let metadata = self.metadata.load().clone();
        let node = metadata.find_node_by_rpc_address(event.addr.addr);
        match event.change_type {
            StatusChangeType::Up => {
                if let Some(node) = node {
                    if node.state() != NodeState::Up {
                        debug!(?node, "Setting existing node state to up.");

                        // node was down or in an unknown state
                        let node = node.clone_with_node_state(NodeState::Up);
                        self.metadata
                            .store(Arc::new(metadata.clone_with_node(node)));
                    } else {
                        debug!(?node, "Ignoring up node event for already up node.");
                    }
                } else {
                    self.add_new_node(event.addr.addr, NodeState::Up, metadata)
                        .await;
                }
            }
            StatusChangeType::Down => {
                if let Some(node) = node {
                    if node.state() != NodeState::Down {
                        debug!(?node, "Setting existing node state to down.");

                        // node was up or in an unknown state
                        let node = node.clone_with_node_state(NodeState::Down);
                        self.metadata
                            .store(Arc::new(metadata.clone_with_node(node)));
                    } else {
                        debug!(?node, "Ignoring down node event for already downed node.");
                    }
                } else {
                    debug!(broadcast_rpc_address = %event.addr.addr, "Unknown node down.");
                }
            }
        }
    }

    fn remove_keyspace(&self, keyspace: &str) {
        let metadata = self.metadata.load().clone();
        self.metadata
            .store(Arc::new(metadata.clone_without_keyspace(keyspace)));
    }

    async fn refresh_keyspace(&self, keyspace: &str) {
        if let Err(error) = self.try_refresh_keyspace(keyspace).await {
            error!(?error, %keyspace, "Error refreshing keyspace!");
        }
    }

    async fn try_refresh_keyspace(&self, keyspace: &str) -> Result<()> {
        debug!(%keyspace, "Refreshing keyspace.");

        let control_transport = self.control_transport()?;
        send_query_with_values(
            "SELECT keyspace_name, toJson(replication) AS replication FROM system_schema.keyspaces WHERE keyspace_name = ?",
            QueryValues::SimpleValues(vec![keyspace.into()]),
            control_transport.as_ref(),
            self.version
        )
        .await
        .map(|rows| { rows.and_then(|mut rows| rows.pop()) })
        .and_then(|row| {
            match row {
                Some(row) => {
                    let (keyspace_name, keyspace) = build_keyspace(&row)?;
                    let metadata = self.metadata.load().clone();
                    self.metadata.store(Arc::new(
                        metadata.clone_with_keyspace(keyspace_name, keyspace),
                    ));
                }
                None => {
                    warn!(%keyspace, "Keyspace to refresh disappeared.");
                    self.remove_keyspace(keyspace);
                }
            }

            Ok(())
        })
    }

    async fn add_new_node(
        &self,
        broadcast_rpc_address: SocketAddr,
        state: NodeState,
        metadata: Arc<ClusterMetadata<T, CM>>,
    ) {
        debug!(%broadcast_rpc_address, %state, "Adding new node to metadata.");

        let new_node_info = self.find_new_node_info(broadcast_rpc_address).await;
        match new_node_info {
            Ok(Some(new_node_info)) => {
                self.metadata.store(Arc::new(add_new_node(
                    new_node_info,
                    metadata.as_ref(),
                    &self.connection_manager,
                    state,
                )));
            }
            Ok(None) => {
                warn!(%broadcast_rpc_address, "Cannot find new node info. Ignoring new node.");
            }
            Err(error) => {
                error!(%error, %broadcast_rpc_address, "Error finding new node info!");
            }
        }
    }

    async fn find_new_node_info(
        &self,
        broadcast_rpc_address: SocketAddr,
    ) -> Result<Option<NodeInfo>> {
        debug!(%broadcast_rpc_address, "Fetching info about a new node.");

        let control_transport = self.control_transport()?;
        let control_addr = control_transport.address();

        // in the awkward case we have the control connection node up, it won't be in peers
        if broadcast_rpc_address == control_addr {
            let local_info = fetch_control_connection_info(
                control_transport.as_ref(),
                &control_addr,
                self.version,
            )
            .await?;

            return build_node_info(&local_info, broadcast_rpc_address).map(Some);
        }

        send_query(
            &format!("SELECT * FROM {}", self.peer_table_name()),
            control_transport.as_ref(),
            self.version,
        )
        .await
        .map(|peers| {
            peers.and_then(|peers| {
                find_in_peers(&peers, broadcast_rpc_address, control_addr).transpose()
            })
        })?
        .transpose()
    }

    #[inline]
    fn control_transport(&self) -> Result<Arc<T>> {
        self.session_context
            .control_connection_transport
            .load()
            .clone()
            .ok_or_else(|| "Cannot fetch information without a control connection!".into())
    }

    #[inline]
    fn peer_table_name(&self) -> &'static str {
        if self.is_schema_v2.load(Ordering::Relaxed) {
            "system.peers_v2"
        } else {
            "system.peers"
        }
    }

    #[inline]
    pub fn metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.metadata.load().clone()
    }

    // Refreshes stored metadata. Note: it is expected to be called by the control connection.
    pub async fn refresh_metadata(&self) -> Result<()> {
        let (node_infos, keyspaces) =
            tokio::try_join!(self.refresh_node_infos(), self.refresh_keyspaces())?;

        if self
            .did_initial_refresh
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.metadata.store(Arc::new(
                build_initial_metadata(
                    node_infos,
                    keyspaces,
                    &self.contact_points,
                    &self.connection_manager,
                    self.node_distance_evaluator.as_ref(),
                )
                .await,
            ));
        } else {
            self.metadata.rcu(move |old_metadata| {
                refresh_metadata(
                    &node_infos,
                    old_metadata.as_ref(),
                    &self.connection_manager,
                    self.node_distance_evaluator.as_ref(),
                )
            });
        };

        Ok(())
    }

    async fn refresh_keyspaces(&self) -> Result<FxHashMap<String, KeyspaceMetadata>> {
        let control_transport = self.control_transport()?;
        send_query(
            "SELECT keyspace_name, toJson(replication) AS replication FROM system_schema.keyspaces",
            control_transport.as_ref(),
            self.version,
        )
        .await
        .and_then(|rows| {
            rows.map(|rows| rows.iter().map(build_keyspace).try_collect())
                .transpose()
        })
        .map(|keyspaces| keyspaces.unwrap_or_default())
    }

    async fn refresh_node_infos(&self) -> Result<Vec<NodeInfo>> {
        let control_transport = self.control_transport()?;
        let control_addr = control_transport.address();

        let local =
            fetch_control_connection_info(control_transport.as_ref(), &control_addr, self.version)
                .await?;

        if !is_peer_row_valid(&local) {
            return Err("Invalid local row info!".into());
        }

        let local_broadcast_rpc_address = broadcast_rpc_address_from_row(&local, control_addr);
        let local_broadcast_rpc_address =
            build_node_broadcast_rpc_address(&local, local_broadcast_rpc_address, control_addr);

        let mut node_infos = vec![build_node_info(&local, local_broadcast_rpc_address)?];

        let peers = self.query_peers(control_transport.as_ref()).await?;
        if let Some(peers) = peers {
            node_infos.reserve(peers.len());
            node_infos = peers
                .iter()
                .filter_map(|row| {
                    if !is_peer_row_valid(row) {
                        return None;
                    }

                    broadcast_rpc_address_from_row(row, control_addr)
                        .map(|broadcast_rpc_address| build_node_info(row, broadcast_rpc_address))
                })
                .fold_ok(node_infos, |mut node_infos, node_info| {
                    node_infos.push(node_info);
                    node_infos
                })?;
        }

        Ok(node_infos)
    }

    async fn query_peers(&self, transport: &T) -> Result<Option<Vec<Row>>> {
        let peers_v2_result =
            send_query("SELECT * FROM system.peers_v2", transport, self.version).await;
        match peers_v2_result {
            Ok(result) => Ok(result),
            // peers_v2 does not exist
            Err(Error::Server(CdrsError {
                additional_info: AdditionalErrorInfo::Invalid,
                ..
            })) => {
                self.is_schema_v2.store(false, Ordering::Relaxed);
                send_query("SELECT * FROM system.peers", transport, self.version).await
            }
            Err(error) => Err(error),
        }
    }
}
