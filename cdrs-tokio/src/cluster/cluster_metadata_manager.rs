use arc_swap::ArcSwap;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::events::{SchemaChange, ServerEvent};
use cassandra_protocol::frame::events::{
    SchemaChangeOptions, SchemaChangeType, StatusChange, StatusChangeType, TopologyChange,
    TopologyChangeType,
};
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::{Envelope, Flags, Version};
use cassandra_protocol::query::{QueryParams, QueryParamsBuilder, QueryValues};
use cassandra_protocol::types::list::List;
use cassandra_protocol::types::rows::Row;
use cassandra_protocol::types::{AsRustType, ByName, IntoRustByName};
use fxhash::FxHashMap;
use itertools::Itertools;
use rand::{rng, RngExt};
use serde_json::{Map, Value as JsonValue};
use std::convert::TryInto;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tracing::*;

use crate::cluster::connection_pool::{ConnectionPoolConfig, ConnectionPoolFactory};
use crate::cluster::metadata_builder::{add_new_node, build_initial_metadata, refresh_metadata};
use crate::cluster::topology::{KeyspaceMetadata, Node, NodeState, ReplicationStrategy};
use crate::cluster::Murmur3Token;
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::cluster::{NodeInfo, SessionContext};
use crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use crate::transport::CdrsTransport;

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
    beta_protocol: bool,
) -> Result<Option<Vec<Row>>> {
    let query_params = QueryParamsBuilder::new().build();
    send_query_with_params(query, query_params, transport, version, beta_protocol).await
}

async fn send_query_with_values<T: CdrsTransport, V: Into<QueryValues>>(
    query: &str,
    values: V,
    transport: &T,
    version: Version,
    beta_protocol: bool,
) -> Result<Option<Vec<Row>>> {
    let query_params = QueryParamsBuilder::new().with_values(values.into()).build();
    send_query_with_params(query, query_params, transport, version, beta_protocol).await
}

async fn send_query_with_params<T: CdrsTransport>(
    query: &str,
    query_params: QueryParams,
    transport: &T,
    version: Version,
    beta_protocol: bool,
) -> Result<Option<Vec<Row>>> {
    let query = BodyReqQuery {
        query: query.to_string(),
        query_params,
    };

    let flags = if beta_protocol {
        Flags::BETA
    } else {
        Flags::empty()
    };

    let envelope = Envelope::new_query(query, flags, version);

    transport
        .write_envelope(&envelope, false)
        .await
        .and_then(|envelope| envelope.response_body())
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
                .map(|token| {
                    token.try_into().unwrap_or_else(|_| {
                    warn!(%broadcast_rpc_address, "Unsupported token type - using a dummy value.");
                    Murmur3Token::new(rng().random()) })
                })
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
    beta_protocol: bool,
) -> Result<Row> {
    send_query(
        "SELECT * FROM system.local",
        control_transport,
        version,
        beta_protocol,
    )
    .await?
    .and_then(|mut rows| rows.pop())
    .ok_or_else(|| format!("Node {control_addr} failed to return info about itself!").into())
}

fn build_keyspace(row: &Row) -> Result<(String, KeyspaceMetadata)> {
    let keyspace_name = row.get_r_by_name("keyspace_name")?;

    let replication: String = row.get_r_by_name("replication")?;
    let replication: JsonValue = serde_json::from_str(&replication).map_err(|error| {
        Error::General(format!(
            "Error parsing replication for {keyspace_name}: {error}"
        ))
    })?;

    let replication_strategy = match replication {
        JsonValue::Object(properties) => build_replication_strategy(properties)?,
        _ => {
            return Err(Error::InvalidReplicationFormat {
                keyspace: keyspace_name,
            })
        }
    };

    Ok((keyspace_name, KeyspaceMetadata::new(replication_strategy)))
}

fn build_replication_strategy(
    mut properties: Map<String, JsonValue>,
) -> Result<ReplicationStrategy> {
    match properties.remove("class") {
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

            result.map_err(|error| {
                format!("Failed to parse ('{replication_factor}'): {error}").into()
            })
        }
        _ => Err("Missing replication factor!".into()),
    }
}

pub(crate) struct ClusterMetadataManager<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
> {
    metadata: ArcSwap<ClusterMetadata<T, CM>>,
    contact_points: Vec<Arc<Node<T, CM>>>,
    connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
    did_initial_refresh: AtomicBool,
    is_schema_v2: AtomicBool,
    session_context: Arc<SessionContext<T>>,
    node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
    version: Version,
    beta_protocol: bool,
}

impl<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> ClusterMetadataManager<T, CM> {
    pub(crate) fn new(
        contact_points: Vec<Arc<Node<T, CM>>>,
        connection_pool_factory: Arc<ConnectionPoolFactory<T, CM>>,
        session_context: Arc<SessionContext<T>>,
        node_distance_evaluator: Box<dyn NodeDistanceEvaluator + Send + Sync>,
        version: Version,
        beta_protocol: bool,
    ) -> Self {
        ClusterMetadataManager {
            metadata: ArcSwap::from_pointee(ClusterMetadata::default()),
            contact_points,
            connection_pool_factory,
            did_initial_refresh: AtomicBool::new(false),
            is_schema_v2: AtomicBool::new(true),
            session_context,
            node_distance_evaluator,
            version,
            beta_protocol,
        }
    }

    pub(crate) fn listen_to_events(self: &Arc<Self>, mut event_receiver: Receiver<ServerEvent>) {
        let cmm = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let event = event_receiver.recv().await;
                match event {
                    Ok(event) => {
                        if let Some(cmm) = cmm.upgrade() {
                            cmm.process_event(event).await;
                        } else {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!("Skipped {} events - refreshing metadata.", n);

                        if let Some(cmm) = cmm.upgrade() {
                            if let Err(error) = cmm.refresh_metadata(true).await {
                                error!(%error, "Error refreshing metadata after skipping events!");
                            }
                        } else {
                            break;
                        }
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    async fn process_event(&self, event: ServerEvent) {
        debug!(?event);

        match event {
            ServerEvent::TopologyChange(event) => self.process_topology_event(event).await,
            ServerEvent::StatusChange(event) => self.process_status_event(event).await,
            ServerEvent::SchemaChange(event) => self.process_schema_event(event).await,
            _ => warn!(?event, "Unrecognized event."),
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
                _ => warn!(?event, "Unrecognized schema event."),
            }
        }
    }

    async fn process_topology_event(&self, event: TopologyChange) {
        match event.change_type {
            TopologyChangeType::NewNode => {
                if self.metadata.load().has_node_by_rpc_address(event.addr) {
                    debug!(
                        broadcast_rpc_address = %event.addr,
                        "Trying to add already existing node - ignoring."
                    );
                } else {
                    self.add_new_node(event.addr, NodeState::Unknown).await;
                }
            }
            TopologyChangeType::RemovedNode => {
                // RemovedNode is a pure transform - use rcu so a concurrent
                // metadata update (e.g. another event landing at the same
                // time) isn't lost between load() and store().
                debug!(broadcast_rpc_address = %event.addr, "Removing node from cluster (if present).");
                self.metadata
                    .rcu(|metadata| Arc::new(metadata.clone_without_node(event.addr)));
            }
            TopologyChangeType::MovedNode => self.refresh_moved_node(event.addr).await,
            _ => warn!(?event, "Unrecognized topology change type."),
        }
    }

    async fn process_status_event(&self, event: StatusChange) {
        // Known nodes change state in place, which every node sharing their connectivity sees, so
        // metadata only changes when a forced down node needs fresh connectivity.
        let node = self.metadata.load().find_node_by_rpc_address(event.addr);
        match event.change_type {
            StatusChangeType::Up => match node {
                Some(node) if node.promote_up() => {
                    debug!(?node, "Set existing node state to up.");
                }
                Some(_) => {
                    // The rcu closure can run more than once, so the in-place change on a node
                    // which is no longer forced down is idempotent.
                    self.metadata.rcu(|metadata| {
                        match metadata.find_node_by_rpc_address(event.addr) {
                            Some(node) if !node.promote_up() => {
                                debug!(?node, "Recreating forced down node as up.");
                                let new_node = node.clone_with_node_state(NodeState::Up);
                                Arc::new(metadata.clone_with_node(new_node))
                            }
                            _ => metadata.clone(),
                        }
                    });
                }
                None => self.add_new_node(event.addr, NodeState::Up).await,
            },
            StatusChangeType::Down => match node {
                Some(node) => {
                    let state = node.state();
                    if state != NodeState::Down
                        && state != NodeState::ForcedDown
                        && node.is_any_connection_up().await
                    {
                        warn!(
                            ?node,
                            "Marking node as down while there are established connections."
                        );
                    }

                    // forced down nodes stay that way
                    if node.try_set_state(NodeState::Down) {
                        debug!(?node, "Set existing node state to down.");
                    }
                }
                None => debug!(broadcast_rpc_address = %event.addr, "Unknown node down."),
            },
            _ => warn!(?event, "Unrecognized status event."),
        }
    }

    fn remove_keyspace(&self, keyspace: &str) {
        // Use rcu so that a concurrent metadata update (e.g. another schema
        // event arriving in parallel) cannot be silently overwritten between
        // load() and store(). The closure may run more than once if the
        // ArcSwap loses a CAS race.
        self.metadata
            .rcu(|metadata| Arc::new(metadata.clone_without_keyspace(keyspace)));
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
            self.version,
            self.beta_protocol,
        )
        .await
        .map(|rows| { rows.and_then(|mut rows| rows.pop()) })
        .and_then(|row| {
            match row {
                Some(row) => {
                    let (keyspace_name, keyspace) = build_keyspace(&row)?;
                    // Use rcu so that a concurrent metadata mutation (e.g.
                    // another schema event landing in parallel, or the
                    // status/topology event handlers) cannot be silently
                    // overwritten between load() and store(). The closure
                    // may run more than once if the ArcSwap loses a CAS
                    // race, so we clone the freshly-built keyspace data
                    // each iteration.
                    self.metadata.rcu(|metadata| {
                        Arc::new(
                            metadata
                                .clone_with_keyspace(keyspace_name.clone(), keyspace.clone()),
                        )
                    });
                }
                None => {
                    warn!(%keyspace, "Keyspace to refresh disappeared.");
                    self.remove_keyspace(keyspace);
                }
            }

            Ok(())
        })
    }

    async fn add_new_node(&self, broadcast_rpc_address: SocketAddr, state: NodeState) {
        debug!(%broadcast_rpc_address, %state, "Adding new node to metadata.");

        let new_node_info = self.find_new_node_info(broadcast_rpc_address).await;
        match new_node_info {
            Ok(Some(new_node_info)) => {
                // metadata might have changed while waiting for node info
                self.metadata.rcu(|metadata| {
                    add_new_node(
                        new_node_info.clone(),
                        metadata.as_ref(),
                        &self.connection_pool_factory,
                        self.node_distance_evaluator.as_ref(),
                        state,
                    )
                    .map(Arc::new)
                    .unwrap_or_else(|| metadata.clone())
                });
            }
            Ok(None) => {
                warn!(%broadcast_rpc_address, "Cannot find new node info. Ignoring new node.");
            }
            Err(error) => {
                error!(%error, %broadcast_rpc_address, "Error finding new node info!");
            }
        }
    }

    async fn refresh_moved_node(&self, broadcast_rpc_address: SocketAddr) {
        debug!(%broadcast_rpc_address, "Refreshing moved node.");

        let node_info = self.find_new_node_info(broadcast_rpc_address).await;
        match node_info {
            Ok(Some(node_info)) => {
                self.metadata.rcu(|metadata| {
                    match metadata.find_node_by_host_id(&node_info.host_id) {
                        Some(node) => {
                            Arc::new(metadata.clone_with_node(node.clone_with_node_info(
                                node_info.clone(),
                                self.node_distance_evaluator.as_ref(),
                            )))
                        }
                        None => {
                            debug!(%broadcast_rpc_address, "Unknown node moved - ignoring.");
                            metadata.clone()
                        }
                    }
                });
            }
            Ok(None) => {
                warn!(%broadcast_rpc_address, "Cannot find moved node info. Ignoring moved node.");
            }
            Err(error) => {
                error!(%error, %broadcast_rpc_address, "Error finding moved node info!");
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
                self.beta_protocol,
            )
            .await?;

            return build_node_info(&local_info, broadcast_rpc_address).map(Some);
        }

        send_query(
            &format!("SELECT * FROM {}", self.peer_table_name()),
            control_transport.as_ref(),
            self.version,
            self.beta_protocol,
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
    pub(crate) fn metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.metadata.load().clone()
    }

    #[inline]
    pub(crate) fn connection_pool_config(&self) -> &ConnectionPoolConfig {
        self.connection_pool_factory.config()
    }

    #[inline]
    pub(crate) fn find_node_by_rpc_address(
        &self,
        broadcast_rpc_address: SocketAddr,
    ) -> Option<Arc<Node<T, CM>>> {
        self.metadata
            .load()
            .find_node_by_rpc_address(broadcast_rpc_address)
    }

    // Refreshes stored metadata. Note: it is expected to be called by the control connection.
    pub(crate) async fn refresh_metadata(&self, full_refresh: bool) -> Result<()> {
        let (node_infos, keyspaces) =
            tokio::try_join!(self.refresh_node_infos(), self.refresh_keyspaces())?;

        if self
            .did_initial_refresh
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.metadata.store(Arc::new(build_initial_metadata(
                node_infos,
                keyspaces,
                &self.contact_points,
                None,
                &self.connection_pool_factory,
                self.node_distance_evaluator.as_ref(),
            )));
        } else {
            self.metadata.rcu(move |old_metadata| {
                if full_refresh {
                    build_initial_metadata(
                        node_infos.clone(),
                        keyspaces.clone(),
                        &self.contact_points,
                        Some(old_metadata.as_ref()),
                        &self.connection_pool_factory,
                        self.node_distance_evaluator.as_ref(),
                    )
                } else {
                    refresh_metadata(
                        &node_infos,
                        keyspaces.clone(),
                        old_metadata.as_ref(),
                        &self.connection_pool_factory,
                        self.node_distance_evaluator.as_ref(),
                    )
                }
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
            self.beta_protocol,
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

        let local = fetch_control_connection_info(
            control_transport.as_ref(),
            &control_addr,
            self.version,
            self.beta_protocol,
        )
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
        if !self.is_schema_v2.load(Ordering::Relaxed) {
            // we've already checked for v2 before, so proceed with legacy peers
            return self.query_legacy_peers(transport).await;
        }

        let peers_v2_result = send_query(
            "SELECT * FROM system.peers_v2",
            transport,
            self.version,
            self.beta_protocol,
        )
        .await;

        match peers_v2_result {
            Ok(result) => Ok(result),
            // peers_v2 does not exist
            Err(Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Invalid,
                        ..
                    },
                ..
            }) => {
                self.is_schema_v2.store(false, Ordering::Relaxed);
                self.query_legacy_peers(transport).await
            }
            Err(Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Server,
                        ref message,
                        ..
                    },
                ..
            }) if message.contains("Unknown keyspace/cf pair (system.peers_v2)") => {
                self.is_schema_v2.store(false, Ordering::Relaxed);
                self.query_legacy_peers(transport).await
            }
            Err(error) => Err(error),
        }
    }

    #[inline]
    async fn query_legacy_peers(&self, transport: &T) -> Result<Option<Vec<Row>>> {
        send_query(
            "SELECT * FROM system.peers",
            transport,
            self.version,
            self.beta_protocol,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_protocol::frame::message_request::RequestBody;
    use cassandra_protocol::frame::message_result::{
        BodyResResultRows, ColSpec, ColType, ColTypeOption, ColTypeOptionValue, ResResultBody,
        RowsMetadata, RowsMetadataFlags, TableSpec,
    };
    use cassandra_protocol::frame::{Direction, Opcode, Serialize};
    use cassandra_protocol::types::CBytes;
    use futures::future::ready;
    use futures::FutureExt;
    use std::net::Ipv4Addr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use tokio::sync::{oneshot, watch};
    use uuid::Uuid;

    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::test_support::{
        self, break_connections, create_slowly_reconnecting_connection_pool_factory, void_response,
        wait_for_connection, ConnectionTracker, TestConnectionManager, TestNode, TestTransport,
    };
    use crate::cluster::topology::NodeDistance;
    use crate::load_balancing::node_distance_evaluator::AllLocalNodeDistanceEvaluator;
    use crate::retry::MockReconnectionPolicy;
    use crate::transport::MockCdrsTransport;

    type TestMetadataManager =
        ClusterMetadataManager<MockCdrsTransport, MockConnectionManager<MockCdrsTransport>>;

    fn address(last_octet: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, last_octet)), 9042)
    }

    fn simple_type(id: ColType) -> ColTypeOption {
        ColTypeOption { id, value: None }
    }

    fn column(name: &str, col_type: ColTypeOption) -> ColSpec {
        ColSpec::new(None, name, col_type)
    }

    // system.local identifies the node by broadcast_address, system.peers_v2 by peer
    fn node_columns(address_column: &str) -> Vec<ColSpec> {
        vec![
            column("rpc_address", simple_type(ColType::Inet)),
            column(address_column, simple_type(ColType::Inet)),
            column("host_id", simple_type(ColType::Uuid)),
            column("data_center", simple_type(ColType::Varchar)),
            column("rack", simple_type(ColType::Varchar)),
            column(
                "tokens",
                ColTypeOption {
                    id: ColType::Set,
                    value: Some(ColTypeOptionValue::CSet(Box::new(simple_type(
                        ColType::Varchar,
                    )))),
                },
            ),
            column("schema_version", simple_type(ColType::Uuid)),
        ]
    }

    fn node_row(address: SocketAddr, host_id: Uuid, tokens: &[i64]) -> Vec<CBytes> {
        let ip = match address.ip() {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        };

        let mut encoded_tokens = (tokens.len() as i32).to_be_bytes().to_vec();
        for token in tokens {
            let token = token.to_string();
            encoded_tokens.extend_from_slice(&(token.len() as i32).to_be_bytes());
            encoded_tokens.extend_from_slice(token.as_bytes());
        }

        vec![
            CBytes::new(ip.clone()),
            CBytes::new(ip),
            CBytes::new(host_id.as_bytes().to_vec()),
            CBytes::new(b"dc1".to_vec()),
            CBytes::new(b"r1".to_vec()),
            CBytes::new(encoded_tokens),
            CBytes::new(Uuid::new_v4().as_bytes().to_vec()),
        ]
    }

    fn result_envelope(body: ResResultBody) -> Envelope {
        Envelope::new(
            Version::V4,
            Direction::Response,
            Flags::empty(),
            Opcode::Result,
            0,
            body.serialize_to_vec(Version::V4),
            None,
            vec![],
        )
    }

    fn rows_envelope(col_specs: Vec<ColSpec>, rows: Vec<Vec<CBytes>>) -> Envelope {
        result_envelope(ResResultBody::Rows(BodyResResultRows::new(
            RowsMetadata::new(
                RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                col_specs.len() as i32,
                None,
                None,
                Some(TableSpec::new("system", "test")),
                col_specs,
            ),
            rows.len() as i32,
            rows,
            Version::V4,
        )))
    }

    fn query_text(envelope: &Envelope) -> String {
        match envelope.request_body() {
            Ok(RequestBody::Query(query)) => query.query,
            _ => panic!("Expected a query!"),
        }
    }

    fn create_metadata_manager(transport: MockCdrsTransport) -> TestMetadataManager {
        let (_, keyspace_receiver) = watch::channel(None);
        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            MockConnectionManager::new(),
            keyspace_receiver,
            Arc::new(MockReconnectionPolicy::new()),
        ));

        let session_context = Arc::new(SessionContext::default());
        session_context
            .control_connection_transport
            .store(Some(Arc::new(transport)));

        ClusterMetadataManager::new(
            vec![],
            connection_pool_factory,
            session_context,
            Box::new(AllLocalNodeDistanceEvaluator),
            Version::V4,
            false,
        )
    }

    #[tokio::test]
    async fn should_keep_concurrent_changes_when_adding_node() {
        let new_node_address = address(2);
        let new_node_host_id = Uuid::new_v4();

        let (response_sender, response_receiver) = oneshot::channel::<()>();
        let response_receiver = Mutex::new(Some(response_receiver));

        let mut transport = MockCdrsTransport::new();
        transport.expect_address().return_const(address(1));
        transport
            .expect_write_envelope()
            .returning(move |envelope, _| {
                assert_eq!(query_text(envelope), "SELECT * FROM system.peers_v2");

                let response_receiver = response_receiver.lock().unwrap().take().unwrap();
                let response = rows_envelope(
                    node_columns("peer"),
                    vec![node_row(new_node_address, new_node_host_id, &[1])],
                );

                async move {
                    response_receiver.await.unwrap();
                    Ok(response)
                }
                .boxed()
            });

        let metadata_manager = create_metadata_manager(transport);

        // the keyspace is added while the peers query is still in flight
        tokio::join!(
            metadata_manager.add_new_node(new_node_address, NodeState::Up),
            async {
                metadata_manager.metadata.rcu(|metadata| {
                    Arc::new(metadata.clone_with_keyspace(
                        "ks".into(),
                        KeyspaceMetadata::new(ReplicationStrategy::Other),
                    ))
                });
                response_sender.send(()).unwrap();
            }
        );

        let metadata = metadata_manager.metadata();
        assert!(metadata.keyspace("ks").is_some());
        assert!(metadata
            .find_node_by_rpc_address(new_node_address)
            .is_some());
    }

    #[tokio::test]
    async fn should_refresh_metadata_after_skipping_events() {
        let control_address = address(1);
        let control_host_id = Uuid::new_v4();

        let mut transport = MockCdrsTransport::new();
        transport.expect_address().return_const(control_address);
        transport
            .expect_write_envelope()
            .returning(move |envelope, _| {
                let response = match query_text(envelope).as_str() {
                    "SELECT * FROM system.local" => rows_envelope(
                        node_columns("broadcast_address"),
                        vec![node_row(control_address, control_host_id, &[1])],
                    ),
                    "SELECT * FROM system.peers_v2" => rows_envelope(node_columns("peer"), vec![]),
                    _ => result_envelope(ResResultBody::Void),
                };

                async move { Ok(response) }.boxed()
            });

        let metadata_manager = Arc::new(create_metadata_manager(transport));

        let (event_sender, event_receiver) = tokio::sync::broadcast::channel(1);
        let event =
            ServerEvent::StatusChange(StatusChange::new(StatusChangeType::Down, address(3)));

        // the second event overwrites the first one, so the receiver lags behind
        event_sender.send(event.clone()).unwrap();
        event_sender.send(event).unwrap();

        metadata_manager.listen_to_events(event_receiver);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !metadata_manager.metadata().has_nodes() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Metadata should be refreshed after skipping events!");

        assert!(metadata_manager
            .metadata()
            .find_node_by_host_id(&control_host_id)
            .is_some());
    }

    #[tokio::test]
    async fn should_refresh_tokens_of_moved_node() {
        let moved_node_address = address(2);
        let moved_node_host_id = Uuid::new_v4();

        let mut transport = MockCdrsTransport::new();
        transport.expect_address().return_const(address(1));
        transport
            .expect_write_envelope()
            .returning(move |envelope, _| {
                assert_eq!(query_text(envelope), "SELECT * FROM system.peers_v2");

                let response = rows_envelope(
                    node_columns("peer"),
                    vec![node_row(moved_node_address, moved_node_host_id, &[50])],
                );

                async move { Ok(response) }.boxed()
            });

        let metadata_manager = create_metadata_manager(transport);

        let mut nodes = FxHashMap::default();
        nodes.insert(
            moved_node_host_id,
            Arc::new(Node::new_with_state(
                metadata_manager.connection_pool_factory.clone(),
                moved_node_address,
                None,
                Some(moved_node_host_id),
                Some(NodeDistance::Local),
                NodeState::Up,
                vec![Murmur3Token::new(1)],
                "r1".into(),
                "dc1".into(),
            )),
        );
        metadata_manager
            .metadata
            .store(Arc::new(ClusterMetadata::new(nodes, Default::default())));

        metadata_manager
            .process_topology_event(TopologyChange::new(
                TopologyChangeType::MovedNode,
                moved_node_address,
            ))
            .await;

        let metadata = metadata_manager.metadata();
        let node = metadata.find_node_by_host_id(&moved_node_host_id).unwrap();
        assert_eq!(node.tokens(), &[Murmur3Token::new(50)]);
        assert_eq!(node.state(), NodeState::Up);

        let first_ring_node = metadata
            .token_map()
            .nodes_for_token(Murmur3Token::new(0))
            .next()
            .unwrap();
        assert_eq!(first_ring_node.tokens(), &[Murmur3Token::new(50)]);
    }

    // A metadata manager knowing a single up node, whose connection attempts are counted.
    fn create_counting_metadata_manager(
        connect_count: Arc<AtomicUsize>,
    ) -> (
        ClusterMetadataManager<TestTransport, TestConnectionManager>,
        Arc<TestNode>,
    ) {
        create_metadata_manager_with_node(test_support::create_connection_pool_factory(
            TestConnectionManager::new(|_, _| ready(Ok(void_response(Version::V4))).boxed())
                .with_connect_handler(move |_| {
                    connect_count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }),
            Version::V4,
        ))
    }

    // A metadata manager knowing a single up node.
    fn create_metadata_manager_with_node(
        connection_pool_factory: Arc<ConnectionPoolFactory<TestTransport, TestConnectionManager>>,
    ) -> (
        ClusterMetadataManager<TestTransport, TestConnectionManager>,
        Arc<TestNode>,
    ) {
        let node = Arc::new(Node::new_with_state(
            connection_pool_factory.clone(),
            address(1),
            None,
            Some(Uuid::new_v4()),
            Some(NodeDistance::Local),
            NodeState::Up,
            Default::default(),
            "".into(),
            "".into(),
        ));

        let metadata_manager = ClusterMetadataManager::new(
            vec![],
            connection_pool_factory,
            Arc::new(SessionContext::default()),
            Box::new(AllLocalNodeDistanceEvaluator),
            Version::V4,
            false,
        );

        let mut nodes = FxHashMap::default();
        nodes.insert(node.host_id().unwrap(), node.clone());
        metadata_manager
            .metadata
            .store(Arc::new(ClusterMetadata::new(nodes, Default::default())));

        (metadata_manager, node)
    }

    #[tokio::test]
    async fn should_keep_connection_pool_on_status_down_and_up_events() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let (metadata_manager, node) = create_counting_metadata_manager(connect_count.clone());
        node.persistent_connection().await.unwrap();

        metadata_manager
            .process_status_event(StatusChange::new(
                StatusChangeType::Down,
                node.broadcast_rpc_address(),
            ))
            .await;

        let current_node = metadata_manager
            .metadata()
            .find_node_by_rpc_address(node.broadcast_rpc_address())
            .unwrap();
        assert_eq!(current_node.state(), NodeState::Down);
        assert_eq!(node.state(), NodeState::Down);

        metadata_manager
            .process_status_event(StatusChange::new(
                StatusChangeType::Up,
                node.broadcast_rpc_address(),
            ))
            .await;

        let current_node = metadata_manager
            .metadata()
            .find_node_by_rpc_address(node.broadcast_rpc_address())
            .unwrap();
        assert_eq!(current_node.state(), NodeState::Up);
        assert_eq!(node.state(), NodeState::Up);

        current_node.persistent_connection().await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 1);
        assert!(current_node.shares_connectivity_with(&node));
    }

    #[tokio::test]
    async fn should_recreate_connection_pool_when_forced_down_node_comes_up() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let (metadata_manager, node) = create_counting_metadata_manager(connect_count.clone());
        node.persistent_connection().await.unwrap();
        node.force_down();

        metadata_manager
            .process_status_event(StatusChange::new(
                StatusChangeType::Up,
                node.broadcast_rpc_address(),
            ))
            .await;

        let current_node = metadata_manager
            .metadata()
            .find_node_by_rpc_address(node.broadcast_rpc_address())
            .unwrap();
        assert_eq!(current_node.state(), NodeState::Up);
        assert_eq!(node.state(), NodeState::ForcedDown);
        assert!(!current_node.shares_connectivity_with(&node));

        current_node.persistent_connection().await.unwrap();
        assert_eq!(connect_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn should_reconnect_right_away_when_down_node_comes_up() {
        let tracker = ConnectionTracker::default();
        let (metadata_manager, node) = create_metadata_manager_with_node(
            create_slowly_reconnecting_connection_pool_factory(tracker.clone()),
        );
        node.persistent_connection().await.unwrap();
        break_connections(&node, &tracker).await;

        metadata_manager
            .process_status_event(StatusChange::new(
                StatusChangeType::Up,
                node.broadcast_rpc_address(),
            ))
            .await;

        wait_for_connection(&node).await;
        assert_eq!(tracker.count(), 2);
    }
}
