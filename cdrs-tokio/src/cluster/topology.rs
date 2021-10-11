use crate as cdrs_tokio;
use crate::cluster::routing::Token;
use crate::error;
use crate::frame::Frame;
use crate::query::utils::prepare_flags;
use crate::query::{Query, QueryParamsBuilder};
use crate::transport::CdrsTransport;

use self::cdrs_tokio::frame::Serialize;
use self::cdrs_tokio::types::prelude::*;
use crate::types::from_cdrs::FromCdrsByName;
use cdrs_tokio_helpers_derive::IntoCdrsValue;
use cdrs_tokio_helpers_derive::TryFromRow;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::str::FromStr;

#[derive(Debug)]
pub struct TopologyInfo {
    pub peers: Vec<Peer>,
    pub keyspaces: HashMap<String, Keyspace>,
}

#[derive(TryFromRow, IntoCdrsValue, Debug)]
pub struct Peer {
    pub address: IpAddr,
    pub port: Option<i16>,
    pub tokens: Vec<Token>,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    pub strategy: Strategy,
}

#[derive(TryFromRow)]
pub struct KeyspaceRow {
    keyspace_name: String,
    replication: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

/// Allows to read current topology info from the cluster
pub struct TopologyReader<T: CdrsTransport + Send + Sync + 'static> {
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport + Send + Sync + 'static> TopologyReader<T> {
    /// Creates new TopologyReader, which connects to known_peers in the background
    //pub fn new(_control_connection: ControlConnection<T, CM, LB>) -> Self {
    pub fn new() -> Self {
        TopologyReader {
            _transport: Default::default(),
        }
    }

    // Fetches current topology info from the cluster
    pub async fn read_topology_info(&mut self, transport: T) -> error::Result<TopologyInfo> {
        let topology = self.fetch_topology_info(&transport).await?;
        Ok(topology)
    }

    async fn fetch_topology_info(&self, transport: &T) -> error::Result<TopologyInfo> {
        query_topology_info(transport).await
    }

    //fn update_known_peers(&mut self, topology_info: &TopologyInfo) {}
}

async fn query_topology_info<T: CdrsTransport>(transport: &T) -> error::Result<TopologyInfo> {
    let peers_query = query_peers(transport);
    let keyspaces_query = query_keyspaces(transport);

    let (peers, keyspaces) = tokio::try_join!(peers_query, keyspaces_query)?;

    // There must be at least one peer
    if peers.is_empty() {
        return Err(error::Error::General(
            "Bad TopologyInfo: peers list is empty".to_string(),
        ));
    }

    // At least one peer has to have some tokens
    if peers.iter().all(|peer| peer.tokens.is_empty()) {
        return Err(error::Error::General(
            "Bad TopoologyInfo: All peers have empty token list".to_string(),
        ));
    }

    Ok(TopologyInfo { peers, keyspaces })
}

async fn query_peers<T: CdrsTransport>(transport: &T) -> error::Result<Vec<Peer>> {
    let peers_query = query(
        "select peer, data_center, rack, tokens from system.peers",
        transport,
    );
    let local_query = query(
        "select rpc_address, data_center, rack, tokens from system.local",
        transport,
    );

    let (peers_res, local_res) = tokio::try_join!(peers_query, local_query)?;

    let connect_port = transport.addr().port();

    let peers_rows: Vec<Peer> = peers_res
        .body()?
        .into_rows()
        .ok_or_else(|| {
            error::Error::General("system.peers query response was not Rows".to_string())
        })?
        .into_iter()
        .map(|row| Peer::try_from_row(row).unwrap())
        .map(|mut row| {
            row.port = Some(connect_port as i16);
            row
        })
        .collect();

    // // For the local node we should use connection's address instead of rpc_address unless SNI is enabled (TODO)
    // // Replace address in local_rows with connection's address
    let local_address: IpAddr = transport.addr().ip();
    let local_rows: Vec<Peer> = local_res
        .body()?
        .into_rows()
        .ok_or_else(|| {
            error::Error::General("system.local query response was not Rows".to_string())
        })?
        .into_iter()
        .map(|row| Peer::try_from_row(row).unwrap())
        .map(|mut row| {
            row.address = local_address;
            row.port = Some(connect_port as i16);
            row
        })
        .collect();

    let mut result: Vec<Peer> = Vec::with_capacity(peers_rows.len() + local_rows.len() + 2);
    result.extend(local_rows);
    result.extend(peers_rows);

    Ok(result)
}

async fn query_keyspaces<T: CdrsTransport>(transport: &T) -> Result<HashMap<String, Keyspace>> {
    let rows: Vec<KeyspaceRow> = query(
        "select keyspace_name, toJson(replication) from system_schema.keyspaces",
        transport,
    )
    .await?
    .body()?
    .into_rows()
    .ok_or_else(|| {
        error::Error::General("system_schema.keyspaces query response was not Rows".to_string())
    })?
    .into_iter()
    .map(|row| KeyspaceRow::try_from_row(row).unwrap())
    .collect();

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows {
        let strategy_map: HashMap<String, String> = json_to_string_map(&row.replication)?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;

        result.insert(row.keyspace_name, Keyspace { strategy });
    }

    Ok(result)
}

async fn query<Q: ToString, T: CdrsTransport>(query: Q, transport: &T) -> error::Result<Frame> {
    let query_params = QueryParamsBuilder::new().finalize();
    let query = Query {
        query: query.to_string(),
        params: query_params,
    };

    let flags = prepare_flags(false, false);
    let query_frame = Frame::new_query(query, flags);

    match transport.write_frame(&query_frame).await {
        Ok(frame) => Ok(frame),
        Err(error) => Err(error),
    }
}

fn json_to_string_map(json_text: &str) -> error::Result<HashMap<String, String>> {
    use serde_json::Value;

    let json: Value = match serde_json::from_str(json_text) {
        Ok(v) => v,
        Err(_e) => return Err(error::Error::General("Stategy was not JSON".to_string())),
    };

    let object_map = match json {
        Value::Object(map) => map,
        _ => {
            return Err(error::Error::General(
                "keyspaces map json is not a json object".to_string(),
            ))
        }
    };

    let mut result = HashMap::with_capacity(object_map.len());

    for (key, val) in object_map.into_iter() {
        match val {
            Value::String(string) => result.insert(key, string),
            _ => {
                return Err(error::Error::General(
                    "json keyspaces map does not contain strings".to_string(),
                ))
            }
        };
    }

    Ok(result)
}

fn strategy_from_string_map(mut strategy_map: HashMap<String, String>) -> Result<Strategy> {
    let strategy_name: String = strategy_map.remove("class").ok_or_else(|| {
        error::Error::General("strategy map should have a 'class' field".to_string())
    })?;

    let strategy: Strategy = match strategy_name.as_str() {
        "org.apache.cassandra.locator.SimpleStrategy" => {
            let rep_factor_str: String =
                strategy_map.remove("replication_factor").ok_or_else(|| {
                    error::Error::General(
                        "SimpleStrategy in strategy map does not have a replication factor"
                            .to_string(),
                    )
                })?;

            let replication_factor: usize = usize::from_str(&rep_factor_str).map_err(|_| {
                error::Error::General(
                    "Could not parse replication factor as an integer".to_string(),
                )
            })?;

            Strategy::SimpleStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.NetworkTopologyStrategy" => {
            let mut datacenter_repfactors: HashMap<String, usize> =
                HashMap::with_capacity(strategy_map.len());

            for (datacenter, rep_factor_str) in strategy_map.drain() {
                let rep_factor: usize = match usize::from_str(&rep_factor_str) {
                    Ok(number) => number,
                    Err(_) => continue, // There might be other things in the map, we care only about rep_factors
                };

                datacenter_repfactors.insert(datacenter, rep_factor);
            }

            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }
        }
        "org.apache.cassandra.locator.LocalStrategy" => Strategy::LocalStrategy,
        _ => Strategy::Other {
            name: strategy_name,
            data: strategy_map,
        },
    };

    Ok(strategy)
}
