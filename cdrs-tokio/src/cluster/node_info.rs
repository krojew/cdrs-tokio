use derive_more::Constructor;
use std::net::SocketAddr;
use uuid::Uuid;

use cassandra_protocol::query::query_params::Murmur3Token;

/// Information about a node.
#[derive(Debug, Constructor, Clone)]
pub struct NodeInfo {
    pub host_id: Uuid,
    pub broadcast_rpc_address: SocketAddr,
    pub broadcast_address: Option<SocketAddr>,
    pub datacenter: String,
    pub tokens: Vec<Murmur3Token>,
    pub rack: String,
}
