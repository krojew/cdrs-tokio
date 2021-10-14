use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug)]
pub struct NodeInfo {
    pub host_id: Uuid,
    pub broadcast_rpc_address: SocketAddr,
    pub broadcast_address: Option<SocketAddr>,
}

impl NodeInfo {
    pub fn new(
        host_id: Uuid,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
    ) -> Self {
        NodeInfo {
            host_id,
            broadcast_rpc_address,
            broadcast_address,
        }
    }
}
