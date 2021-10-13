use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug)]
pub struct NodeInfo {
    pub host_id: Uuid,
    pub broadcast_rpc_address: SocketAddr,
}

impl NodeInfo {
    pub fn new(host_id: Uuid, broadcast_rpc_address: SocketAddr) -> Self {
        NodeInfo {
            host_id,
            broadcast_rpc_address,
        }
    }
}
