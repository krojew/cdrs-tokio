use derivative::Derivative;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::cluster::Murmur3Token;

/// Information about a node.
///
/// New fields may be added in future releases. To build it, e.g. for testing a custom
/// [`NodeDistanceEvaluator`](crate::load_balancing::node_distance_evaluator::NodeDistanceEvaluator),
/// use [`NodeInfo::new`].
#[derive(Clone, Derivative)]
#[derivative(Debug)]
#[non_exhaustive]
pub struct NodeInfo {
    pub host_id: Uuid,
    pub broadcast_rpc_address: SocketAddr,
    pub broadcast_address: Option<SocketAddr>,
    pub datacenter: String,
    #[derivative(Debug = "ignore")]
    pub tokens: Vec<Murmur3Token>,
    pub rack: String,
}

impl NodeInfo {
    /// Creates node information. Fields added in future releases get their default values.
    pub fn new(
        host_id: Uuid,
        broadcast_rpc_address: SocketAddr,
        broadcast_address: Option<SocketAddr>,
        datacenter: String,
        tokens: Vec<Murmur3Token>,
        rack: String,
    ) -> Self {
        NodeInfo {
            host_id,
            broadcast_rpc_address,
            broadcast_address,
            datacenter,
            tokens,
            rack,
        }
    }
}
