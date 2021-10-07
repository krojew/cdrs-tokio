mod random;
mod request;
mod round_robin;

use std::sync::Arc;

use crate::cluster::{ConnectionManager, Node};
pub use crate::load_balancing::random::RandomLoadBalancingStrategy;
pub use crate::load_balancing::request::Request;
pub use crate::load_balancing::round_robin::RoundRobinBalancingStrategy;
use crate::transport::CdrsTransport;

pub type QueryPlan<T, CM> = Vec<Arc<Node<T, CM>>>;

/// Load balancing strategy, usually used for managing target node connections.
pub trait LoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    /// Initializes the strategy with given connection managers to nodes.
    fn init(&mut self, connection_managers: Vec<Arc<Node<T, CM>>>);

    /// Returns query plan for given request.
    fn query_plan(&self, request: Request) -> QueryPlan<T, CM>;
}
