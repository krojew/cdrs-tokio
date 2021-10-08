mod random;
mod request;
mod round_robin;

use std::sync::Arc;

use crate::cluster::{ClusterMetadata, ConnectionManager, Node};
pub use crate::load_balancing::random::RandomLoadBalancingStrategy;
pub use crate::load_balancing::request::Request;
pub use crate::load_balancing::round_robin::RoundRobinBalancingStrategy;
use crate::transport::CdrsTransport;

pub type QueryPlan<T, CM> = Vec<Arc<Node<T, CM>>>;

/// Load balancing strategy, usually used for managing target node connections.
pub trait LoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    /// Returns query plan for given request.  If no request is given, return a generic plan for
    /// establishing connection(s) to node(s).
    fn query_plan(
        &self,
        request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM>;
}
