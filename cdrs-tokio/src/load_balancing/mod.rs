mod initializing_wrapper;
mod random;
mod request;
mod round_robin;

use std::sync::Arc;

pub(crate) use self::initializing_wrapper::InitializingWrapperLoadBalancingStrategy;
pub use self::random::RandomLoadBalancingStrategy;
pub use self::request::Request;
pub use self::round_robin::RoundRobinBalancingStrategy;
use crate::cluster::topology::Node;
use crate::cluster::{ClusterMetadata, ConnectionManager};
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
