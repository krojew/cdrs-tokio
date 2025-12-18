mod initializing_wrapper;
pub mod node_distance_evaluator;
mod random;
mod request;
mod round_robin;
mod topology_aware;

pub(crate) use self::initializing_wrapper::InitializingWrapperLoadBalancingStrategy;
pub use self::random::RandomLoadBalancingStrategy;
pub use self::request::Request;
pub use self::round_robin::RoundRobinLoadBalancingStrategy;
pub use self::topology_aware::TopologyAwareLoadBalancingStrategy;
use crate::cluster::topology::Node;
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::transport::CdrsTransport;
use derive_more::Constructor;
use std::sync::Arc;

#[derive(Debug, Constructor, Default)]
pub struct QueryPlan<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static> {
    pub nodes: Vec<Arc<Node<T, CM>>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> Clone for QueryPlan<T, CM> {
    fn clone(&self) -> Self {
        QueryPlan::new(self.nodes.clone())
    }
}

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
