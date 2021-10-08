use std::marker::PhantomData;

use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Round-robin load balancing.
#[derive(Default)]
pub struct RoundRobinBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RoundRobinBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RoundRobinBalancingStrategy {
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RoundRobinBalancingStrategy<T, CM>
{
    fn query_plan(
        &self,
        _request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        // TODO: do actual dc aware round-robin
        cluster.all_nodes().clone()
    }
}
