use std::marker::PhantomData;
use std::sync::Arc;

use crate::cluster::{ConnectionManager, Node};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Round-robin load balancing.
#[derive(Default)]
pub struct RoundRobinBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    cluster: Vec<Arc<Node<T, CM>>>,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RoundRobinBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RoundRobinBalancingStrategy {
            cluster: Default::default(),
            _transport: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RoundRobinBalancingStrategy<T, CM>
{
    fn init(&mut self, connection_managers: Vec<Arc<Node<T, CM>>>) {
        self.cluster = connection_managers;
    }

    fn query_plan(&self, _request: Option<Request>) -> QueryPlan<T, CM> {
        // TODO: do actual dc aware round-robin
        self.cluster.clone()
    }
}
