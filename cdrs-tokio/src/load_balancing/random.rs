use std::marker::PhantomData;
use std::sync::Arc;

use rand::prelude::*;
use rand::thread_rng;

use crate::cluster::{ConnectionManager, Node};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Pure random load balancing.
#[derive(Default)]
pub struct RandomLoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    cluster: Vec<Arc<Node<T, CM>>>,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RandomLoadBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RandomLoadBalancingStrategy {
            cluster: Default::default(),
            _transport: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RandomLoadBalancingStrategy<T, CM>
{
    fn init(&mut self, connection_managers: Vec<Arc<Node<T, CM>>>) {
        self.cluster = connection_managers;
    }

    fn query_plan(&self, _request: Option<Request>) -> QueryPlan<T, CM> {
        let mut result = self.cluster.clone();
        result.shuffle(&mut thread_rng());
        result
    }
}
