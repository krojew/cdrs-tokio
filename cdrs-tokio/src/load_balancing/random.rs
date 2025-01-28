use derivative::Derivative;
use std::marker::PhantomData;

use rand::prelude::*;
use rand::rng;

use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Pure random load balancing.
#[derive(Default, Derivative)]
#[derivative(Debug)]
pub struct RandomLoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    #[derivative(Debug = "ignore")]
    _transport: PhantomData<T>,
    #[derivative(Debug = "ignore")]
    _connection_manager: PhantomData<CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RandomLoadBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RandomLoadBalancingStrategy {
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RandomLoadBalancingStrategy<T, CM>
{
    fn query_plan(
        &self,
        _request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        let mut result = cluster.unignored_nodes();

        result.shuffle(&mut rng());
        result
    }
}
