use derivative::Derivative;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Round-robin load balancing.
#[derive(Derivative, Default)]
#[derivative(Debug)]
pub struct RoundRobinLoadBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    prev_idx: AtomicUsize,
    #[derivative(Debug = "ignore")]
    _transport: PhantomData<T>,
    #[derivative(Debug = "ignore")]
    _connection_manager: PhantomData<CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RoundRobinLoadBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RoundRobinLoadBalancingStrategy {
            prev_idx: AtomicUsize::new(0),
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RoundRobinLoadBalancingStrategy<T, CM>
{
    fn query_plan(
        &self,
        _request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        let mut nodes = cluster.unignored_nodes();
        if nodes.is_empty() {
            return nodes;
        }

        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst) % nodes.len();

        nodes.rotate_left(cur_idx);
        nodes
    }
}
