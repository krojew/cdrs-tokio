use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cluster::{ConnectionManager, Node};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Simple round-robin load balancing.
#[derive(Default)]
pub struct RoundRobinBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    cluster: Vec<Arc<Node<T, CM>>>,
    prev_idx: AtomicUsize,
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RoundRobinBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RoundRobinBalancingStrategy {
            cluster: Default::default(),
            prev_idx: Default::default(),
            _transport: Default::default(),
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> LoadBalancingStrategy<T, CM>
    for RoundRobinBalancingStrategy<T, CM>
{
    fn init(&mut self, connection_managers: Vec<Arc<Node<T, CM>>>) {
        self.cluster = connection_managers;
        self.prev_idx = AtomicUsize::new(0);
    }

    fn query_plan(&self, _request: Request) -> QueryPlan<T, CM> {
        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst);

        if let Some(node) = self.cluster.get(cur_idx % self.cluster.len()) {
            vec![node.clone()]
        } else {
            vec![]
        }
    }
}
