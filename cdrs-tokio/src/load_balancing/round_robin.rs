use itertools::Itertools;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

/// Round-robin load balancing.
#[derive(Default)]
pub struct RoundRobinBalancingStrategy<T: CdrsTransport, CM: ConnectionManager<T>> {
    prev_idx: AtomicUsize,
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> RoundRobinBalancingStrategy<T, CM> {
    pub fn new() -> Self {
        RoundRobinBalancingStrategy {
            prev_idx: AtomicUsize::new(0),
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
        let mut nodes = cluster
            .nodes()
            .iter()
            .filter_map(|(_, node)| node.is_ignored().then(|| node.clone()))
            .collect_vec();

        if nodes.is_empty() {
            return nodes;
        }

        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst) % nodes.len();

        nodes.rotate_left(cur_idx);
        nodes
    }
}
