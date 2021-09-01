use std::sync::Arc;

mod random;
mod round_robin;
mod single_node;

pub use crate::load_balancing::random::Random;
pub use crate::load_balancing::round_robin::RoundRobin;
pub use crate::load_balancing::single_node::SingleNode;

/// Load balancing strategy, usually used for managing target node connections.
pub trait LoadBalancingStrategy<N> {
    fn init(&mut self, cluster: Vec<Arc<N>>);
    fn next(&self) -> Option<Arc<N>>;
    fn size(&self) -> usize;

    fn find<F>(&self, filter: F) -> Option<Arc<N>>
    where
        F: FnMut(&N) -> bool;
}
