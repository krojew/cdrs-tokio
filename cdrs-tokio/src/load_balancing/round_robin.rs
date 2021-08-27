use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::LoadBalancingStrategy;

#[derive(Debug)]
pub struct RoundRobin<N> {
    cluster: Vec<Arc<N>>,
    prev_idx: AtomicUsize,
}

impl<N> RoundRobin<N> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<N> Default for RoundRobin<N> {
    fn default() -> Self {
        RoundRobin {
            cluster: vec![],
            prev_idx: Default::default(),
        }
    }
}

impl<N> From<Vec<Arc<N>>> for RoundRobin<N> {
    fn from(cluster: Vec<Arc<N>>) -> RoundRobin<N> {
        RoundRobin {
            prev_idx: AtomicUsize::new(0),
            cluster,
        }
    }
}

impl<N> LoadBalancingStrategy<N> for RoundRobin<N>
where
    N: Sync + Send,
{
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns next node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst);
        self.cluster.get(cur_idx % self.cluster.len()).cloned()
    }

    fn size(&self) -> usize {
        self.cluster.len()
    }

    fn find<F>(&self, mut filter: F) -> Option<Arc<N>>
    where
        F: FnMut(&N) -> bool,
    {
        self.cluster.iter().find(|node| filter(*node)).cloned()
    }

    fn remove_node<F>(&mut self, mut filter: F)
    where
        F: FnMut(&N) -> bool,
    {
        if let Some(i) = self.cluster.iter().position(|node| filter(node)) {
            self.cluster.remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        let nodes = vec!["a", "b", "c"];
        let nodes_c = nodes.clone();
        let load_balancer = RoundRobin::from(
            nodes
                .iter()
                .map(|value| Arc::new(*value))
                .collect::<Vec<Arc<&str>>>(),
        );
        for i in 0..10 {
            assert_eq!(&nodes_c[i % 3], load_balancer.next().unwrap().as_ref());
        }
    }

    #[test]
    fn remove_from_round_robin() {
        let nodes = vec!["a", "b"];
        let mut load_balancer = RoundRobin::from(
            nodes
                .iter()
                .map(|value| Arc::new(*value))
                .collect::<Vec<Arc<&str>>>(),
        );
        assert_eq!(&"a", load_balancer.next().unwrap().as_ref());

        load_balancer.remove_node(|n| n == &"a");
        assert_eq!(&"b", load_balancer.next().unwrap().as_ref());
    }
}
