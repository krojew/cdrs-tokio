use rand::prelude::*;
use std::sync::Arc;

use super::LoadBalancingStrategy;

/// Load balancing based on selecting a random node.
pub struct Random<N> {
    pub cluster: Vec<Arc<N>>,
}

impl<N> Random<N> {
    pub fn new(cluster: Vec<Arc<N>>) -> Self {
        Random { cluster }
    }
}

impl<N> From<Vec<Arc<N>>> for Random<N> {
    fn from(cluster: Vec<Arc<N>>) -> Random<N> {
        Random { cluster }
    }
}

impl<N> LoadBalancingStrategy<N> for Random<N>
where
    N: Sync,
{
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns next random node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        let len = self.cluster.len();
        if len == 0 {
            return None;
        }
        self.cluster.choose(&mut thread_rng()).cloned()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_random() {
        let nodes = vec!["a", "b", "c", "d", "e", "f", "g"];
        let load_balancer = Random::from(
            nodes
                .iter()
                .map(|value| Arc::new(*value))
                .collect::<Vec<Arc<&str>>>(),
        );
        for _ in 0..100 {
            let s = load_balancer.next();
            assert!(s.is_some());
        }
    }
}
