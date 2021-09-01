use std::sync::Arc;

use super::LoadBalancingStrategy;

/// Load balancing strategy always returning the first node.
pub struct SingleNode<N> {
    cluster: Vec<Arc<N>>,
}

impl<N> SingleNode<N> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<N> Default for SingleNode<N> {
    fn default() -> Self {
        SingleNode { cluster: vec![] }
    }
}

impl<N> From<Vec<Arc<N>>> for SingleNode<N> {
    fn from(cluster: Vec<Arc<N>>) -> SingleNode<N> {
        SingleNode { cluster }
    }
}

impl<N> LoadBalancingStrategy<N> for SingleNode<N>
where
    N: Sync + Send,
{
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns first node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        self.cluster.get(0).cloned()
    }

    fn size(&self) -> usize {
        if self.cluster.is_empty() {
            0
        } else {
            1
        }
    }

    fn find<F>(&self, mut filter: F) -> Option<Arc<N>>
    where
        F: FnMut(&N) -> bool,
    {
        let node = self.cluster.get(0)?;
        if filter(node) {
            Some(node.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node() {
        let node = "a";
        let load_balancer = SingleNode::from(vec![Arc::new(node)]);
        assert_eq!(node, *load_balancer.next().unwrap());
        // and one more time to check
        assert_eq!(node, *load_balancer.next().unwrap());
    }
}
