#[cfg(test)]
use mockall::*;

use crate::cluster::topology::NodeDistance;
use crate::cluster::NodeInfo;

/// A node distance evaluator evaluates given node distance in relation to the driver.
#[cfg_attr(test, automock)]
pub trait NodeDistanceEvaluator {
    /// Tries to compute a distance to a given node. Can return `None` if the distance cannot be
    /// determined. In such case, the driver will try to "guess" the distance based on
    /// configuration, but this process might not always be possible. Nodes without a distance are
    /// expected to be ignored by load balancers.
    fn compute_distance(&self, node: &NodeInfo) -> Option<NodeDistance>;
}

/// A simple evaluator which treats all nodes as local.
#[derive(Default)]
pub struct AllLocalNodeDistanceEvaluator;

impl NodeDistanceEvaluator for AllLocalNodeDistanceEvaluator {
    fn compute_distance(&self, _node: &NodeInfo) -> Option<NodeDistance> {
        Some(NodeDistance::Local)
    }
}
