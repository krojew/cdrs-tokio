#[cfg(test)]
use mockall::*;

use crate::cluster::topology::NodeDistance;
use crate::cluster::NodeInfo;

/// A node distance evaluator evaluates given node distance in relation to the driver.
#[cfg_attr(test, automock)]
pub trait NodeDistanceEvaluator {
    /// Tries to compute a distance to a given node. Can return `None` if the distance cannot be
    /// determined. In such case, the nodes without a distance are expected to be ignored by load
    /// balancers.
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

/// An evaluator which is aware of node location in relation to local DC. Built-in
/// [`TopologyAwareLoadBalancingStrategy`](crate::load_balancing::TopologyAwareLoadBalancingStrategy)
/// can use this information to properly identify which nodes to use in query plans.
pub struct TopologyAwareNodeDistanceEvaluator {
    local_dc: String,
}

impl NodeDistanceEvaluator for TopologyAwareNodeDistanceEvaluator {
    fn compute_distance(&self, node: &NodeInfo) -> Option<NodeDistance> {
        Some(if node.datacenter == self.local_dc {
            NodeDistance::Local
        } else {
            NodeDistance::Remote
        })
    }
}

impl TopologyAwareNodeDistanceEvaluator {
    /// Local DC name represents the datacenter local to where the driver is running.
    pub fn new(local_dc: String) -> Self {
        TopologyAwareNodeDistanceEvaluator { local_dc }
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use uuid::Uuid;

    use crate::cluster::topology::NodeDistance;
    use crate::cluster::NodeInfo;
    use crate::load_balancing::node_distance_evaluator::TopologyAwareNodeDistanceEvaluator;
    use crate::load_balancing::NodeDistanceEvaluator;

    #[test]
    fn should_return_topology_aware_distance() {
        let local_dc = "test";
        let evaluator = TopologyAwareNodeDistanceEvaluator::new(local_dc.into());

        assert_eq!(
            evaluator
                .compute_distance(&NodeInfo::new(
                    Uuid::new_v4(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
                    None,
                    "".into(),
                    Default::default(),
                    "".into(),
                ))
                .unwrap(),
            NodeDistance::Remote
        );
        assert_eq!(
            evaluator
                .compute_distance(&NodeInfo::new(
                    Uuid::new_v4(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
                    None,
                    local_dc.into(),
                    Default::default(),
                    "".into(),
                ))
                .unwrap(),
            NodeDistance::Local
        );
    }
}
