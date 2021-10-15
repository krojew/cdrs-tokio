use derive_more::Display;

/// The state of a node, as viewed from the driver.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Display)]
pub enum NodeState {
    /// The driver has never tried to connect to the node, nor received any topology events about it.
    ///
    /// This happens when nodes are first added to the cluster, and will persist if your
    /// [`LoadBalancingPolicy`] decides to ignore them. Since the driver does not connect to them, the
    /// only way it can assess their states is from topology events.
    Unknown,
    /// A node is considered up in either of the following situations: 1) the driver has at least
    /// one active connection to the node, or 2) the driver is not actively trying to connect to the
    /// node (because it's ignored by the [`LoadBalancingPolicy`]), but it has received a topology
    /// event indicating that the node is up.
    Up,
    /// A node is considered down in either of the following situations: 1) the driver has lost all
    /// connections to the node (and is currently trying to reconnect), or 2) the driver is not
    /// actively trying to connect to the node (because it's ignored by the [`LoadBalancingPolicy`],
    /// but it has received a topology event indicating that the node is down.
    Down,
}
