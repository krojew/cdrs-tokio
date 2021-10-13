/// Determines how the driver will manage connections to a Cassandra node.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum NodeDistance {
    /// An "active" distance that, indicates that the driver should maintain connections to the
    /// node; it also marks it as "preferred", meaning that the number or capacity of the
    /// connections may be higher, and that the node may also have priority for some tasks (for
    /// example, being chosen as the control host).
    Local,
    /// An "active" distance that, indicates that the driver should maintain connections to the
    /// node; it also marks it as "less preferred", meaning that the number or capacity of the
    /// connections may be lower, and that other nodes may have a higher priority for some tasks
    /// (for example, being chosen as the control host).
    Remote,
}
