use derive_more::Display;

/// Determines how the driver will manage connections to a Cassandra node.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Display)]
pub enum NodeDistance {
    /// An "active" distance that, indicates that the driver should maintain connections to the
    /// node; it also marks it as "preferred", meaning that the node may have priority for
    /// some tasks (for example, being chosen as the control connection host).
    Local,
    /// An "active" distance that, indicates that the driver should maintain connections to the
    /// node; it also marks it as "less preferred", meaning that other nodes may have a higher
    /// priority for some tasks (for example, being chosen as the control connection host).
    Remote,
}
