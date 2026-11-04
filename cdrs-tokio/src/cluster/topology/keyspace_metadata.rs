use crate::cluster::topology::ReplicationStrategy;

/// Keyspace metadata.
///
/// New fields may be added in future releases. To build it, use [`KeyspaceMetadata::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct KeyspaceMetadata {
    pub replication_strategy: ReplicationStrategy,
}

impl KeyspaceMetadata {
    /// Creates keyspace metadata with the given replication strategy. Fields added in future
    /// releases get their default values.
    pub fn new(replication_strategy: ReplicationStrategy) -> Self {
        KeyspaceMetadata {
            replication_strategy,
        }
    }
}
