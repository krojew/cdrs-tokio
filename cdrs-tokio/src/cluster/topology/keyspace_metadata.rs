use derive_more::Constructor;

use crate::cluster::topology::ReplicationStrategy;

/// Keyspace metadata.
#[derive(Clone, Debug, Constructor)]
pub struct KeyspaceMetadata {
    pub replication_strategy: ReplicationStrategy,
}
