use fxhash::FxHashMap;

/// A replication strategy determines the nodes where replicas are placed.
///
/// New variants may be added in future releases.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReplicationStrategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        datacenter_replication_factor: FxHashMap<String, usize>,
    },
    Other,
}
