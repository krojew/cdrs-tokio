use fxhash::FxHashMap;

/// A replication strategy determines the nodes where replicas are placed.
#[derive(Debug, Clone)]
pub enum ReplicationStrategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        datacenter_replication_factor: FxHashMap<String, usize>,
    },
    Other,
}
