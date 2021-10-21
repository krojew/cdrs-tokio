use std::sync::Arc;

use fxhash::FxHashMap;
use uuid::Uuid;

pub mod cluster_metadata;
mod datacenter_metadata;
mod keyspace_metadata;
mod node;
mod node_distance;
mod node_state;
mod replication_strategy;

pub use self::datacenter_metadata::DatacenterMetadata;
pub use self::keyspace_metadata::KeyspaceMetadata;
pub use self::node::Node;
pub use self::node_distance::NodeDistance;
pub use self::node_state::NodeState;
pub use self::replication_strategy::ReplicationStrategy;

/// Map from host id to a node.
pub type NodeMap<T, CM> = FxHashMap<Uuid, Arc<Node<T, CM>>>;
