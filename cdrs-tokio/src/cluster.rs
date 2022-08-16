pub(crate) use self::cluster_metadata_manager::ClusterMetadataManager;
#[cfg(feature = "rust-tls")]
pub use self::config_rustls::{NodeRustlsConfig, NodeRustlsConfigBuilder};
pub use self::config_tcp::{NodeTcpConfig, NodeTcpConfigBuilder};
pub use self::connection_manager::{startup, ConnectionManager};
pub use self::keyspace_holder::KeyspaceHolder;
pub use self::node_address::NodeAddress;
pub use self::node_info::NodeInfo;
pub use self::pager::{ExecPager, PagerState, QueryPager, SessionPager};
#[cfg(feature = "rust-tls")]
pub use self::rustls_connection_manager::RustlsConnectionManager;
pub use self::session::connect_generic;
pub(crate) use self::session_context::SessionContext;
pub use self::tcp_connection_manager::TcpConnectionManager;
pub use self::token_map::TokenMap;
pub use self::topology::cluster_metadata::ClusterMetadata;
use crate::cluster::connection_pool::ConnectionPoolConfig;
use crate::future::BoxFuture;
use crate::transport::CdrsTransport;
use cassandra_protocol::error;
use cassandra_protocol::frame::Version;
use std::sync::Arc;

mod cluster_metadata_manager;
#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
pub(crate) mod connection_manager;
pub mod connection_pool;
mod control_connection;
mod keyspace_holder;
mod metadata_builder;
mod node_address;
mod node_info;
mod pager;
#[cfg(feature = "rust-tls")]
mod rustls_connection_manager;
pub mod send_envelope;
pub mod session;
mod session_context;
mod tcp_connection_manager;
mod token_map;
pub mod topology;

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
pub trait GenericClusterConfig<T: CdrsTransport, CM: ConnectionManager<T>>: Send + Sync {
    fn create_manager(&self, keyspace_holder: Arc<KeyspaceHolder>) -> BoxFuture<error::Result<CM>>;

    /// Returns desired event channel capacity. Take a look at
    /// [`Session`](self::session::Session) builders for more info.
    fn event_channel_capacity(&self) -> usize;

    /// Cassandra protocol version to use.
    fn version(&self) -> Version;

    /// Connection pool configuration.
    fn connection_pool_config(&self) -> ConnectionPoolConfig;

    /// Enable beta protocol support.
    fn beta_protocol(&self) -> bool {
        false
    }
}
