pub use topology::cluster_metadata::ClusterMetadata;

pub(crate) use crate::cluster::cluster_metadata_manager::ClusterMetadataManager;
#[cfg(feature = "rust-tls")]
pub use crate::cluster::config_rustls::{NodeRustlsConfig, NodeRustlsConfigBuilder};
pub use crate::cluster::config_tcp::{NodeTcpConfig, NodeTcpConfigBuilder};
pub use crate::cluster::connection_manager::{startup, ConnectionManager};
pub use crate::cluster::keyspace_holder::KeyspaceHolder;
pub use crate::cluster::node_address::NodeAddress;
pub use crate::cluster::pager::{ExecPager, PagerState, QueryPager, SessionPager};
#[cfg(feature = "rust-tls")]
pub use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
pub use crate::cluster::session::connect_generic;
pub use crate::cluster::tcp_connection_manager::TcpConnectionManager;
use crate::error::Result;
use crate::future::BoxFuture;
use crate::transport::CdrsTransport;

mod cluster_metadata_manager;
#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
mod connection_manager;
mod control_connection;
mod keyspace_holder;
mod metadata_builder;
mod node_address;
mod node_info;
mod pager;
#[cfg(feature = "rust-tls")]
mod rustls_connection_manager;
pub mod session;
mod session_context;
mod tcp_connection_manager;
pub mod topology;

pub use self::node_info::NodeInfo;
pub(crate) use self::session_context::SessionContext;

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
pub trait GenericClusterConfig<T: CdrsTransport, CM: ConnectionManager<T>>: Send + Sync {
    fn create_manager(&self) -> BoxFuture<Result<CM>>;

    /// Returns desired event channel capacity. Take a look at ['Session'] builders for more info.
    fn event_channel_capacity(&self) -> usize;
}
