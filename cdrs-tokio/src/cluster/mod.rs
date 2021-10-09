mod cluster_metadata;
mod cluster_metadata_manager;
#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
mod connection_manager;
mod control_connection;
mod keyspace_holder;
mod node;
mod node_address;
mod pager;
#[cfg(feature = "rust-tls")]
mod rustls_connection_manager;
pub mod session;
mod tcp_connection_manager;
mod topology;

pub use crate::cluster::cluster_metadata::ClusterMetadata;
pub(crate) use crate::cluster::cluster_metadata_manager::ClusterMetadataManager;
#[cfg(feature = "rust-tls")]
pub use crate::cluster::config_rustls::{
    ClusterRustlsConfig, NodeRustlsConfig, NodeRustlsConfigBuilder,
};
pub use crate::cluster::config_tcp::{ClusterTcpConfig, NodeTcpConfig, NodeTcpConfigBuilder};
pub use crate::cluster::keyspace_holder::KeyspaceHolder;
pub use crate::cluster::node_address::NodeAddress;
pub use crate::cluster::pager::{ExecPager, PagerState, QueryPager, SessionPager};
pub use crate::cluster::session::connect_generic_static;

pub use crate::cluster::connection_manager::{startup, ConnectionManager};
#[cfg(feature = "rust-tls")]
pub use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
pub use crate::cluster::tcp_connection_manager::TcpConnectionManager;

pub use crate::cluster::node::Node;

use crate::error::Result;
use crate::future::BoxFuture;
use crate::retry::RetryPolicy;
use crate::transport::CdrsTransport;

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
pub trait GenericClusterConfig<T: CdrsTransport, CM: ConnectionManager<T>>: Send + Sync {
    type Address: Clone;

    fn create_manager(&self, addr: Self::Address) -> BoxFuture<Result<CM>>;
}

/// `GetRetryPolicy` trait provides a unified interface for Session to get current retry policy.
pub trait GetRetryPolicy {
    fn retry_policy(&self) -> &dyn RetryPolicy;
}
