use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;

#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
mod connection_manager;
mod keyspace_holder;
mod node_address;
mod pager;
#[cfg(feature = "rust-tls")]
mod rustls_connection_manager;
pub mod session;
mod tcp_connection_manager;

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

use crate::error::Result;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::RetryPolicy;
use crate::transport::CdrsTransport;

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
#[async_trait]
pub trait GenericClusterConfig<T: CdrsTransport, CM: ConnectionManager<T>>: Send + Sync {
    type Address: Clone;

    async fn create_manager(&self, addr: Self::Address) -> Result<CM>;
}

/// `GetConnection` trait provides a unified interface for Session to get a connection
/// from a load balancer
#[async_trait]
pub trait GetConnection<T: CdrsTransport + Send + Sync + 'static> {
    /// Returns connection from a load balancer
    async fn load_balanced_connection(&self) -> Option<Result<Arc<T>>>;

    // Returns connection to the desired node
    async fn node_connection(&self, node: &SocketAddr) -> Option<Result<Arc<T>>>;
}

/// `GetRetryPolicy` trait provides a unified interface for Session to get current retry policy.
pub trait GetRetryPolicy {
    fn retry_policy(&self) -> &dyn RetryPolicy;
}

/// `CdrsSession` trait wraps up whole query functionality. Use it only if whole query
/// machinery is needed and direct sub traits otherwise.
pub trait CdrsSession<T: CdrsTransport + 'static>:
    GetConnection<T>
    + QueryExecutor<T>
    + PrepareExecutor<T>
    + ExecExecutor<T>
    + BatchExecutor<T>
    + GetRetryPolicy
{
}
