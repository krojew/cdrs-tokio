use async_trait::async_trait;
use std::sync::Arc;

#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
mod generic_connection_pool;
mod keyspace_holder;
mod pager;
#[cfg(feature = "rust-tls")]
mod rustls_connection_pool;
pub mod session;
mod tcp_connection_pool;

#[cfg(feature = "rust-tls")]
pub use crate::cluster::config_rustls::{
    ClusterRustlsConfig, NodeRustlsConfig, NodeRustlsConfigBuilder,
};
pub use crate::cluster::config_tcp::{ClusterTcpConfig, NodeTcpConfig, NodeTcpConfigBuilder};
pub use crate::cluster::keyspace_holder::KeyspaceHolder;
pub use crate::cluster::pager::{ExecPager, PagerState, QueryPager, SessionPager};
#[cfg(feature = "rust-tls")]
pub use crate::cluster::rustls_connection_pool::{
    new_rustls_pool, RustlsConnectionPool, RustlsConnectionsManager,
};
#[cfg(feature = "unstable-dynamic-cluster")]
pub use crate::cluster::session::connect_generic_dynamic;
pub use crate::cluster::session::connect_generic_static;
pub use crate::cluster::tcp_connection_pool::{
    new_tcp_pool, startup, TcpConnectionPool, TcpConnectionsManager,
};
pub use generic_connection_pool::ConnectionPool;

use crate::error::Error;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::RetryPolicy;
use crate::transport::CdrsTransport;

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
#[async_trait]
pub trait GenericClusterConfig: Send + Sync {
    type Transport: CdrsTransport + Send + Sync;
    type Address: Clone;

    async fn connect(&self, addr: Self::Address) -> Result<ConnectionPool<Self::Transport>, Error>;
}

/// `GetConnection` trait provides a unified interface for Session to get a connection
/// from a load balancer
#[async_trait]
pub trait GetConnection<T: CdrsTransport + Send + Sync + 'static> {
    /// Returns connection from a load balancer.
    async fn connection(&self) -> Option<Arc<ConnectionPool<T>>>;
}

/// `GetRetryPolicy` trait provides a unified interface for Session to get current retry policy.
pub trait GetRetryPolicy {
    fn retry_policy(&self) -> &dyn RetryPolicy;
}

/// `CdrsSession` trait wrap ups whole query functionality. Use it only if whole query
/// machinery is needed and direct sub traits otherwise.
pub trait CdrsSession<T: CdrsTransport + Unpin + 'static>:
    GetConnection<T>
    + QueryExecutor<T>
    + PrepareExecutor<T>
    + ExecExecutor<T>
    + BatchExecutor<T>
    + GetRetryPolicy
{
}
