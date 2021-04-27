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

use crate::frame::{Frame, StreamId};
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::RetryPolicy;
use crate::transport::CdrsTransport;
use crate::{compression::Compression, error::FromCdrsError};

/// Generic connection configuration trait that can be used to create user-supplied
/// connection objects that can be used with the `session::connect()` function.
#[async_trait]
pub trait GenericClusterConfig: Send + Sync {
    type Transport: CdrsTransport + Send + Sync;
    type Address: Clone;
    type Error: FromCdrsError;

    async fn connect(
        &self,
        addr: Self::Address,
    ) -> Result<ConnectionPool<Self::Transport>, Self::Error>;
}

/// `GetConnection` trait provides a unified interface for Session to get a connection
/// from a load balancer
#[async_trait]
pub trait GetConnection<T: CdrsTransport + Send + Sync + 'static> {
    /// Returns connection from a load balancer.
    async fn connection(&self) -> Option<Arc<ConnectionPool<T>>>;
}

/// `GetCompressor` trait provides a unified interface for Session to get a compressor
/// for further decompressing received data.
pub trait GetCompressor {
    /// Returns actual compressor.
    fn compressor(&self) -> Compression;
}

/// `ResponseCache` caches responses to match them by their stream id to requests.
#[async_trait]
pub trait ResponseCache {
    async fn match_or_cache_response(&self, stream_id: StreamId, frame: Frame) -> Option<Frame>;
}

/// `GetRetryPolicy` trait provides a unified interface for Session to get current retry policy.
pub trait GetRetryPolicy {
    fn retry_policy(&self) -> &dyn RetryPolicy;
}

/// `CdrsSession` trait wrap ups whole query functionality. Use it only if whole query
/// machinery is needed and direct sub traits otherwise.
pub trait CdrsSession<T: CdrsTransport + Unpin + 'static>:
    GetCompressor
    + GetConnection<T>
    + QueryExecutor<T>
    + PrepareExecutor<T>
    + ExecExecutor<T>
    + BatchExecutor<T>
    + GetRetryPolicy
{
}
