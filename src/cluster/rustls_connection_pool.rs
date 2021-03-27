use async_trait::async_trait;
use bb8::{Builder, ManageConnection, PooledConnection};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use std::net;
use std::sync::Arc;

use crate::authenticators::Authenticator;
use crate::cluster::ConnectionPool;
use crate::cluster::{startup, KeyspaceHolder, NodeRustlsConfig};
use crate::compression::Compression;
use crate::error;
use crate::frame::parser::parse_frame;
use crate::frame::{AsBytes, Frame};
use crate::transport::TransportRustls;
use std::ops::Deref;

pub type RustlsConnectionPool = ConnectionPool<TransportRustls>;

/// `bb8::Pool` of SSL-based CDRS connections.
///
/// Used internally for SSL Session for holding connections to a specific Cassandra node.
pub async fn new_rustls_pool(node_config: NodeRustlsConfig) -> error::Result<RustlsConnectionPool> {
    let manager = RustlsConnectionsManager::new(
        node_config.addr,
        node_config.dns_name,
        node_config.config,
        node_config.authenticator,
    );

    let pool = Builder::new()
        .max_size(node_config.max_size)
        .min_idle(node_config.min_idle)
        .max_lifetime(node_config.max_lifetime)
        .idle_timeout(node_config.idle_timeout)
        .connection_timeout(node_config.connection_timeout)
        .build(manager)
        .await
        .map_err(|err| error::Error::from(err.to_string()))?;

    Ok(RustlsConnectionPool::new(pool, node_config.addr))
}

/// `bb8` connection manager.
pub struct RustlsConnectionsManager {
    addr: net::SocketAddr,
    dns_name: webpki::DNSName,
    config: Arc<rustls::ClientConfig>,
    auth: Arc<dyn Authenticator + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
}

impl RustlsConnectionsManager {
    #[inline]
    pub fn new(
        addr: net::SocketAddr,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
        auth: Arc<dyn Authenticator + Send + Sync>,
    ) -> Self {
        Self {
            addr,
            dns_name,
            config,
            auth,
            keyspace_holder: Default::default(),
        }
    }
}

#[async_trait]
impl ManageConnection for RustlsConnectionsManager {
    type Connection = Mutex<TransportRustls>;
    type Error = error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = Mutex::new(
            TransportRustls::new(
                self.addr,
                self.dns_name.clone(),
                self.config.clone(),
                self.keyspace_holder.clone(),
            )
            .await?,
        );
        startup(&transport, self.auth.deref(), self.keyspace_holder.deref()).await?;

        Ok(transport)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let options_frame = Frame::new_req_options().as_bytes();
        conn.lock().await.write(options_frame.as_slice()).await?;

        parse_frame(&conn, Compression::None).await.map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
