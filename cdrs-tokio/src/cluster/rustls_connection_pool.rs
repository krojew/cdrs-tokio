use async_trait::async_trait;
use bb8::{Builder, ManageConnection, PooledConnection};

use std::net;
use std::ops::Deref;
use std::sync::Arc;

use crate::authenticators::SaslAuthenticatorProvider;
use crate::cluster::ConnectionPool;
use crate::cluster::{startup, KeyspaceHolder, NodeRustlsConfig};
use crate::compression::Compression;
use crate::error;
use crate::frame::Frame;
use crate::transport::{CdrsTransport, TransportRustls};

pub type RustlsConnectionPool = ConnectionPool<TransportRustls>;

/// `bb8::Pool` of SSL-based CDRS connections.
///
/// Used internally for SSL Session for holding connections to a specific Cassandra node.
pub async fn new_rustls_pool(
    node_config: NodeRustlsConfig,
    compression: Compression,
) -> error::Result<RustlsConnectionPool> {
    let manager = RustlsConnectionsManager::new(
        node_config.addr,
        node_config.dns_name,
        node_config.config,
        node_config.authenticator,
        compression,
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
    auth: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
    compression: Compression,
}

impl RustlsConnectionsManager {
    #[inline]
    pub fn new(
        addr: net::SocketAddr,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
        auth: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        compression: Compression,
    ) -> Self {
        RustlsConnectionsManager {
            addr,
            dns_name,
            config,
            auth,
            keyspace_holder: Default::default(),
            compression,
        }
    }
}

#[async_trait]
impl ManageConnection for RustlsConnectionsManager {
    type Connection = TransportRustls;
    type Error = error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = TransportRustls::new(
            self.addr,
            self.dns_name.clone(),
            self.config.clone(),
            self.keyspace_holder.clone(),
            None,
            self.compression,
        )
        .await?;
        startup(
            &transport,
            self.auth.deref(),
            self.keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        Ok(transport)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let options_frame = Frame::new_req_options();
        conn.write_frame(options_frame).await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_broken()
    }
}
