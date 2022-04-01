use futures::FutureExt;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::cluster::connection_manager::{startup, ConnectionManager};
use crate::cluster::KeyspaceHolder;
use crate::future::BoxFuture;
use crate::retry::ReconnectionPolicy;
use crate::transport::TransportRustls;
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::{Frame, Version};

pub struct RustlsConnectionManager {
    dns_name: rustls::ServerName,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
    keyspace_holder: Arc<KeyspaceHolder>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
    version: Version,
}

impl ConnectionManager<TransportRustls> for RustlsConnectionManager {
    fn connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<TransportRustls>> {
        async move {
            let mut schedule = self.reconnection_policy.new_node_schedule();

                self.establish_connection(event_handler.clone(), error_handler.clone(), addr)
                    .await
        }
        .boxed()
    }
}

impl RustlsConnectionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dns_name: rustls::ServerName,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        config: Arc<rustls::ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
        version: Version,
    ) -> Self {
        RustlsConnectionManager {
            dns_name,
            authenticator_provider,
            config,
            keyspace_holder,
            reconnection_policy,
            compression,
            buffer_size,
            tcp_nodelay,
            version,
        }
    }

    async fn establish_connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> Result<TransportRustls> {
        let transport = TransportRustls::new(
            addr,
            self.dns_name.clone(),
            self.config.clone(),
            self.keyspace_holder.clone(),
            event_handler,
            error_handler,
            self.compression,
            self.buffer_size,
            self.tcp_nodelay,
        )
        .await?;

        startup(
            &transport,
            self.authenticator_provider.deref(),
            self.keyspace_holder.deref(),
            self.compression,
            self.version,
        )
        .await?;

        Ok(transport)
    }
}
