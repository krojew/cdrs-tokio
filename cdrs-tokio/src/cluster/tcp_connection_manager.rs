use futures::FutureExt;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::cluster::connection_manager::{
    startup, ConnectionManager, ThreadSafeReconnectionPolicy,
};
use crate::cluster::{KeyspaceHolder, NodeTcpConfig};
use crate::compression::Compression;
use crate::error::Result;
use crate::frame::Frame;
use crate::future::BoxFuture;
use crate::transport::{CdrsTransport, TransportTcp};

pub struct TcpConnectionManager {
    config: NodeTcpConfig,
    keyspace_holder: Arc<KeyspaceHolder>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
    event_handler: Option<Sender<Frame>>,
    connection: RwLock<Option<Arc<TransportTcp>>>,
}

impl ConnectionManager<TransportTcp> for TcpConnectionManager {
    fn connection<'a>(
        &'a self,
        reconnection_policy: &'a ThreadSafeReconnectionPolicy,
    ) -> BoxFuture<Result<Arc<TransportTcp>>> {
        async move {
            {
                let connection = self.connection.read().await;
                if let Some(connection) = connection.deref() {
                    if !connection.is_broken() {
                        return Ok(connection.clone());
                    }
                }
            }

            let mut connection = self.connection.write().await;
            if let Some(connection) = connection.deref() {
                if !connection.is_broken() {
                    // somebody established connection in the meantime
                    return Ok(connection.clone());
                }
            }

            let mut schedule = reconnection_policy.new_node_schedule();

            loop {
                let transport = self.establish_connection().await;
                match transport {
                    Ok(transport) => {
                        let transport = Arc::new(transport);
                        *connection = Some(transport.clone());

                        return Ok(transport);
                    }
                    Err(error) => {
                        let delay = schedule.next_delay().ok_or(error)?;
                        sleep(delay).await;
                    }
                }
            }
        }
        .boxed()
    }

    #[inline]
    fn addr(&self) -> SocketAddr {
        self.config.addr
    }
}

impl TcpConnectionManager {
    pub fn new(
        config: NodeTcpConfig,
        keyspace_holder: Arc<KeyspaceHolder>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
        event_handler: Option<Sender<Frame>>,
    ) -> Self {
        TcpConnectionManager {
            config,
            keyspace_holder,
            compression,
            buffer_size,
            tcp_nodelay,
            event_handler,
            connection: Default::default(),
        }
    }

    async fn establish_connection(&self) -> Result<TransportTcp> {
        let transport = TransportTcp::new(
            self.config.addr,
            self.keyspace_holder.clone(),
            self.event_handler.clone(),
            self.compression,
            self.buffer_size,
            self.tcp_nodelay,
        )
        .await?;

        startup(
            &transport,
            self.config.authenticator_provider.deref(),
            self.keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        Ok(transport)
    }
}
