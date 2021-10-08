use futures::FutureExt;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::cluster::connection_manager::{startup, ConnectionManager};
use crate::cluster::{KeyspaceHolder, NodeTcpConfig};
use crate::compression::Compression;
use crate::error::Result;
use crate::frame::Frame;
use crate::future::BoxFuture;
use crate::retry::ReconnectionPolicy;
use crate::transport::TransportTcp;

pub struct TcpConnectionManager {
    config: NodeTcpConfig,
    keyspace_holder: Arc<KeyspaceHolder>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
}

impl ConnectionManager<TransportTcp> for TcpConnectionManager {
    fn connection(&self, event_handler: Option<Sender<Frame>>) -> BoxFuture<Result<TransportTcp>> {
        async move {
            let mut schedule = self.reconnection_policy.new_node_schedule();

            loop {
                let transport = self.establish_connection(event_handler.clone()).await;
                match transport {
                    Ok(transport) => return Ok(transport),
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
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
    ) -> Self {
        TcpConnectionManager {
            config,
            keyspace_holder,
            reconnection_policy,
            compression,
            buffer_size,
            tcp_nodelay,
        }
    }

    async fn establish_connection(
        &self,
        event_handler: Option<Sender<Frame>>,
    ) -> Result<TransportTcp> {
        let transport = TransportTcp::new(
            self.config.addr,
            self.keyspace_holder.clone(),
            event_handler,
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
