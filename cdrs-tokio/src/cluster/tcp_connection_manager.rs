use derive_more::Constructor;
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
use crate::transport::TransportTcp;
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::Frame;

#[derive(Constructor)]
pub struct TcpConnectionManager {
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
}

impl ConnectionManager<TransportTcp> for TcpConnectionManager {
    fn connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<TransportTcp>> {
        async move {
            let mut schedule = self.reconnection_policy.new_node_schedule();

            loop {
                let transport = self
                    .establish_connection(event_handler.clone(), error_handler.clone(), addr)
                    .await;
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
}

impl TcpConnectionManager {
    async fn establish_connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> Result<TransportTcp> {
        let transport = TransportTcp::new(
            addr,
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
        )
        .await?;

        Ok(transport)
    }
}
