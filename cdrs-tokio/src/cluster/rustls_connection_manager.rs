#[cfg(feature = "http-proxy")]
use async_http_proxy::{http_connect_tokio, http_connect_tokio_with_basic_auth};
use futures::FutureExt;
use std::io;
#[cfg(feature = "http-proxy")]
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
#[cfg(feature = "http-proxy")]
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::cluster::connection_manager::{startup, ConnectionManager};
#[cfg(feature = "http-proxy")]
use crate::cluster::HttpProxyConfig;
use crate::cluster::KeyspaceHolder;
use crate::frame_encoding::FrameEncodingFactory;
use crate::future::BoxFuture;
use crate::retry::ReconnectionPolicy;
use crate::transport::TransportRustls;
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::{Envelope, Version};

pub struct RustlsConnectionManager {
    dns_name: rustls::ServerName,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
    keyspace_holder: Arc<KeyspaceHolder>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    frame_encoder_factory: Box<dyn FrameEncodingFactory + Send + Sync>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
    version: Version,
    #[cfg(feature = "http-proxy")]
    http_proxy: Option<HttpProxyConfig>,
}

impl ConnectionManager<TransportRustls> for RustlsConnectionManager {
    //noinspection DuplicatedCode
    fn connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<TransportRustls>> {
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

impl RustlsConnectionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dns_name: rustls::ServerName,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        config: Arc<rustls::ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        frame_encoder_factory: Box<dyn FrameEncodingFactory + Send + Sync>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
        version: Version,
        #[cfg(feature = "http-proxy")] http_proxy: Option<HttpProxyConfig>,
    ) -> Self {
        RustlsConnectionManager {
            dns_name,
            authenticator_provider,
            config,
            keyspace_holder,
            reconnection_policy,
            frame_encoder_factory,
            compression,
            buffer_size,
            tcp_nodelay,
            version,
            #[cfg(feature = "http-proxy")]
            http_proxy,
        }
    }

    //noinspection DuplicatedCode
    #[cfg(feature = "http-proxy")]
    async fn create_transport(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> io::Result<TransportRustls> {
        if let Some(http_proxy) = &self.http_proxy {
            let mut stream = TcpStream::connect(&http_proxy.address).await?;

            if let Some(auth) = &http_proxy.basic_auth {
                http_connect_tokio_with_basic_auth(
                    &mut stream,
                    &addr.ip().to_string(),
                    addr.port(),
                    &auth.username,
                    &auth.password,
                )
                .await
                .map_err(|error| io::Error::new(ErrorKind::Other, error.to_string()))?;
            } else {
                http_connect_tokio(&mut stream, &addr.ip().to_string(), addr.port())
                    .await
                    .map_err(|error| io::Error::new(ErrorKind::Other, error.to_string()))?;
            }

            stream.set_nodelay(self.tcp_nodelay)?;
            TransportRustls::with_stream(
                stream,
                addr,
                self.dns_name.clone(),
                self.config.clone(),
                self.keyspace_holder.clone(),
                event_handler,
                error_handler,
                self.compression,
                self.frame_encoder_factory
                    .create_encoder(self.version, self.compression),
                self.frame_encoder_factory
                    .create_decoder(self.version, self.compression),
                self.buffer_size,
            )
            .await
        } else {
            TransportRustls::new(
                addr,
                self.dns_name.clone(),
                self.config.clone(),
                self.keyspace_holder.clone(),
                event_handler,
                error_handler,
                self.compression,
                self.frame_encoder_factory
                    .create_encoder(self.version, self.compression),
                self.frame_encoder_factory
                    .create_decoder(self.version, self.compression),
                self.buffer_size,
                self.tcp_nodelay,
            )
            .await
        }
    }

    //noinspection DuplicatedCode
    #[cfg(not(feature = "http-proxy"))]
    async fn create_transport(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> io::Result<TransportRustls> {
        TransportRustls::new(
            addr,
            self.dns_name.clone(),
            self.config.clone(),
            self.keyspace_holder.clone(),
            event_handler,
            error_handler,
            self.compression,
            self.frame_encoder_factory
                .create_encoder(self.version, self.compression),
            self.frame_encoder_factory
                .create_decoder(self.version, self.compression),
            self.buffer_size,
            self.tcp_nodelay,
        )
        .await
    }

    async fn establish_connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> Result<TransportRustls> {
        let transport = self
            .create_transport(event_handler, error_handler, addr)
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
