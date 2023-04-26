use crate::cluster::connection_manager::{startup, ConnectionManager};
#[cfg(feature = "http-proxy")]
use crate::cluster::HttpProxyConfig;
use crate::cluster::KeyspaceHolder;
use crate::frame_encoding::FrameEncodingFactory;
use crate::future::BoxFuture;
use crate::transport::TransportTcp;
#[cfg(feature = "http-proxy")]
use async_http_proxy::{http_connect_tokio, http_connect_tokio_with_basic_auth};
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::{Envelope, Version};
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

pub struct TcpConnectionManager {
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
    frame_encoder_factory: Box<dyn FrameEncodingFactory + Send + Sync>,
    compression: Compression,
    buffer_size: usize,
    tcp_nodelay: bool,
    version: Version,
    #[cfg(feature = "http-proxy")]
    http_proxy: Option<HttpProxyConfig>,
}

impl ConnectionManager<TransportTcp> for TcpConnectionManager {
    //noinspection DuplicatedCode
    fn connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<TransportTcp>> {
        self.establish_connection(event_handler, error_handler, addr)
            .boxed()
    }
}

impl TcpConnectionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        keyspace_holder: Arc<KeyspaceHolder>,
        frame_encoder_factory: Box<dyn FrameEncodingFactory + Send + Sync>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
        version: Version,
        #[cfg(feature = "http-proxy")] http_proxy: Option<HttpProxyConfig>,
    ) -> Self {
        Self {
            authenticator_provider,
            keyspace_holder,
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
    ) -> io::Result<TransportTcp> {
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
            TransportTcp::with_stream(
                stream,
                addr,
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
        } else {
            TransportTcp::new(
                addr,
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
    ) -> io::Result<TransportTcp> {
        TransportTcp::new(
            addr,
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
    ) -> Result<TransportTcp> {
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
