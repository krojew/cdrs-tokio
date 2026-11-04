#[cfg(feature = "http-proxy")]
use crate::cluster::config_proxy::connect_host;
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
    beta_protocol: bool,
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
    ) -> BoxFuture<'_, Result<TransportTcp>> {
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
            beta_protocol: false,
            #[cfg(feature = "http-proxy")]
            http_proxy,
        }
    }

    /// Marks every envelope sent over created connections with the beta protocol flag, which
    /// Cassandra requires on all frames when a beta protocol version is in use.
    #[must_use]
    pub fn with_beta_protocol(mut self, beta_protocol: bool) -> Self {
        self.beta_protocol = beta_protocol;
        self
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
                    &connect_host(addr),
                    addr.port(),
                    &auth.username,
                    &auth.password,
                )
                .await
                .map_err(|error| io::Error::new(ErrorKind::Other, error.to_string()))?;
            } else {
                http_connect_tokio(&mut stream, &connect_host(addr), addr.port())
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
            .await?
            .with_beta_protocol(self.beta_protocol);

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

#[cfg(all(test, feature = "http-proxy"))]
mod tests {
    use super::*;
    use crate::cluster::HttpProxyConfigBuilder;
    use crate::frame_encoding::ProtocolFrameEncodingFactory;
    use cassandra_protocol::authenticators::NoneAuthenticatorProvider;
    use std::net::Ipv6Addr;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::sync::watch;

    #[tokio::test]
    async fn http_proxy_connect_request_brackets_ipv6_host() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = listener.local_addr().unwrap();

        let proxy = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);

            let mut request_line = String::new();
            stream.read_line(&mut request_line).await.unwrap();

            loop {
                let mut header = String::new();
                if stream.read_line(&mut header).await.unwrap() == 0 || header == "\r\n" {
                    break;
                }
            }

            stream
                .get_mut()
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();

            request_line
        });

        let (keyspace_sender, _) = watch::channel(None);
        let manager = TcpConnectionManager::new(
            Arc::new(NoneAuthenticatorProvider),
            Arc::new(KeyspaceHolder::new(keyspace_sender)),
            Box::new(ProtocolFrameEncodingFactory),
            Compression::None,
            32,
            true,
            Version::V4,
            Some(HttpProxyConfigBuilder::new(proxy_address.to_string()).build()),
        );

        manager
            .create_transport(
                None,
                None,
                SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 9042),
            )
            .await
            .unwrap();

        assert_eq!(proxy.await.unwrap(), "CONNECT [::1]:9042 HTTP/1.1\r\n");
    }
}

#[cfg(test)]
mod beta_protocol_tests {
    use super::*;
    use crate::frame_encoding::ProtocolFrameEncodingFactory;
    use cassandra_protocol::authenticators::NoneAuthenticatorProvider;
    use cassandra_protocol::frame::{Direction, Flags, Opcode, ParseEnvelopeError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::watch;

    #[tokio::test]
    async fn beta_protocol_marks_startup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![];
            let startup = loop {
                match Envelope::from_buffer(&buffer, Compression::None) {
                    Ok(parsed) => break parsed.envelope,
                    Err(ParseEnvelopeError::NotEnoughBytes) => {
                        let read = stream.read_buf(&mut buffer).await.unwrap();
                        assert_ne!(read, 0, "client closed the connection");
                    }
                    Err(error) => panic!("invalid request: {}", error),
                }
            };

            let ready = Envelope::new(
                Version::V4,
                Direction::Response,
                Flags::empty(),
                Opcode::Ready,
                startup.stream_id,
                vec![],
                None,
                vec![],
            );
            stream
                .write_all(&ready.encode_with(Compression::None).unwrap())
                .await
                .unwrap();

            startup.flags
        });

        let (keyspace_sender, _) = watch::channel(None);
        let manager = TcpConnectionManager::new(
            Arc::new(NoneAuthenticatorProvider),
            Arc::new(KeyspaceHolder::new(keyspace_sender)),
            Box::new(ProtocolFrameEncodingFactory),
            Compression::None,
            32,
            true,
            Version::V4,
            #[cfg(feature = "http-proxy")]
            None,
        )
        .with_beta_protocol(true);

        manager.connection(None, None, address).await.unwrap();

        assert!(server.await.unwrap().contains(Flags::BETA));
    }
}
