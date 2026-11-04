use crate::cluster::connection_manager::{startup, ConnectionManager};
#[cfg(feature = "http-proxy")]
use crate::cluster::tcp_connection_manager::connect_through_http_proxy;
#[cfg(feature = "http-proxy")]
use crate::cluster::HttpProxyConfig;
use crate::cluster::KeyspaceHolder;
use crate::frame_encoding::FrameEncodingFactory;
use crate::future::BoxFuture;
use crate::transport::TransportRustls;
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::{Envelope, Version};
use futures::FutureExt;
use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig};

type ServerNameResolver = dyn Fn(SocketAddr) -> ServerName<'static> + Send + Sync;

fn resolve_server_name(
    dns_name: &ServerName<'static>,
    resolver: Option<&ServerNameResolver>,
    addr: SocketAddr,
) -> ServerName<'static> {
    match resolver {
        Some(resolver) => resolver(addr),
        None => dns_name.clone(),
    }
}

pub struct RustlsConnectionManager {
    dns_name: ServerName<'static>,
    server_name_resolver: Option<Box<ServerNameResolver>>,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<ClientConfig>,
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

impl ConnectionManager<TransportRustls> for RustlsConnectionManager {
    //noinspection DuplicatedCode
    fn connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, Result<TransportRustls>> {
        self.establish_connection(event_handler, error_handler, addr)
            .boxed()
    }
}

impl RustlsConnectionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dns_name: ServerName<'static>,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        config: Arc<ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        frame_encoder_factory: Box<dyn FrameEncodingFactory + Send + Sync>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
        version: Version,
        #[cfg(feature = "http-proxy")] http_proxy: Option<HttpProxyConfig>,
    ) -> Self {
        RustlsConnectionManager {
            dns_name,
            server_name_resolver: None,
            authenticator_provider,
            config,
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

    /// Resolves the server name used for SNI and certificate verification separately for each
    /// node address, e.g. when nodes present certificates with distinct names. By default, the
    /// name given at construction is used for all nodes.
    #[must_use]
    pub fn with_server_name_resolver<F>(mut self, resolver: F) -> Self
    where
        F: Fn(SocketAddr) -> ServerName<'static> + Send + Sync + 'static,
    {
        self.server_name_resolver = Some(Box::new(resolver));
        self
    }

    fn server_name(&self, addr: SocketAddr) -> ServerName<'static> {
        resolve_server_name(&self.dns_name, self.server_name_resolver.as_deref(), addr)
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
            let stream = connect_through_http_proxy(http_proxy, addr, self.tcp_nodelay).await?;
            TransportRustls::with_stream(
                stream,
                addr,
                self.server_name(addr),
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
                self.server_name(addr),
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
            self.server_name(addr),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::net::{IpAddr, Ipv4Addr};

    fn node_address(last_octet: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last_octet)), 9042)
    }

    #[test]
    fn server_name_defaults_to_dns_name_for_every_node() {
        let dns_name = ServerName::try_from("cluster.example").unwrap();

        for last_octet in 1..3 {
            assert_eq!(
                resolve_server_name(&dns_name, None, node_address(last_octet)),
                dns_name
            );
        }
    }

    #[test]
    fn server_name_is_resolved_per_node_address() {
        let dns_name = ServerName::try_from("cluster.example").unwrap();
        let resolver: Box<ServerNameResolver> =
            Box::new(|addr| ServerName::try_from(format!("node-{}.example", addr.ip())).unwrap());

        assert_eq!(
            resolve_server_name(&dns_name, Some(resolver.as_ref()), node_address(1)),
            ServerName::try_from("node-10.0.0.1.example").unwrap()
        );
        assert_eq!(
            resolve_server_name(&dns_name, Some(resolver.as_ref()), node_address(2)),
            ServerName::try_from("node-10.0.0.2.example").unwrap()
        );
    }
}
