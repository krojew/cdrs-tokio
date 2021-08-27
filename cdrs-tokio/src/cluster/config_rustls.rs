use std::net::SocketAddr;
use std::sync::Arc;

use crate::authenticators::SaslAuthenticatorProvider;

/// Cluster configuration that holds per node SSL configs
pub struct ClusterRustlsConfig(pub Vec<NodeRustlsConfig>);

/// Single node TLS connection config.
#[derive(Clone)]
pub struct NodeRustlsConfig {
    pub addr: SocketAddr,
    pub dns_name: webpki::DNSName,
    pub authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub config: Arc<rustls::ClientConfig>,
}

/// Builder structure that helps to configure TLS connection for node.
pub struct NodeRustlsConfigBuilder {
    addr: SocketAddr,
    dns_name: webpki::DNSName,
    authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
}

impl NodeRustlsConfigBuilder {
    pub fn new(
        addr: SocketAddr,
        dns_name: webpki::DNSName,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        config: Arc<rustls::ClientConfig>,
    ) -> Self {
        NodeRustlsConfigBuilder {
            addr,
            dns_name,
            authenticator,
            config,
        }
    }

    /// Sets new authenticator.
    pub fn authenticator(
        mut self,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    ) -> Self {
        self.authenticator = authenticator;
        self
    }

    /// Finalizes building process
    pub fn build(self) -> NodeRustlsConfig {
        NodeRustlsConfig {
            addr: self.addr,
            dns_name: self.dns_name,
            authenticator: self.authenticator,
            config: self.config,
        }
    }
}
