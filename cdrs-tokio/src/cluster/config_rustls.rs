use std::net::SocketAddr;
use std::sync::Arc;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};

/// Cluster configuration that holds per node SSL configs
#[derive(Clone, Default)]
pub struct ClusterRustlsConfig(pub Vec<NodeRustlsConfig>);

/// Single node TLS connection config.
#[derive(Clone)]
pub struct NodeRustlsConfig {
    pub addr: SocketAddr,
    pub dns_name: webpki::DNSName,
    pub authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub config: Arc<rustls::ClientConfig>,
}

/// Builder structure that helps to configure TLS connection for node.
pub struct NodeRustlsConfigBuilder {
    addr: SocketAddr,
    dns_name: webpki::DNSName,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
}

impl NodeRustlsConfigBuilder {
    pub fn new(
        addr: SocketAddr,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
    ) -> Self {
        NodeRustlsConfigBuilder {
            addr,
            dns_name,
            authenticator_provider: Arc::new(NoneAuthenticatorProvider),
            config,
        }
    }

    /// Sets new authenticator.
    #[deprecated(note = "Use with_authenticator_provider().")]
    pub fn authenticator(
        self,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    ) -> Self {
        self.with_authenticator_provider(authenticator)
    }

    /// Sets new authenticator.
    pub fn with_authenticator_provider(
        mut self,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    ) -> Self {
        self.authenticator_provider = authenticator_provider;
        self
    }

    /// Finalizes building process
    pub fn build(self) -> NodeRustlsConfig {
        NodeRustlsConfig {
            addr: self.addr,
            dns_name: self.dns_name,
            authenticator_provider: self.authenticator_provider,
            config: self.config,
        }
    }
}
