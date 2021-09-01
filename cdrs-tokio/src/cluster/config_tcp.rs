use std::net::SocketAddr;
use std::sync::Arc;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};

/// Cluster configuration that holds per node TCP configs
#[derive(Clone, Default)]
pub struct ClusterTcpConfig(pub Vec<NodeTcpConfig>);

/// Single node TCP connection config.
#[derive(Clone)]
pub struct NodeTcpConfig {
    pub addr: SocketAddr,
    pub authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
}

/// Builder structure that helps to configure TCP connection for node.
pub struct NodeTcpConfigBuilder {
    addr: SocketAddr,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
}

impl NodeTcpConfigBuilder {
    pub fn new(addr: SocketAddr) -> NodeTcpConfigBuilder {
        NodeTcpConfigBuilder {
            addr,
            authenticator_provider: Arc::new(NoneAuthenticatorProvider),
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
    pub fn build(self) -> NodeTcpConfig {
        NodeTcpConfig {
            addr: self.addr,
            authenticator_provider: self.authenticator_provider,
        }
    }
}
