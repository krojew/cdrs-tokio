use std::net::SocketAddr;
use std::sync::Arc;

use crate::authenticators::SaslAuthenticatorProvider;

/// Cluster configuration that holds per node TCP configs
pub struct ClusterTcpConfig(pub Vec<NodeTcpConfig>);

/// Single node TCP connection config.
#[derive(Clone)]
pub struct NodeTcpConfig {
    pub addr: SocketAddr,
    pub authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
}

/// Builder structure that helps to configure TCP connection for node.
pub struct NodeTcpConfigBuilder {
    addr: SocketAddr,
    authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
}

impl NodeTcpConfigBuilder {
    pub fn new(
        addr: SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    ) -> NodeTcpConfigBuilder {
        NodeTcpConfigBuilder {
            addr,
            authenticator,
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
    pub fn build(self) -> NodeTcpConfig {
        NodeTcpConfig {
            addr: self.addr,
            authenticator: self.authenticator,
        }
    }
}
