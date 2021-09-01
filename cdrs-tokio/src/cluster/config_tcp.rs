use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::lookup_host;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use crate::cluster::NodeAddress;
use crate::error::Result;

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
    addrs: Vec<NodeAddress>,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
}

impl Default for NodeTcpConfigBuilder {
    fn default() -> Self {
        NodeTcpConfigBuilder {
            addrs: vec![],
            authenticator_provider: Arc::new(NoneAuthenticatorProvider),
        }
    }
}

impl NodeTcpConfigBuilder {
    pub fn new() -> NodeTcpConfigBuilder {
        Default::default()
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

    /// Adds node address.
    pub fn with_node_address(mut self, addr: NodeAddress) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Finalizes building process
    pub async fn build(self) -> Result<Vec<NodeTcpConfig>> {
        // replace with map() when async lambdas become available
        let mut configs = Vec::with_capacity(self.addrs.len());
        for addr in self.addrs {
            configs
                .append(&mut Self::create_config(self.authenticator_provider.clone(), addr).await?);
        }

        Ok(configs)
    }

    async fn create_config(
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        addr: NodeAddress,
    ) -> Result<Vec<NodeTcpConfig>> {
        match addr {
            NodeAddress::Direct(addr) => Ok(vec![NodeTcpConfig {
                addr,
                authenticator_provider,
            }]),
            NodeAddress::Hostname(hostname) => lookup_host(hostname)
                .await
                .map(|addrs| {
                    addrs
                        .map(|addr| NodeTcpConfig {
                            addr,
                            authenticator_provider: authenticator_provider.clone(),
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(Into::into),
        }
    }
}
