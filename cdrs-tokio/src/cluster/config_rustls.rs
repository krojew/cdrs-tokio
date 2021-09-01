use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::lookup_host;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use crate::cluster::NodeAddress;
use crate::error::Result;

/// Cluster configuration that holds per node TLS configs
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
    addrs: Vec<NodeAddress>,
    dns_name: webpki::DNSName,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
}

impl NodeRustlsConfigBuilder {
    pub fn new(dns_name: webpki::DNSName, config: Arc<rustls::ClientConfig>) -> Self {
        NodeRustlsConfigBuilder {
            addrs: vec![],
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

    /// Adds node address.
    pub fn with_node_address(mut self, addr: NodeAddress) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Finalizes building process
    pub async fn build(self) -> Result<Vec<NodeRustlsConfig>> {
        // replace with map() when async lambdas become available
        let mut configs = Vec::with_capacity(self.addrs.len());
        for addr in self.addrs {
            configs.append(
                &mut Self::create_config(
                    self.dns_name.clone(),
                    self.authenticator_provider.clone(),
                    self.config.clone(),
                    addr,
                )
                .await?,
            );
        }

        Ok(configs)
    }

    async fn create_config(
        dns_name: webpki::DNSName,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        config: Arc<rustls::ClientConfig>,
        addr: NodeAddress,
    ) -> Result<Vec<NodeRustlsConfig>> {
        match addr {
            NodeAddress::Direct(addr) => Ok(vec![NodeRustlsConfig {
                addr,
                dns_name,
                authenticator_provider,
                config,
            }]),
            NodeAddress::Hostname(hostname) => lookup_host(hostname)
                .await
                .map(|addrs| {
                    addrs
                        .map(|addr| NodeRustlsConfig {
                            addr,
                            dns_name: dns_name.clone(),
                            authenticator_provider: authenticator_provider.clone(),
                            config: config.clone(),
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(Into::into),
        }
    }
}
