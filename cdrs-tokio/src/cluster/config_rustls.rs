use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::NodeAddress;
use cassandra_protocol::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use cassandra_protocol::error::Result;
use cassandra_protocol::frame::Version;
use rustls::ServerName;

/// Single node TLS connection config.
#[derive(Clone)]
pub struct NodeRustlsConfig {
    pub contact_points: Vec<SocketAddr>,
    pub dns_name: ServerName,
    pub authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub config: Arc<rustls::ClientConfig>,
    pub version: Version,
}

/// Builder structure that helps to configure TLS connection for node.
pub struct NodeRustlsConfigBuilder {
    addrs: Vec<NodeAddress>,
    dns_name: ServerName,
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    config: Arc<rustls::ClientConfig>,
    version: Version,
}

impl NodeRustlsConfigBuilder {
    pub fn new(dns_name: ServerName, config: Arc<rustls::ClientConfig>) -> Self {
        NodeRustlsConfigBuilder {
            addrs: vec![],
            dns_name,
            authenticator_provider: Arc::new(NoneAuthenticatorProvider),
            config,
            version: Version::V4,
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

    /// Adds initial node address (a contact point). Contact points are considered local to the
    /// driver until a topology refresh occurs.
    pub fn with_contact_point(mut self, addr: NodeAddress) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Adds initial node addresses
    pub fn with_contact_points(mut self, addr: Vec<NodeAddress>) -> Self {
        self.addrs.extend(addr);
        self
    }

    /// Set cassandra protocol version
    pub fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Finalizes building process
    pub async fn build(self) -> Result<NodeRustlsConfig> {
        // replace with map() when async lambdas become available
        let mut contact_points = Vec::with_capacity(self.addrs.len());
        for contact_point in self.addrs {
            contact_points.append(&mut contact_point.resolve_address().await?);
        }

        Ok(NodeRustlsConfig {
            contact_points,
            dns_name: self.dns_name,
            authenticator_provider: self.authenticator_provider,
            config: self.config,
            version: self.version,
        })
    }
}
