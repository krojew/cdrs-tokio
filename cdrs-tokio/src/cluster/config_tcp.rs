use std::net::SocketAddr;
use std::sync::Arc;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use crate::cluster::NodeAddress;
use crate::error::Result;

/// Single node TCP connection config.
#[derive(Clone)]
pub struct NodeTcpConfig {
    pub contact_points: Vec<SocketAddr>,
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

    /// Adds initial node address (a contact point). Contact points are considered local to the
    /// driver until a topology refresh occurs.
    pub fn with_contact_point(mut self, addr: NodeAddress) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Finalizes building process
    pub async fn build(self) -> Result<NodeTcpConfig> {
        // replace with map() when async lambdas become available
        let mut contact_points = Vec::with_capacity(self.addrs.len());
        for contact_point in self.addrs {
            contact_points.append(&mut contact_point.resolve_address().await?);
        }

        Ok(NodeTcpConfig {
            contact_points,
            authenticator_provider: self.authenticator_provider,
        })
    }
}
