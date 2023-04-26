use cassandra_protocol::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use cassandra_protocol::error::Result;
use cassandra_protocol::frame::Version;
use derivative::Derivative;
use std::net::SocketAddr;
use std::sync::Arc;

#[cfg(feature = "http-proxy")]
use crate::cluster::HttpProxyConfig;
use crate::cluster::NodeAddress;

/// Single node TCP connection config. See [NodeTcpConfigBuilder].
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct NodeTcpConfig {
    pub(crate) contact_points: Vec<SocketAddr>,
    #[derivative(Debug = "ignore")]
    pub(crate) authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub(crate) version: Version,
    pub(crate) beta_protocol: bool,
    #[cfg(feature = "http-proxy")]
    pub(crate) http_proxy: Option<HttpProxyConfig>,
}

/// Builder structure that helps to configure TCP connection for node.
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct NodeTcpConfigBuilder {
    addrs: Vec<NodeAddress>,
    #[derivative(Debug = "ignore")]
    authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    version: Version,
    beta_protocol: bool,
    #[cfg(feature = "http-proxy")]
    http_proxy: Option<HttpProxyConfig>,
}

impl Default for NodeTcpConfigBuilder {
    fn default() -> Self {
        NodeTcpConfigBuilder {
            addrs: vec![],
            authenticator_provider: Arc::new(NoneAuthenticatorProvider),
            version: Version::V4,
            beta_protocol: false,
            #[cfg(feature = "http-proxy")]
            http_proxy: None,
        }
    }
}

impl NodeTcpConfigBuilder {
    pub fn new() -> NodeTcpConfigBuilder {
        Default::default()
    }

    /// Sets new authenticator.
    #[must_use]
    pub fn with_authenticator_provider(
        mut self,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    ) -> Self {
        self.authenticator_provider = authenticator_provider;
        self
    }

    /// Adds initial node address (a contact point). Contact points are considered local to the
    /// driver until a topology refresh occurs.
    #[must_use]
    pub fn with_contact_point(mut self, addr: NodeAddress) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Adds initial node addresses
    #[must_use]
    pub fn with_contact_points(mut self, addr: Vec<NodeAddress>) -> Self {
        self.addrs.extend(addr);
        self
    }

    /// Set cassandra protocol version
    #[must_use]
    pub fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Sets beta protocol usage flag
    #[must_use]
    pub fn with_beta_protocol(mut self, beta_protocol: bool) -> Self {
        self.beta_protocol = beta_protocol;
        self
    }

    /// Adds HTTP proxy configuration
    #[cfg(feature = "http-proxy")]
    #[must_use]
    pub fn with_http_proxy(mut self, config: HttpProxyConfig) -> Self {
        self.http_proxy = Some(config);
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
            version: self.version,
            beta_protocol: self.beta_protocol,
            #[cfg(feature = "http-proxy")]
            http_proxy: self.http_proxy,
        })
    }
}
