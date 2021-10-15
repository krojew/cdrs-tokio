use derive_more::Display;
use std::net::SocketAddr;
use tokio::net::lookup_host;

use crate::error::Result;

/// Representation of a node address. Can be a direct socket address or a hostname. In the latter
/// case, the host can be resolved to multiple addresses, which could result in multiple node
/// configurations.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Debug)]
pub enum NodeAddress {
    Direct(SocketAddr),
    Hostname(String),
}

impl From<SocketAddr> for NodeAddress {
    fn from(addr: SocketAddr) -> Self {
        NodeAddress::Direct(addr)
    }
}

impl From<String> for NodeAddress {
    fn from(value: String) -> Self {
        NodeAddress::Hostname(value)
    }
}

impl From<&String> for NodeAddress {
    fn from(value: &String) -> Self {
        NodeAddress::Hostname(value.clone())
    }
}

impl From<&str> for NodeAddress {
    fn from(value: &str) -> Self {
        NodeAddress::Hostname(value.to_string())
    }
}

impl NodeAddress {
    /// Resolves this address to socket addresses.
    pub async fn resolve_address(&self) -> Result<Vec<SocketAddr>> {
        match self {
            NodeAddress::Direct(addr) => Ok(vec![*addr]),
            NodeAddress::Hostname(hostname) => lookup_host(hostname)
                .await
                .map(|addrs| addrs.collect())
                .map_err(Into::into),
        }
    }
}
