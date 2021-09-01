use std::net::SocketAddr;

/// Representation of a node address. Can be a direct socket address or a hostname. In the latter
/// case, the host can be resolved to multiple addresses, which could result in multiple node
/// configurations.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

impl From<&str> for NodeAddress {
    fn from(value: &str) -> Self {
        NodeAddress::Hostname(value.to_string())
    }
}
