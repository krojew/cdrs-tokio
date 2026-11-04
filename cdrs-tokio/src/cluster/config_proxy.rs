use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

/// Returns the host part of an HTTP CONNECT request target, with IPv6 literals in brackets.
pub(crate) fn connect_host(addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => format!("[{}]", addr.ip()),
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct HttpBasicAuth {
    pub(crate) username: String,
    pub(crate) password: String,
}

impl Debug for HttpBasicAuth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpBasicAuth")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct HttpProxyConfig {
    pub(crate) address: String,
    pub(crate) basic_auth: Option<HttpBasicAuth>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct HttpProxyConfigBuilder {
    address: String,
    basic_auth: Option<HttpBasicAuth>,
}

impl HttpProxyConfigBuilder {
    /// Creates a new proxy configuration builder with given proxy address.
    pub fn new(address: String) -> Self {
        Self {
            address,
            basic_auth: None,
        }
    }

    /// Adds HTTP basic Auth.
    pub fn with_basic_auth(mut self, username: String, password: String) -> Self {
        self.basic_auth = Some(HttpBasicAuth { password, username });
        self
    }

    /// Build the resulting configuration.
    pub fn build(self) -> HttpProxyConfig {
        HttpProxyConfig {
            basic_auth: self.basic_auth,
            address: self.address,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn connect_host_brackets_only_ipv6() {
        assert_eq!(
            connect_host(SocketAddr::new(Ipv4Addr::new(10, 0, 0, 1).into(), 9042)),
            "10.0.0.1"
        );
        assert_eq!(
            connect_host(SocketAddr::new(
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).into(),
                9042
            )),
            "[2001:db8::1]"
        );
    }

    #[test]
    fn debug_output_redacts_basic_auth_password() {
        let config = HttpProxyConfigBuilder::new("127.0.0.1:3128".into())
            .with_basic_auth("proxy-user".into(), "proxy-secret".into());

        for output in [format!("{config:?}"), format!("{:?}", config.build())] {
            assert!(output.contains("proxy-user"), "{}", output);
            assert!(!output.contains("proxy-secret"), "{}", output);
        }
    }
}
