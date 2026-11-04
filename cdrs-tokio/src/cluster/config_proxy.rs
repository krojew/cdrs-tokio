use std::fmt::{Debug, Formatter};

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
