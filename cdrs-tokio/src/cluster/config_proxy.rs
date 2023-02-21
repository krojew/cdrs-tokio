#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) struct HttpBasicAuth {
    pub(crate) username: String,
    pub(crate) password: String,
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
