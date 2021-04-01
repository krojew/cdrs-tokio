use crate::types::CBytes;

pub trait Authenticator {
    fn auth_token(&self) -> CBytes;
    fn cassandra_name(&self) -> Option<&str>;
}

#[derive(Debug, Clone)]
pub struct StaticPasswordAuthenticator {
    username: String,
    password: String,
}

impl StaticPasswordAuthenticator {
    pub fn new<S: ToString>(username: S, password: S) -> StaticPasswordAuthenticator {
        StaticPasswordAuthenticator {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl Authenticator for StaticPasswordAuthenticator {
    fn auth_token(&self) -> CBytes {
        let mut token = vec![0];
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());

        CBytes::new(token)
    }

    fn cassandra_name(&self) -> Option<&str> {
        Some("org.apache.cassandra.auth.PasswordAuthenticator")
    }
}

#[derive(Debug, Clone)]
pub struct NoneAuthenticator;

impl Authenticator for NoneAuthenticator {
    fn auth_token(&self) -> CBytes {
        CBytes::new(vec![0])
    }

    fn cassandra_name(&self) -> Option<&str> {
        None
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_static_password_authenticator_new() {
        StaticPasswordAuthenticator::new("foo", "bar");
    }

    #[test]
    fn test_static_password_authenticator_cassandra_name() {
        let auth = StaticPasswordAuthenticator::new("foo", "bar");
        assert_eq!(
            auth.cassandra_name(),
            Some("org.apache.cassandra.auth.PasswordAuthenticator")
        );
    }

    #[test]
    fn test_authenticator_none_cassandra_name() {
        let auth = NoneAuthenticator;
        assert_eq!(auth.cassandra_name(), None);
        assert_eq!(auth.auth_token().into_plain().unwrap(), vec![0]);
    }
}
