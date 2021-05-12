use crate::error::Result;
use crate::types::CBytes;

#[deprecated(note = "Use the new SaslAuthenticator.")]
pub trait Authenticator {
    fn auth_token(&self) -> CBytes;
    fn cassandra_name(&self) -> Option<&str>;
}

/// Handles SASL authentication. The lifecycle of an authenticator consists of:
/// - The `initial_response` function will be called. The initial return value will be sent to the
/// server to initiate the handshake.
/// - The server will respond to each client response by either issuing a challenge or indicating
///  that the authentication is complete (successfully or not). If a new challenge is issued,
///  the authenticator's `evaluate_challenge` function will be called to produce a response
///  that will be sent to the server. This challenge/response negotiation will continue until
///  the server responds that authentication is successful or an error is raised.
pub trait SaslAuthenticator {
    fn name(&self) -> Option<&str>;

    fn initial_response(&self) -> CBytes;

    fn evaluate_challenge(&self, challenge: CBytes) -> Result<CBytes>;
}

#[allow(deprecated)]
impl<A> SaslAuthenticator for A
where
    A: Authenticator,
{
    fn name(&self) -> Option<&str> {
        self.cassandra_name()
    }

    fn initial_response(&self) -> CBytes {
        self.auth_token()
    }

    fn evaluate_challenge(&self, _challenge: CBytes) -> Result<CBytes> {
        Err("Server challenges are not supported for legacy authenticators!".into())
    }
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

impl SaslAuthenticator for StaticPasswordAuthenticator {
    fn name(&self) -> Option<&str> {
        Some("org.apache.cassandra.auth.PasswordAuthenticator")
    }

    fn initial_response(&self) -> CBytes {
        let mut token = vec![0];
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());

        CBytes::new(token)
    }

    fn evaluate_challenge(&self, _challenge: CBytes) -> Result<CBytes> {
        Err("Server challenge is not supported for StaticPasswordAuthenticator!".into())
    }
}

#[derive(Debug, Clone)]
pub struct NoneAuthenticator;

impl SaslAuthenticator for NoneAuthenticator {
    fn name(&self) -> Option<&str> {
        None
    }

    fn initial_response(&self) -> CBytes {
        CBytes::new(vec![0])
    }

    fn evaluate_challenge(&self, _challenge: CBytes) -> Result<CBytes> {
        Err("Server challenge is not supported for NoneAuthenticator!".into())
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
            auth.name(),
            Some("org.apache.cassandra.auth.PasswordAuthenticator")
        );
    }

    #[test]
    fn test_authenticator_none_cassandra_name() {
        let auth = NoneAuthenticator;
        assert_eq!(auth.name(), None);
        assert_eq!(auth.initial_response().into_plain().unwrap(), vec![0]);
    }
}
