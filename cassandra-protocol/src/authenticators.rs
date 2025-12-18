use crate::error::Result;
use crate::types::CBytes;

/// Handles SASL authentication.
///
/// The lifecycle of an authenticator consists of:
/// - The `initial_response` function will be called. The initial return value will be sent to the
///   server to initiate the handshake.
/// - The server will respond to each client response by either issuing a challenge or indicating
///   that the authentication is complete (successfully or not). If a new challenge is issued,
///   the authenticator's `evaluate_challenge` function will be called to produce a response
///   that will be sent to the server. This challenge/response negotiation will continue until
///   the server responds that authentication is successful or an error is raised.
/// - On success, the `handle_success` will be called with data returned by the server.
pub trait SaslAuthenticator {
    fn initial_response(&self) -> CBytes;

    fn evaluate_challenge(&self, challenge: CBytes) -> Result<CBytes>;

    fn handle_success(&self, data: CBytes) -> Result<()>;
}

/// Provides authenticators per new connection.
pub trait SaslAuthenticatorProvider {
    fn name(&self) -> Option<&str>;

    fn create_authenticator(&self) -> Box<dyn SaslAuthenticator + Send>;
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

    fn handle_success(&self, _data: CBytes) -> Result<()> {
        Ok(())
    }
}

/// Authentication provider with a username and password.
#[derive(Debug, Clone)]
pub struct StaticPasswordAuthenticatorProvider {
    username: String,
    password: String,
}

impl SaslAuthenticatorProvider for StaticPasswordAuthenticatorProvider {
    fn name(&self) -> Option<&str> {
        Some("org.apache.cassandra.auth.PasswordAuthenticator")
    }

    fn create_authenticator(&self) -> Box<dyn SaslAuthenticator + Send> {
        Box::new(StaticPasswordAuthenticator::new(
            self.username.clone(),
            self.password.clone(),
        ))
    }
}

impl StaticPasswordAuthenticatorProvider {
    pub fn new<S: ToString>(username: S, password: S) -> Self {
        StaticPasswordAuthenticatorProvider {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NoneAuthenticator;

impl SaslAuthenticator for NoneAuthenticator {
    fn initial_response(&self) -> CBytes {
        CBytes::new(vec![0])
    }

    fn evaluate_challenge(&self, _challenge: CBytes) -> Result<CBytes> {
        Err("Server challenge is not supported for NoneAuthenticator!".into())
    }

    fn handle_success(&self, _data: CBytes) -> Result<()> {
        Ok(())
    }
}

/// Provider for no authentication.
#[derive(Debug, Clone)]
pub struct NoneAuthenticatorProvider;

impl SaslAuthenticatorProvider for NoneAuthenticatorProvider {
    fn name(&self) -> Option<&str> {
        None
    }

    fn create_authenticator(&self) -> Box<dyn SaslAuthenticator + Send> {
        Box::new(NoneAuthenticator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_password_authenticator_new() {
        StaticPasswordAuthenticator::new("foo", "bar");
    }

    #[test]
    fn test_static_password_authenticator_cassandra_name() {
        let auth = StaticPasswordAuthenticatorProvider::new("foo", "bar");
        assert_eq!(
            auth.name(),
            Some("org.apache.cassandra.auth.PasswordAuthenticator")
        );
    }

    #[test]
    fn test_authenticator_none_cassandra_name() {
        let auth = NoneAuthenticator;
        let provider = NoneAuthenticatorProvider;
        assert_eq!(provider.name(), None);
        assert_eq!(auth.initial_response().into_bytes().unwrap(), vec![0]);
    }
}
