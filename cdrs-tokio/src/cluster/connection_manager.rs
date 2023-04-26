use std::io;
use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;

#[cfg(test)]
use mockall::*;

use crate::cluster::KeyspaceHolder;
use crate::future::BoxFuture;
use crate::transport::CdrsTransport;
use cassandra_protocol::authenticators::SaslAuthenticatorProvider;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::error::{Error, Result};
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::{Envelope, Opcode, Version};
use cassandra_protocol::query::utils::quote;

/// Manages establishing connections to nodes.
pub trait ConnectionManager<T: CdrsTransport>: Send + Sync {
    /// Tries to establish a new, ready to use connection with optional server event and error
    /// handlers.
    fn connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<T>>;
}

#[cfg(test)]
mock! {
    pub ConnectionManager<T: CdrsTransport> {
    }

    #[allow(dead_code)]
    impl<T: CdrsTransport> ConnectionManager<T> for ConnectionManager<T> {
        fn connection<'a>(
            &'a self,
            event_handler: Option<Sender<Envelope>>,
            error_handler: Option<Sender<Error>>,
            addr: SocketAddr,
        ) -> BoxFuture<'a, Result<T>>;
    }
}

/// Establishes Cassandra connection with given authentication, last used keyspace and compression.
pub async fn startup<
    T: CdrsTransport + 'static,
    A: SaslAuthenticatorProvider + Send + Sync + ?Sized + 'static,
>(
    transport: &T,
    authenticator_provider: &A,
    keyspace_holder: &KeyspaceHolder,
    compression: Compression,
    version: Version,
) -> Result<()> {
    let startup_envelope =
        Envelope::new_req_startup(compression.as_str().map(String::from), version);

    let start_response = match transport.write_envelope(&startup_envelope, true).await {
        Ok(response) => Ok(response),
        Err(Error::Server { body, .. }) if body.is_bad_protocol() => {
            Err(Error::InvalidProtocol(transport.address()))
        }
        Err(error) => Err(error),
    }?;

    if start_response.opcode == Opcode::Ready {
        return set_keyspace(transport, keyspace_holder, version).await;
    }

    if start_response.opcode == Opcode::Authenticate {
        let body = start_response.response_body()?;
        let authenticator = body.authenticator()
            .ok_or_else(|| Error::General("Cassandra server did communicate that it needed authentication but the auth schema was missing in the body response".into()))?;

        // This creates a new scope; avoiding a clone
        // and we check whether
        // 1. any authenticators has been passed in by client and if not send error back
        // 2. authenticator is provided by the client and `auth_scheme` presented by
        //      the server and client are same if not send error back
        // 3. if it falls through it means the preliminary conditions are true

        authenticator_provider
            .name()
            .ok_or_else(|| Error::General("No authenticator was provided".to_string()))
            .and_then(|auth| {
                if authenticator != auth {
                    let io_err = io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Unsupported type of authenticator. {authenticator:?} got,
                             but {auth} is supported."
                        ),
                    );
                    return Err(Error::Io(io_err));
                }
                Ok(())
            })?;

        let authenticator = authenticator_provider.create_authenticator();
        let response = authenticator.initial_response();
        let mut envelope = transport
            .write_envelope(&Envelope::new_req_auth_response(response, version), false)
            .await?;

        loop {
            match envelope.response_body()? {
                ResponseBody::AuthChallenge(challenge) => {
                    let response = authenticator.evaluate_challenge(challenge.data)?;

                    envelope = transport
                        .write_envelope(&Envelope::new_req_auth_response(response, version), false)
                        .await?;
                }
                ResponseBody::AuthSuccess(success) => {
                    authenticator.handle_success(success.data)?;
                    break;
                }
                _ => return Err(Error::UnexpectedAuthResponse(envelope.opcode)),
            }
        }

        return set_keyspace(transport, keyspace_holder, version).await;
    }

    Err(Error::UnexpectedStartupResponse(start_response.opcode))
}

async fn set_keyspace<T: CdrsTransport>(
    transport: &T,
    keyspace_holder: &KeyspaceHolder,
    version: Version,
) -> Result<()> {
    if let Some(current_keyspace) = keyspace_holder.current_keyspace() {
        let use_envelope = Envelope::new_req_query(
            format!("USE {}", quote(current_keyspace.as_ref())),
            Default::default(),
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            Default::default(),
            version,
        );

        transport
            .write_envelope(&use_envelope, false)
            .await
            .map(|_| ())
    } else {
        Ok(())
    }
}
