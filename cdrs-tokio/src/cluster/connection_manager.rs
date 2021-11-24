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
use cassandra_protocol::frame::frame_response::ResponseBody;
use cassandra_protocol::frame::{Frame, Opcode, Version};

/// Manages establishing connections to nodes.
pub trait ConnectionManager<T: CdrsTransport>: Send + Sync {
    /// Tries to establish a new, ready to use connection with optional server event and error
    /// handlers.
    fn connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<T>>;
}

#[cfg(test)]
mock! {
    pub ConnectionManager<T: CdrsTransport> {
    }

    impl<T: CdrsTransport> ConnectionManager<T> for ConnectionManager<T> {
        fn connection<'a>(
            &'a self,
            event_handler: Option<Sender<Frame>>,
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
    let startup_frame = Frame::new_req_startup(compression.as_str(), version);
    let start_response = transport.write_frame(&startup_frame).await?;

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

        let auth_check = authenticator_provider
            .name()
            .ok_or_else(|| Error::General("No authenticator was provided".to_string()))
            .map(|auth| {
                if authenticator != auth {
                    let io_err = io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Unsupported type of authenticator. {:?} got,
                             but {} is supported.",
                            authenticator, auth
                        ),
                    );
                    return Err(Error::Io(io_err));
                }
                Ok(())
            });

        if let Err(err) = auth_check {
            return Err(err);
        }

        let authenticator = authenticator_provider.create_authenticator();
        let response = authenticator.initial_response();
        let mut frame = transport
            .write_frame(&Frame::new_req_auth_response(response, version))
            .await?;

        loop {
            match frame.response_body()? {
                ResponseBody::AuthChallenge(challenge) => {
                    let response = authenticator.evaluate_challenge(challenge.data)?;

                    frame = transport
                        .write_frame(&Frame::new_req_auth_response(response, version))
                        .await?;
                }
                ResponseBody::AuthSuccess => break,
                _ => return Err(format!("Unexpected auth response: {:?}", frame.opcode).into()),
            }
        }

        return set_keyspace(transport, keyspace_holder, version).await;
    }

    unreachable!();
}

async fn set_keyspace<T: CdrsTransport>(
    transport: &T,
    keyspace_holder: &KeyspaceHolder,
    version: Version,
) -> Result<()> {
    if let Some(current_keyspace) = keyspace_holder.current_keyspace() {
        let use_frame = Frame::new_req_query(
            format!("USE {}", current_keyspace),
            Default::default(),
            None,
            false,
            None,
            None,
            None,
            None,
            Default::default(),
            false,
            version,
        );

        transport.write_frame(&use_frame).await.map(|_| ())
    } else {
        Ok(())
    }
}
