use std::ops::Deref;

use async_trait::async_trait;
use bb8::{Builder, ManageConnection, PooledConnection};
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use crate::authenticators::SaslAuthenticatorProvider;
use crate::cluster::ConnectionPool;
use crate::cluster::KeyspaceHolder;
use crate::cluster::NodeTcpConfig;
use crate::compression::Compression;
use crate::error;
use crate::frame::frame_response::ResponseBody;
use crate::frame::{Frame, Opcode};
use crate::transport::{CdrsTransport, TransportTcp};

/// Shortcut for `bb8::Pool` type of TCP-based CDRS connections.
pub type TcpConnectionPool = ConnectionPool<TransportTcp>;

/// `bb8::Pool` of TCP-based CDRS connections.
///
/// Used internally for TCP Session for holding connections to a specific Cassandra node.
//noinspection DuplicatedCode
pub async fn new_tcp_pool(
    node_config: NodeTcpConfig,
    compression: Compression,
) -> error::Result<TcpConnectionPool> {
    let manager = TcpConnectionsManager::new(
        node_config.addr.to_string(),
        node_config.authenticator,
        compression,
    );

    let pool = Builder::new()
        .max_size(node_config.max_size)
        .min_idle(node_config.min_idle)
        .max_lifetime(node_config.max_lifetime)
        .idle_timeout(node_config.idle_timeout)
        .connection_timeout(node_config.connection_timeout)
        .build(manager)
        .await
        .map_err(|err| error::Error::from(err.to_string()))?;

    let addr = node_config
        .addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| error::Error::from("Cannot parse address"))?;

    Ok(TcpConnectionPool::new(pool, addr))
}

/// `bb8` connection manager.
pub struct TcpConnectionsManager {
    addr: String,
    auth: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
    compression: Compression,
}

impl TcpConnectionsManager {
    pub fn new<S: ToString>(
        addr: S,
        auth: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        compression: Compression,
    ) -> Self {
        TcpConnectionsManager {
            addr: addr.to_string(),
            auth,
            keyspace_holder: Default::default(),
            compression,
        }
    }
}

#[async_trait]
impl ManageConnection for TcpConnectionsManager {
    type Connection = TransportTcp;
    type Error = error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = TransportTcp::new(
            &self.addr,
            self.keyspace_holder.clone(),
            None,
            self.compression,
        )
        .await?;
        startup(
            &transport,
            self.auth.deref(),
            self.keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        Ok(transport)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let options_frame = Frame::new_req_options();
        conn.write_frame(options_frame).await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_broken()
    }
}

pub async fn startup<
    T: CdrsTransport + Unpin + 'static,
    A: SaslAuthenticatorProvider + Send + Sync + ?Sized + 'static,
>(
    transport: &T,
    session_authenticator: &A,
    keyspace_holder: &KeyspaceHolder,
    compression: Compression,
) -> error::Result<()> {
    let startup_frame = Frame::new_req_startup(compression.as_str());
    let start_response = transport.write_frame(startup_frame).await?;

    if start_response.opcode == Opcode::Ready {
        return set_keyspace(transport, keyspace_holder).await;
    }

    if start_response.opcode == Opcode::Authenticate {
        let body = start_response.body()?;
        let authenticator = body.authenticator().expect(
            "Cassandra Server did communicate that it needed
                authentication but the auth schema was missing in the body response",
        );

        // This creates a new scope; avoiding a clone
        // and we check whether
        // 1. any authenticators has been passed in by client and if not send error back
        // 2. authenticator is provided by the client and `auth_scheme` presented by
        //      the server and client are same if not send error back
        // 3. if it falls through it means the preliminary conditions are true

        let auth_check = session_authenticator
            .name()
            .ok_or_else(|| error::Error::General("No authenticator was provided".to_string()))
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
                    return Err(error::Error::Io(io_err));
                }
                Ok(())
            });

        if let Err(err) = auth_check {
            return Err(err);
        }

        let authenticator = session_authenticator.create_authenticator();
        let response = authenticator.initial_response();
        let mut frame = transport
            .write_frame(Frame::new_req_auth_response(response))
            .await?;

        loop {
            match frame.body()? {
                ResponseBody::AuthChallenge(challenge) => {
                    let response = authenticator.evaluate_challenge(challenge.data)?;

                    frame = transport
                        .write_frame(Frame::new_req_auth_response(response))
                        .await?;
                }
                ResponseBody::AuthSuccess(..) => break,
                _ => return Err(format!("Unexpected auth response: {:?}", frame.opcode).into()),
            }
        }

        return set_keyspace(transport, keyspace_holder).await;
    }

    unreachable!();
}

async fn set_keyspace<T: CdrsTransport + Unpin>(
    transport: &T,
    keyspace_holder: &KeyspaceHolder,
) -> error::Result<()> {
    if let Some(current_keyspace) = keyspace_holder.current_keyspace().await {
        let use_frame = Frame::new_req_query(
            format!("USE {}", current_keyspace),
            Default::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            Default::default(),
            false,
        );

        transport.write_frame(use_frame).await.map(|_| ())
    } else {
        Ok(())
    }
}
