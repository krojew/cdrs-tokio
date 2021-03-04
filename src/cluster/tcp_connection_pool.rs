use async_trait::async_trait;
use bb8::{Builder, ManageConnection, PooledConnection};
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::authenticators::Authenticator;
use crate::cluster::ConnectionPool;
use crate::cluster::KeyspaceHolder;
use crate::cluster::NodeTcpConfig;
use crate::compression::Compression;
use crate::error;
use crate::frame::parser::parse_frame;
use crate::frame::{AsBytes, Frame, Opcode};
use crate::transport::{CDRSTransport, TransportTcp};
use std::ops::Deref;

/// Shortcut for `bb8::Pool` type of TCP-based CDRS connections.
pub type TcpConnectionPool = ConnectionPool<TransportTcp>;

/// `bb8::Pool` of TCP-based CDRS connections.
///
/// Used internally for TCP Session for holding connections to a specific Cassandra node.
pub async fn new_tcp_pool(node_config: NodeTcpConfig) -> error::Result<TcpConnectionPool> {
    let manager =
        TcpConnectionsManager::new(node_config.addr.to_string(), node_config.authenticator);

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
    auth: Arc<dyn Authenticator + Send + Sync>,
    keyspace_holder: Arc<KeyspaceHolder>,
}

impl TcpConnectionsManager {
    pub fn new<S: ToString>(addr: S, auth: Arc<dyn Authenticator + Send + Sync>) -> Self {
        TcpConnectionsManager {
            addr: addr.to_string(),
            auth,
            keyspace_holder: Default::default(),
        }
    }
}

#[async_trait]
impl ManageConnection for TcpConnectionsManager {
    type Connection = Mutex<TransportTcp>;
    type Error = error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport =
            Mutex::new(TransportTcp::new(&self.addr, self.keyspace_holder.clone()).await?);
        startup(&transport, self.auth.deref(), self.keyspace_holder.deref()).await?;

        Ok(transport)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let options_frame = Frame::new_req_options().as_bytes();
        conn.lock().await.write(options_frame.as_slice()).await?;

        parse_frame(&conn, Compression::None).await.map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // cannot synchronously determine broken connection, so return false as per bb8 docs
        false
    }
}

pub async fn startup<
    T: CDRSTransport + Unpin + 'static,
    A: Authenticator + Send + Sync + ?Sized + 'static,
>(
    transport: &Mutex<T>,
    session_authenticator: &A,
    keyspace_holder: &KeyspaceHolder,
) -> error::Result<()> {
    let compression = Compression::None;
    let startup_frame = Frame::new_req_startup(compression.as_str()).as_bytes();

    transport
        .lock()
        .await
        .write(startup_frame.as_slice())
        .await?;

    let start_response = parse_frame(transport, compression).await?;

    if start_response.opcode == Opcode::Ready {
        return Ok(());
    }

    if start_response.opcode == Opcode::Authenticate {
        let body = start_response.get_body()?;
        let authenticator = body.get_authenticator().expect(
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
            .get_cassandra_name()
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

        let auth_token_bytes = session_authenticator.get_auth_token();
        transport
            .lock()
            .await
            .write(
                Frame::new_req_auth_response(auth_token_bytes)
                    .as_bytes()
                    .as_slice(),
            )
            .await?;
        parse_frame(transport, compression).await?;

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
            );

            transport
                .lock()
                .await
                .write(use_frame.as_bytes().as_slice())
                .await?;
            parse_frame(transport, compression).await?;
        }

        return Ok(());
    }

    unreachable!();
}
