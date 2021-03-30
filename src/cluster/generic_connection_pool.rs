use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::CdrsTransport;

/// Generic pool connection that is able to return an
/// `bb8::Pool` as well as an IP address of a node.
#[derive(Debug)]
pub struct ConnectionPool<T: CdrsTransport> {
    pool: Arc<bb8::Pool<T::Manager>>,
    addr: SocketAddr,
}

impl<T: CdrsTransport> ConnectionPool<T> {
    pub fn new(pool: bb8::Pool<T::Manager>, addr: SocketAddr) -> Self {
        ConnectionPool {
            pool: Arc::new(pool),
            addr,
        }
    }

    /// Returns reference to underlying `bb8::Pool`.
    pub fn get_pool(&self) -> Arc<bb8::Pool<T::Manager>> {
        self.pool.clone()
    }

    /// Return an IP address.
    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
}
