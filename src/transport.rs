//!This module contains a declaration of `CDRSTransport` trait which should be implemented
//!for particular transport in order to be able using it as a transport of CDRS client.
//!
//!Currently CDRS provides to concrete transports which implement `CDRSTranpsport` trait. There
//! are:
//!
//! * [`TransportTcp`] is default TCP transport which is usually used to establish
//!connection and exchange frames.
//!
//! * [`TransportRustls`] is a transport which is used to establish SSL encrypted connection
//!with Apache Cassandra server. **Note:** this option is available if and only if CDRS is imported
//!with `rust-tls` feature.
use async_trait::async_trait;
use std::io;
use std::sync::Arc;
use std::task::Context;
use tokio::{io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf}, sync::Mutex};
use tokio::macros::support::{Pin, Poll};
use tokio::net::TcpStream;
use crate::{Error, cluster::{KeyspaceHolder, TcpConnectionsManager}, error::FromCDRSError};

#[cfg(feature = "rust-tls")]
use tokio_rustls::{client::TlsStream as RustlsStream, TlsConnector as RustlsConnector};
#[cfg(feature = "rust-tls")]
use std::net;
#[cfg(feature = "rust-tls")]
use crate::cluster::RustlsConnectionsManager;

// TODO [v x.x.x]: CDRSTransport: ... + BufReader + ButWriter + ...
///General CDRS transport trait. Both [`TransportTcp`]
///and [`TransportRustls`] has their own implementations of this trait. Generaly
///speaking it extends/includes `io::Read` and `io::Write` traits and should be thread safe.
#[async_trait]
pub trait CDRSTransport: Sized + AsyncRead + AsyncWriteExt + Send + Sync {
    type Error: FromCDRSError;
    type Manager: bb8::ManageConnection<Connection = Mutex<Self>, Error = Self::Error>;

    /// Sets last USEd keyspace for further connections from the same pool
    async fn set_current_keyspace(&self, keyspace: &str);
}

/// Default Tcp transport.
#[derive(Debug)]
pub struct TransportTcp {
    tcp: TcpStream,
    keyspace_holder: Arc<KeyspaceHolder>,
}

impl TransportTcp {
    /// Constructs a new `TransportTcp`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cdrs_tokio::transport::TransportTcp;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = "127.0.0.1:9042";
    ///     let tcp_transport = TransportTcp::new(addr, Default::default()).await.unwrap();
    /// }
    /// ```
    pub async fn new(addr: &str, keyspace_holder: Arc<KeyspaceHolder>) -> io::Result<TransportTcp> {
        TcpStream::connect(addr).await.map(|socket| TransportTcp {
            tcp: socket,
            keyspace_holder,
        })
    }
}

impl AsyncRead for TransportTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp).poll_read(cx, buf)
    }
}

impl AsyncWrite for TransportTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.tcp).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.tcp).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.tcp).poll_shutdown(cx)
    }
}

#[async_trait]
impl CDRSTransport for TransportTcp {
    type Error = Error;
    type Manager = TcpConnectionsManager;

    async fn set_current_keyspace(&self, keyspace: &str) {
        self.keyspace_holder.set_current_keyspace(keyspace).await;
    }
}

#[cfg(feature = "rust-tls")]
pub struct TransportRustls {
    inner: RustlsStream<TcpStream>,
    keyspace_holder: Arc<KeyspaceHolder>,
}

#[cfg(feature = "rust-tls")]
impl TransportRustls {
    ///Creates new instance with provided configuration
    pub async fn new(
        addr: net::SocketAddr,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let connector = RustlsConnector::from(config.clone());
        let stream = connector.connect(dns_name.as_ref(), stream).await?;

        Ok(Self {
            inner: stream,
            keyspace_holder,
        })
    }
}

#[cfg(feature = "rust-tls")]
impl AsyncRead for TransportRustls {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[cfg(feature = "rust-tls")]
impl AsyncWrite for TransportRustls {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(feature = "rust-tls")]
#[async_trait]
impl CDRSTransport for TransportRustls {
    type Error = Error;
    type Manager = RustlsConnectionsManager;

    async fn set_current_keyspace(&self, keyspace: &str) {
        self.keyspace_holder.set_current_keyspace(keyspace).await;
    }
}
