//!This module contains a declaration of `CdrsTransport` trait which should be implemented
//!for particular transport in order to be able using it as a transport of CDRS client.
//!
//!Currently CDRS provides to concrete transports which implement `CdrsTransport` trait. There
//! are:
//!
//! * [`TransportTcp`] is default TCP transport which is usually used to establish
//!connection and exchange frames.
//!
//! * [`TransportRustls`] is a transport which is used to establish SSL encrypted connection
//!with Apache Cassandra server. **Note:** this option is available if and only if CDRS is imported
//!with `rust-tls` feature.
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::frame_result::ResultKind;
use cassandra_protocol::frame::{Frame, StreamId};
use cassandra_protocol::frame::{FromBytes, Opcode, EVENT_STREAM_ID};
use cassandra_protocol::types::INT_LEN;
use derive_more::Constructor;
use futures::FutureExt;
use fxhash::FxHashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{split, AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
#[cfg(feature = "rust-tls")]
use tokio_rustls::TlsConnector as RustlsConnector;
use tracing::*;

#[cfg(test)]
use mockall::*;

use crate::cluster::KeyspaceHolder;
use crate::frame_parser::parse_frame;
use crate::future::BoxFuture;
use crate::Error;
use crate::Result;

/// General CDRS transport trait.
pub trait CdrsTransport: Send + Sync {
    /// Schedules data frame for writing and waits for a response
    fn write_frame<'a>(&'a self, frame: &'a Frame) -> BoxFuture<'a, Result<Frame>>;

    /// Checks if the connection is broken (e.g. after read or write errors)
    fn is_broken(&self) -> bool;

    /// Returns associated node address
    fn address(&self) -> SocketAddr;
}

#[cfg(test)]
mock! {
    pub CdrsTransport {
    }

    impl CdrsTransport for CdrsTransport {
        fn write_frame(&self, frame: &Frame) -> BoxFuture<'static, Result<Frame>>;

        fn is_broken(&self) -> bool;

        fn address(&self) -> SocketAddr;
    }
}

/// Default Tcp transport.
pub struct TransportTcp {
    inner: AsyncTransport,
}

impl TransportTcp {
    pub async fn new(
        addr: SocketAddr,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Frame>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
    ) -> io::Result<TransportTcp> {
        TcpStream::connect(addr).await.and_then(move |socket| {
            socket.set_nodelay(tcp_nodelay)?;

            let (read_half, write_half) = split(socket);
            Ok(TransportTcp {
                inner: AsyncTransport::new(
                    addr,
                    compression,
                    buffer_size,
                    read_half,
                    write_half,
                    event_handler,
                    error_handler,
                    keyspace_holder,
                ),
            })
        })
    }
}

impl CdrsTransport for TransportTcp {
    //noinspection DuplicatedCode
    #[inline]
    fn write_frame<'a>(&'a self, frame: &'a Frame) -> BoxFuture<'a, Result<Frame>> {
        self.inner.write_frame(frame).boxed()
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.inner.is_broken()
    }

    #[inline]
    fn address(&self) -> SocketAddr {
        self.inner.addr()
    }
}

#[cfg(feature = "rust-tls")]
pub struct TransportRustls {
    inner: AsyncTransport,
}

#[cfg(feature = "rust-tls")]
impl TransportRustls {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        addr: SocketAddr,
        dns_name: webpki::DNSName,
        config: Arc<rustls::ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Frame>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        buffer_size: usize,
        tcp_nodelay: bool,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(tcp_nodelay)?;

        let connector = RustlsConnector::from(config.clone());
        let stream = connector.connect(dns_name.as_ref(), stream).await?;
        let (read_half, write_half) = split(stream);

        Ok(Self {
            inner: AsyncTransport::new(
                addr,
                compression,
                buffer_size,
                read_half,
                write_half,
                event_handler,
                error_handler,
                keyspace_holder,
            ),
        })
    }
}

#[cfg(feature = "rust-tls")]
impl CdrsTransport for TransportRustls {
    //noinspection DuplicatedCode
    #[inline]
    fn write_frame<'a>(&'a self, frame: &'a Frame) -> BoxFuture<'a, Result<Frame>> {
        self.inner.write_frame(frame).boxed()
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.inner.is_broken()
    }

    #[inline]
    fn address(&self) -> SocketAddr {
        self.inner.addr()
    }
}

struct AsyncTransport {
    addr: SocketAddr,
    compression: Compression,
    write_sender: mpsc::Sender<Request>,
    is_broken: Arc<AtomicBool>,
    processing_handle: JoinHandle<()>,
}

impl Drop for AsyncTransport {
    fn drop(&mut self) {
        self.processing_handle.abort();
    }
}

impl AsyncTransport {
    #[allow(clippy::too_many_arguments)]
    fn new<T: AsyncRead + AsyncWrite + Send + 'static>(
        addr: SocketAddr,
        compression: Compression,
        buffer_size: usize,
        read_half: ReadHalf<T>,
        write_half: WriteHalf<T>,
        event_handler: Option<mpsc::Sender<Frame>>,
        error_handler: Option<mpsc::Sender<Error>>,
        keyspace_holder: Arc<KeyspaceHolder>,
    ) -> Self {
        let (write_sender, write_receiver) = mpsc::channel(buffer_size);
        let is_broken = Arc::new(AtomicBool::new(false));

        let processing_handle = tokio::spawn(Self::start_processing(
            write_receiver,
            event_handler,
            error_handler,
            read_half,
            write_half,
            keyspace_holder,
            is_broken.clone(),
            compression,
        ));

        AsyncTransport {
            addr,
            compression,
            write_sender,
            is_broken,
            processing_handle,
        }
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.is_broken.load(Ordering::Relaxed)
    }

    #[inline]
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn write_frame(&self, frame: &Frame) -> Result<Frame> {
        let (sender, receiver) = oneshot::channel();
        let stream_id = frame.stream;

        // startup message is never compressed
        let data = if frame.opcode != Opcode::Startup {
            frame.encode_with(self.compression)?
        } else {
            frame.encode_with(Compression::None)?
        };

        self.write_sender
            .send(Request::new(data, stream_id, sender))
            .await
            .map_err(|_| Error::General("Connection closed when writing data!".into()))?;

        receiver
            .await
            .map_err(|_| Error::General("Connection closed while waiting for response!".into()))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_processing<T: AsyncRead + AsyncWrite>(
        write_receiver: mpsc::Receiver<Request>,
        event_handler: Option<mpsc::Sender<Frame>>,
        error_handler: Option<mpsc::Sender<Error>>,
        read_half: ReadHalf<T>,
        write_half: WriteHalf<T>,
        keyspace_holder: Arc<KeyspaceHolder>,
        is_broken: Arc<AtomicBool>,
        compression: Compression,
    ) {
        let response_handler_map = ResponseHandlerMap::new();

        let writer = Self::start_writing(
            write_receiver,
            BufWriter::new(write_half),
            &response_handler_map,
        );

        let reader = Self::start_reading(
            read_half,
            event_handler,
            compression,
            keyspace_holder,
            &response_handler_map,
        );

        let result = tokio::try_join!(writer, reader);
        if let Err(error) = result {
            error!(%error, "Transport error!");

            is_broken.store(true, Ordering::Relaxed);
            response_handler_map.signal_general_error(&error.to_string());

            if let Some(error_handler) = error_handler {
                let _ = error_handler.send(error).await;
            }
        }
    }

    async fn start_reading<T: AsyncRead>(
        mut read_half: ReadHalf<T>,
        event_handler: Option<mpsc::Sender<Frame>>,
        compression: Compression,
        keyspace_holder: Arc<KeyspaceHolder>,
        response_handler_map: &ResponseHandlerMap,
    ) -> Result<()> {
        loop {
            let frame = parse_frame(&mut read_half, compression).await;
            match frame {
                Ok(frame) => {
                    if frame.stream >= 0 {
                        // in case we get a SetKeyspace result, we need to store current keyspace
                        // checks are done manually for speed
                        if frame.opcode == Opcode::Result {
                            let result_kind = ResultKind::from_bytes(&frame.body[..INT_LEN])?;
                            if result_kind == ResultKind::SetKeyspace {
                                let response_body = frame.response_body()?;
                                let set_keyspace =
                                    response_body.into_set_keyspace().ok_or_else(|| {
                                        Error::General(
                                            "SetKeyspace not found with SetKeyspace opcode!".into(),
                                        )
                                    })?;

                                keyspace_holder.update_current_keyspace(set_keyspace.body);
                            }
                        }

                        // normal response to query
                        response_handler_map.send_response(frame.stream, Ok(frame))?;
                    } else if frame.stream == EVENT_STREAM_ID {
                        // server event
                        if let Some(event_handler) = &event_handler {
                            let _ = event_handler.send(frame).await;
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn start_writing(
        mut write_receiver: mpsc::Receiver<Request>,
        mut write_half: impl AsyncWrite + Unpin,
        response_handler_map: &ResponseHandlerMap,
    ) -> Result<()> {
        while let Some(mut request) = write_receiver.recv().await {
            loop {
                response_handler_map.add_handler(request.stream_id, request.handler);

                if let Err(error) = write_half.write_all(&request.data).await {
                    response_handler_map.send_response(request.stream_id, Err(error.into()))?;
                    return Err(Error::General("Write channel failure!".into()));
                }

                request = match write_receiver.try_recv() {
                    Ok(request) => request,
                    Err(_) => break,
                }
            }

            if let Err(error) = write_half.flush().await {
                response_handler_map.send_response(request.stream_id, Err(error.into()))?;
                return Err(Error::General("Write channel failure!".into()));
            }
        }

        Ok(())
    }
}

type ResponseHandler = oneshot::Sender<Result<Frame>>;

#[derive(Default)]
struct ResponseHandlerMap {
    stream_handlers: Mutex<FxHashMap<StreamId, ResponseHandler>>,
}

impl ResponseHandlerMap {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn add_handler(&self, stream_id: StreamId, handler: ResponseHandler) {
        self.stream_handlers
            .lock()
            .unwrap()
            .insert(stream_id, handler);
    }

    pub fn send_response(&self, stream_id: StreamId, response: Result<Frame>) -> Result<()> {
        match self.stream_handlers.lock().unwrap().remove(&stream_id) {
            Some(handler) => {
                let _ = handler.send(response);
                Ok(())
            }
            // unmatched stream - probably a bug somewhere
            None => Err(Error::General(format!(
                "Unmatched stream id: {}",
                stream_id
            ))),
        }
    }

    pub fn signal_general_error(&self, error: &str) {
        for (_, handler) in self.stream_handlers.lock().unwrap().drain() {
            let _ = handler.send(Err(Error::General(error.to_string())));
        }
    }
}

#[derive(Constructor)]
struct Request {
    data: Vec<u8>,
    stream_id: StreamId,
    handler: ResponseHandler,
}
