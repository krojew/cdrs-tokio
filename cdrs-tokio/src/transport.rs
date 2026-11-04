//!This module contains a declaration of `CdrsTransport` trait which should be implemented
//!for particular transport to be able using it as transport of CDRS client.
//!
//!Currently, CDRS provides concrete transport which implements `CdrsTransport` trait.
//!There are:
//!
//! * [`TransportTcp`] is default TCP transport which is usually used to establish
//!connection and exchange frames.
//!
//! * [`TransportRustls`] is a transport which is used to establish SSL encrypted connection
//!with Apache Cassandra server. **Note:** this option is available if and only if CDRS is imported
//!with the `rust-tls` feature.
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::frame_decoder::FrameDecoder;
use cassandra_protocol::frame::frame_encoder::FrameEncoder;
use cassandra_protocol::frame::message_result::ResultKind;
use cassandra_protocol::frame::{Envelope, Flags, StreamId, MAX_FRAME_SIZE};
use cassandra_protocol::frame::{FromBytes, Opcode, EVENT_STREAM_ID};
use cassandra_protocol::types::INT_LEN;
use futures::FutureExt;
use fxhash::FxHashMap;
use itertools::Itertools;
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{
    split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, ReadHalf,
    WriteHalf,
};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;
#[cfg(feature = "rust-tls")]
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig};
#[cfg(feature = "rust-tls")]
use tokio_rustls::TlsConnector as RustlsConnector;
use tracing::*;

#[cfg(test)]
use mockall::*;

use crate::cluster::KeyspaceHolder;
use crate::envelope_parser::{convert_envelope_into_result, parse_envelope};
use crate::future::BoxFuture;
use crate::Error;
use crate::Result;

const INITIAL_STREAM_ID: i16 = 1;

/// General CDRS transport trait.
pub trait CdrsTransport: Send + Sync {
    /// Schedules data envelope for writing and waits for a response. Handshake envelopes need to
    /// be marked as such, since their wire representation is different.
    fn write_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>>;

    /// Schedules an envelope issued internally by the driver, e.g. to propagate the current
    /// keyspace to a connection, for writing and waits for a response. Unlike
    /// [`write_envelope`](Self::write_envelope), the response must not change shared session
    /// state, such as the current keyspace. The default implementation delegates to
    /// `write_envelope`.
    fn write_internal_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.write_envelope(envelope, handshake)
    }

    /// Checks if the connection is broken (e.g. after read or write errors).
    fn is_broken(&self) -> bool;

    /// Marks the connection as broken, e.g. when it stops answering heartbeats, and fails all
    /// requests waiting for a response on it. Afterwards, [`is_broken`](Self::is_broken) should
    /// return `true`. The caller handles the failure itself, so the error handler of the
    /// connection does not need to be notified. The default implementation does nothing.
    fn mark_broken(&self) {}

    /// Returns associated node address.
    fn address(&self) -> SocketAddr;
}

#[cfg(test)]
mock! {
    pub CdrsTransport {
    }

    impl CdrsTransport for CdrsTransport {
        fn write_envelope(
            &self,
            envelope: &Envelope,
            handshake: bool,
        ) -> BoxFuture<'static, Result<Envelope>>;

        fn write_internal_envelope(
            &self,
            envelope: &Envelope,
            handshake: bool,
        ) -> BoxFuture<'static, Result<Envelope>>;

        fn is_broken(&self) -> bool;

        fn mark_broken(&self);

        fn address(&self) -> SocketAddr;
    }
}

async fn connect_tcp(addr: SocketAddr, tcp_nodelay: bool) -> io::Result<TcpStream> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    // lets the OS detect dead peers on mostly idle connections, such as the control connection
    socket.set_keepalive(true)?;

    let stream = socket.connect(addr).await?;
    stream.set_nodelay(tcp_nodelay)?;
    Ok(stream)
}

/// Connects like [`connect_tcp`] to the first reachable address `host` resolves to.
#[cfg(feature = "http-proxy")]
pub(crate) async fn connect_tcp_host(host: &str, tcp_nodelay: bool) -> io::Result<TcpStream> {
    let mut last_error = None;
    for addr in tokio::net::lookup_host(host).await? {
        match connect_tcp(addr, tcp_nodelay).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Address {host} does not resolve to any address!"),
        )
    }))
}

/// Default Tcp transport.
#[derive(Debug)]
pub struct TransportTcp {
    inner: AsyncTransport,
}

impl TransportTcp {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        addr: SocketAddr,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
        buffer_size: usize,
        tcp_nodelay: bool,
    ) -> io::Result<TransportTcp> {
        connect_tcp(addr, tcp_nodelay)
            .await
            .and_then(move |socket| {
                Self::with_stream(
                    socket,
                    addr,
                    keyspace_holder,
                    event_handler,
                    error_handler,
                    compression,
                    frame_encoder,
                    frame_decoder,
                    buffer_size,
                )
            })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_stream<T: AsyncRead + AsyncWrite + Send + Sync + 'static>(
        stream: T,
        addr: SocketAddr,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
        buffer_size: usize,
    ) -> io::Result<TransportTcp> {
        let (read_half, write_half) = split(stream);
        Ok(TransportTcp {
            inner: AsyncTransport::new(
                addr,
                compression,
                frame_encoder,
                frame_decoder,
                buffer_size,
                read_half,
                write_half,
                event_handler,
                error_handler,
                keyspace_holder,
            ),
        })
    }

    /// Marks every envelope sent over this transport with the beta protocol flag. Cassandra
    /// rejects frames without it when a beta protocol version is in use.
    #[must_use]
    pub fn with_beta_protocol(mut self, beta_protocol: bool) -> Self {
        self.inner.beta_protocol = beta_protocol;
        self
    }
}

impl CdrsTransport for TransportTcp {
    //noinspection DuplicatedCode
    #[inline]
    fn write_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.inner
            .write_envelope(envelope, handshake, false)
            .boxed()
    }

    #[inline]
    fn write_internal_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.inner.write_envelope(envelope, handshake, true).boxed()
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.inner.is_broken()
    }

    #[inline]
    fn mark_broken(&self) {
        self.inner.mark_broken()
    }

    #[inline]
    fn address(&self) -> SocketAddr {
        self.inner.addr()
    }
}

#[cfg(feature = "rust-tls")]
#[derive(Debug)]
pub struct TransportRustls {
    inner: AsyncTransport,
}

#[cfg(feature = "rust-tls")]
impl TransportRustls {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        addr: SocketAddr,
        dns_name: ServerName<'static>,
        config: Arc<ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
        buffer_size: usize,
        tcp_nodelay: bool,
    ) -> io::Result<Self> {
        let stream = connect_tcp(addr, tcp_nodelay).await?;

        Self::with_stream(
            stream,
            addr,
            dns_name,
            config,
            keyspace_holder,
            event_handler,
            error_handler,
            compression,
            frame_encoder,
            frame_decoder,
            buffer_size,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn with_stream<T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static>(
        stream: T,
        addr: SocketAddr,
        dns_name: ServerName<'static>,
        config: Arc<ClientConfig>,
        keyspace_holder: Arc<KeyspaceHolder>,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        compression: Compression,
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
        buffer_size: usize,
    ) -> io::Result<Self> {
        let connector = RustlsConnector::from(config.clone());
        let stream = connector.connect(dns_name, stream).await?;
        let (read_half, write_half) = split(stream);

        Ok(Self {
            inner: AsyncTransport::new(
                addr,
                compression,
                frame_encoder,
                frame_decoder,
                buffer_size,
                read_half,
                write_half,
                event_handler,
                error_handler,
                keyspace_holder,
            ),
        })
    }

    /// Marks every envelope sent over this transport with the beta protocol flag. Cassandra
    /// rejects frames without it when a beta protocol version is in use.
    #[must_use]
    pub fn with_beta_protocol(mut self, beta_protocol: bool) -> Self {
        self.inner.beta_protocol = beta_protocol;
        self
    }
}

#[cfg(feature = "rust-tls")]
impl CdrsTransport for TransportRustls {
    //noinspection DuplicatedCode
    #[inline]
    fn write_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.inner
            .write_envelope(envelope, handshake, false)
            .boxed()
    }

    #[inline]
    fn write_internal_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.inner.write_envelope(envelope, handshake, true).boxed()
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.inner.is_broken()
    }

    #[inline]
    fn mark_broken(&self) {
        self.inner.mark_broken()
    }

    #[inline]
    fn address(&self) -> SocketAddr {
        self.inner.addr()
    }
}

#[derive(Debug)]
struct AsyncTransport {
    addr: SocketAddr,
    compression: Compression,
    beta_protocol: bool,
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
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
        buffer_size: usize,
        read_half: ReadHalf<T>,
        write_half: WriteHalf<T>,
        event_handler: Option<mpsc::Sender<Envelope>>,
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
            addr,
            frame_encoder,
            frame_decoder,
        ));

        AsyncTransport {
            addr,
            compression,
            beta_protocol: false,
            write_sender,
            is_broken,
            processing_handle,
        }
    }

    #[inline]
    fn is_broken(&self) -> bool {
        self.is_broken.load(Ordering::Relaxed)
    }

    fn mark_broken(&self) {
        self.is_broken.store(true, Ordering::Relaxed);

        // Dropping the processing task fails all requests in flight or still queued for writing
        // by dropping their response handlers, and closes the connection.
        self.processing_handle.abort();
    }

    #[inline]
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn write_envelope(
        &self,
        envelope: &Envelope,
        handshake: bool,
        internal: bool,
    ) -> Result<Envelope> {
        let (sender, receiver) = oneshot::channel();

        // leave stream id empty for now and generate it later

        // handshake messages are never compressed
        let mut data = if handshake {
            envelope.encode_with(Compression::None)?
        } else {
            envelope.encode_with(self.compression)?
        };

        if self.beta_protocol {
            // the second byte of an encoded envelope holds its flags
            data[1] |= Flags::BETA.bits();
        }

        self.write_sender
            .send(Request {
                data,
                handler: sender,
                handshake,
                internal,
            })
            .await
            .map_err(|_| Error::General("Connection closed when writing data!".into()))?;

        receiver
            .await
            .map_err(|_| Error::General("Connection closed while waiting for response!".into()))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_processing<T: AsyncRead + AsyncWrite>(
        write_receiver: mpsc::Receiver<Request>,
        event_handler: Option<mpsc::Sender<Envelope>>,
        error_handler: Option<mpsc::Sender<Error>>,
        read_half: ReadHalf<T>,
        write_half: WriteHalf<T>,
        keyspace_holder: Arc<KeyspaceHolder>,
        is_broken: Arc<AtomicBool>,
        compression: Compression,
        addr: SocketAddr,
        frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
    ) {
        let response_handler_map = ResponseHandlerMap::new();
        let mut failure_handler = ProcessingFailureHandler {
            is_broken,
            error_handler,
        };

        let writer = Self::start_writing(
            write_receiver,
            BufWriter::new(write_half),
            &response_handler_map,
            frame_encoder,
        );

        let reader = Self::start_reading_handshake_frames(
            BufReader::with_capacity(MAX_FRAME_SIZE, read_half),
            event_handler,
            compression,
            addr,
            keyspace_holder,
            &response_handler_map,
            frame_decoder,
        );

        let result = tokio::try_join!(writer, reader);
        if let Err(error) = result {
            error!(%error, "Transport error!");

            failure_handler.fail(error.clone());
            response_handler_map.signal_general_error(&error);
        }
    }

    async fn start_reading_handshake_frames(
        mut read_half: impl AsyncRead + Unpin,
        event_handler: Option<mpsc::Sender<Envelope>>,
        compression: Compression,
        addr: SocketAddr,
        keyspace_holder: Arc<KeyspaceHolder>,
        response_handler_map: &ResponseHandlerMap,
        frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
    ) -> Result<()> {
        // before Authenticate or Ready, envelopes are unframed
        loop {
            let result = parse_envelope(&mut read_half, compression, addr).await;
            match result {
                Ok(envelope) => {
                    if envelope.stream_id >= 0 {
                        let opcode = envelope.opcode;
                        response_handler_map.send_response(envelope.stream_id, Ok(envelope))?;

                        if opcode == Opcode::Authenticate || opcode == Opcode::Ready {
                            // all frames should now be encoded
                            return Self::start_reading_normal_frames(
                                read_half,
                                event_handler,
                                compression,
                                addr,
                                keyspace_holder,
                                response_handler_map,
                                frame_decoder,
                            )
                            .await;
                        }
                    } else if envelope.stream_id == EVENT_STREAM_ID {
                        // server event
                        if let Some(event_handler) = &event_handler {
                            let _ = event_handler.send(envelope).await;
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn start_reading_normal_frames(
        mut read_half: impl AsyncRead + Unpin,
        event_handler: Option<mpsc::Sender<Envelope>>,
        compression: Compression,
        addr: SocketAddr,
        keyspace_holder: Arc<KeyspaceHolder>,
        response_handler_map: &ResponseHandlerMap,
        mut frame_decoder: Box<dyn FrameDecoder + Send + Sync>,
    ) -> Result<()> {
        let mut buffer = Vec::with_capacity(MAX_FRAME_SIZE);
        loop {
            let num_read = read_half.read_buf(&mut buffer).await?;
            if num_read == 0 {
                break Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF",
                )));
            }

            let envelopes = frame_decoder.consume(&mut buffer, compression)?;
            for envelope in envelopes {
                if envelope.stream_id >= 0 {
                    // in case we get a SetKeyspace result, we need to store current keyspace
                    // checks are done manually for speed
                    // internal requests only follow the current keyspace, so a late response
                    // to one must not override a newer keyspace set by the user
                    if envelope.opcode == Opcode::Result {
                        let result_kind = envelope
                            .message_body()?
                            .get(..INT_LEN)
                            .ok_or_else(|| {
                                Error::General("Result body without result kind!".into())
                            })
                            .and_then(ResultKind::from_bytes)?;
                        if result_kind == ResultKind::SetKeyspace
                            && !response_handler_map.is_internal(envelope.stream_id)
                        {
                            let response_body = envelope.response_body()?;
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
                    response_handler_map.send_response(
                        envelope.stream_id,
                        convert_envelope_into_result(envelope, addr),
                    )?;
                } else if envelope.stream_id == EVENT_STREAM_ID {
                    // server event
                    if let Some(event_handler) = &event_handler {
                        let _ = event_handler.send(envelope).await;
                    }
                }
            }
        }
    }

    async fn start_writing(
        mut write_receiver: mpsc::Receiver<Request>,
        mut write_half: impl AsyncWrite + Unpin,
        response_handler_map: &ResponseHandlerMap,
        mut frame_encoder: Box<dyn FrameEncoder + Send + Sync>,
    ) -> Result<()> {
        let mut frame_stream_ids = Vec::with_capacity(1);

        while let Some(mut request) = write_receiver.recv().await {
            frame_stream_ids.clear();

            loop {
                let stream_id = match response_handler_map.try_next_stream_id() {
                    Some(stream_id) => stream_id,
                    None => {
                        // all stream ids are in flight - send everything pending, so that
                        // responses can free some
                        Self::flush_pending(
                            &mut write_half,
                            response_handler_map,
                            &mut frame_stream_ids,
                            frame_encoder.as_mut(),
                        )
                        .await?;

                        response_handler_map.next_stream_id().await?
                    }
                };
                frame_stream_ids.push(stream_id);

                request.set_stream_id(stream_id);
                response_handler_map.add_handler(stream_id, request.handler, request.internal);

                if request.handshake {
                    // handshake messages are not framed, so let's write them directly
                    if let Err(error) = write_half.write_all(&request.data).await {
                        response_handler_map.send_response(stream_id, Err(error.into()))?;
                        return Err(Error::General("Write channel failure!".into()));
                    }
                } else {
                    // post-handshake messages can be aggregated in frames by the encoder
                    loop {
                        if frame_encoder.can_fit(request.data.len()) {
                            frame_encoder.add_envelope(request.data);
                            break;
                        }

                        // flush the previous frame or create a non-self-contained one
                        if frame_encoder.has_envelopes() {
                            // we have some envelopes => flush current frame
                            Self::write_self_contained_frame(
                                &mut write_half,
                                response_handler_map,
                                &mut frame_stream_ids,
                                frame_encoder.as_mut(),
                            )
                            .await?;
                        } else {
                            // non-self-contained
                            let data_len = request.data.len();
                            let mut data_start = 0;

                            while data_start < data_len {
                                let (data_start_offset, frame) = frame_encoder
                                    .finalize_non_self_contained(&request.data[data_start..]);

                                data_start += data_start_offset;

                                Self::write_frame(
                                    &mut write_half,
                                    response_handler_map,
                                    &mut frame_stream_ids,
                                    frame,
                                )
                                .await?;

                                frame_encoder.reset();
                            }

                            break;
                        }
                    }
                }

                request = match write_receiver.try_recv() {
                    Ok(request) => request,
                    Err(_) => {
                        Self::flush_pending(
                            &mut write_half,
                            response_handler_map,
                            &mut frame_stream_ids,
                            frame_encoder.as_mut(),
                        )
                        .await?;

                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush_pending(
        write_half: &mut (impl AsyncWrite + Unpin),
        response_handler_map: &ResponseHandlerMap,
        frame_stream_ids: &mut Vec<StreamId>,
        frame_encoder: &mut (dyn FrameEncoder + Send + Sync),
    ) -> Result<()> {
        if frame_encoder.has_envelopes() {
            Self::write_self_contained_frame(
                write_half,
                response_handler_map,
                frame_stream_ids,
                frame_encoder,
            )
            .await?;
        }

        if let Err(error) = write_half.flush().await {
            Self::notify_error_handlers(response_handler_map, frame_stream_ids, error.into())?;
            return Err(Error::General("Write channel failure!".into()));
        }

        Ok(())
    }

    async fn write_self_contained_frame(
        write_half: &mut (impl AsyncWrite + Unpin),
        response_handler_map: &ResponseHandlerMap,
        frame_stream_ids: &mut Vec<StreamId>,
        frame_encoder: &mut (dyn FrameEncoder + Send + Sync),
    ) -> Result<()> {
        Self::write_frame(
            write_half,
            response_handler_map,
            frame_stream_ids,
            frame_encoder.finalize_self_contained(),
        )
        .await?;

        frame_encoder.reset();

        // Drop the stream ids that just left over the wire. Their handlers
        // are now waiting for actual server responses (or the connection
        // tearing down), and must NOT be notified of write failures from a
        // *subsequent* frame in the same batch. Without this clear we'd
        // accumulate stream ids across every flushed frame and a single late
        // write error would falsely fail every previously-sent envelope.
        frame_stream_ids.clear();
        Ok(())
    }

    async fn write_frame(
        write_half: &mut (impl AsyncWrite + Unpin),
        response_handler_map: &ResponseHandlerMap,
        frame_stream_ids: &mut Vec<StreamId>,
        frame: &[u8],
    ) -> Result<()> {
        // If the underlying socket write fails, fan the error out to every
        // handler whose envelope is in this frame, and then propagate the
        // failure so the writer task can shut down cleanly. The previous
        // implementation returned Ok(()) here, swallowing the original write
        // error, so the writer would happily keep looping on a dead socket.
        if let Err(error) = write_half.write_all(frame).await {
            let propagated: Error = error.into();
            Self::notify_error_handlers(
                response_handler_map,
                frame_stream_ids,
                propagated.clone(),
            )?;
            return Err(propagated);
        }

        Ok(())
    }

    fn notify_error_handlers(
        response_handler_map: &ResponseHandlerMap,
        frame_stream_ids: &mut Vec<StreamId>,
        error: Error,
    ) -> Result<()> {
        frame_stream_ids
            .drain(..)
            .map(|stream_id| response_handler_map.send_response(stream_id, Err(error.clone())))
            .try_collect()
    }
}

/// Marks the transport as broken and notifies the error handler once processing fails, including
/// when the processing task panics.
struct ProcessingFailureHandler {
    is_broken: Arc<AtomicBool>,
    error_handler: Option<mpsc::Sender<Error>>,
}

impl ProcessingFailureHandler {
    fn fail(&mut self, error: Error) {
        self.is_broken.store(true, Ordering::Relaxed);

        if let Some(error_handler) = self.error_handler.take() {
            match error_handler.try_send(error) {
                Ok(_) => debug!("Error handler notified!"),
                Err(e) => warn!("Error handler failed to notify: {e}"),
            }
        }
    }
}

impl Drop for ProcessingFailureHandler {
    fn drop(&mut self) {
        // A panicking task is dropped while unwinding. An aborted task is dropped normally and
        // must not report an error, since that only happens when its transport is dropped.
        if std::thread::panicking() {
            self.fail(Error::General("Transport processing task panicked!".into()));
        }
    }
}

type ResponseHandler = oneshot::Sender<Result<Envelope>>;

struct PendingResponse {
    handler: ResponseHandler,
    internal: bool,
}

struct ResponseHandlerMap {
    stream_handlers: Mutex<FxHashMap<StreamId, PendingResponse>>,
    // Responses are matched with requests by stream id, so an id can be handed out again only
    // after a response (or an error) has been sent to the handler of its previous request.
    free_stream_ids: Mutex<VecDeque<StreamId>>,
    // one permit per free stream id
    free_stream_id_permits: Semaphore,
}

impl ResponseHandlerMap {
    #[inline]
    pub fn new() -> Self {
        let free_stream_ids: VecDeque<_> = (INITIAL_STREAM_ID..=StreamId::MAX).collect();

        ResponseHandlerMap {
            stream_handlers: Default::default(),
            free_stream_id_permits: Semaphore::new(free_stream_ids.len()),
            free_stream_ids: Mutex::new(free_stream_ids),
        }
    }

    #[inline]
    pub fn add_handler(&self, stream_id: StreamId, handler: ResponseHandler, internal: bool) {
        self.stream_handlers
            .lock()
            .unwrap()
            .insert(stream_id, PendingResponse { handler, internal });
    }

    /// Checks if the request in flight with given stream id was issued internally by the driver.
    pub fn is_internal(&self, stream_id: StreamId) -> bool {
        self.stream_handlers
            .lock()
            .unwrap()
            .get(&stream_id)
            .is_some_and(|pending| pending.internal)
    }

    pub fn send_response(&self, stream_id: StreamId, response: Result<Envelope>) -> Result<()> {
        let pending = self.stream_handlers.lock().unwrap().remove(&stream_id);
        match pending {
            Some(pending) => {
                self.release_stream_id(stream_id);
                let _ = pending.handler.send(response);
                Ok(())
            }
            // unmatched stream - probably a bug somewhere
            None => Err(Error::General(format!("Unmatched stream id: {stream_id}"))),
        }
    }

    pub fn signal_general_error(&self, error: &Error) {
        let pending: Vec<_> = self.stream_handlers.lock().unwrap().drain().collect();
        for (stream_id, pending) in pending {
            self.release_stream_id(stream_id);
            let _ = pending.handler.send(Err(error.clone()));
        }
    }

    /// Takes a free stream id, if one is available.
    pub fn try_next_stream_id(&self) -> Option<StreamId> {
        self.free_stream_id_permits.try_acquire().ok()?.forget();
        self.take_free_stream_id()
    }

    /// Takes a free stream id, waiting for one to be released if all are in flight.
    pub async fn next_stream_id(&self) -> Result<StreamId> {
        self.free_stream_id_permits
            .acquire()
            .await
            .map_err(|_| Error::General("Stream ids are no longer available!".into()))?
            .forget();

        self.take_free_stream_id()
            .ok_or_else(|| Error::General("No free stream id despite a permit!".into()))
    }

    fn take_free_stream_id(&self) -> Option<StreamId> {
        self.free_stream_ids.lock().unwrap().pop_front()
    }

    fn release_stream_id(&self, stream_id: StreamId) {
        self.free_stream_ids.lock().unwrap().push_back(stream_id);
        self.free_stream_id_permits.add_permits(1);
    }
}

struct Request {
    data: Vec<u8>,
    handler: ResponseHandler,
    handshake: bool,
    internal: bool,
}

impl Request {
    #[inline]
    fn set_stream_id(&mut self, stream_d: StreamId) {
        self.data[2..4].copy_from_slice(&stream_d.to_be_bytes());
    }
}

#[cfg(test)]
mod write_buffer_tests {
    use super::*;
    use cassandra_protocol::frame::frame_encoder::UncompressedFrameEncoder;
    use tokio::io::sink;

    // Verifies that once a frame has been successfully written, the stream
    // ids that were aggregated into that frame are dropped from the
    // bookkeeping vector. Otherwise a later write failure would notify those
    // handlers - whose envelopes have already been sent and may yet receive
    // a real server response - with a spurious "Write channel failure".
    #[tokio::test]
    async fn write_self_contained_frame_clears_stream_ids_on_success() {
        let map = ResponseHandlerMap::new();
        let mut sink = sink();
        let mut encoder: Box<dyn FrameEncoder + Send + Sync> =
            Box::new(UncompressedFrameEncoder::default());

        // Pretend two envelopes' worth of work has been added to the encoder.
        // We don't actually need to register handlers on the map for this
        // test - we're checking that the frame_stream_ids buffer is emptied
        // once the frame goes out, regardless of map state.
        encoder.add_envelope(vec![0; 16]);
        let mut frame_stream_ids: Vec<StreamId> = vec![1, 2];

        AsyncTransport::write_self_contained_frame(
            &mut sink,
            &map,
            &mut frame_stream_ids,
            encoder.as_mut(),
        )
        .await
        .expect("sink writes always succeed");

        // After a successful frame write the vector must be empty so the
        // *next* frame's failure can't accidentally notify handlers from
        // already-sent frames.
        assert!(
            frame_stream_ids.is_empty(),
            "frame_stream_ids should be empty after successful write, was {:?}",
            frame_stream_ids
        );
    }
}

#[cfg(test)]
mod stream_id_tests {
    use super::*;

    #[test]
    fn next_stream_id_starts_at_initial_value() {
        let map = ResponseHandlerMap::new();
        assert_eq!(map.try_next_stream_id(), Some(INITIAL_STREAM_ID));
        assert_eq!(map.try_next_stream_id(), Some(INITIAL_STREAM_ID + 1));
    }

    #[tokio::test]
    async fn stream_ids_are_handed_out_again_only_after_release() {
        let map = ResponseHandlerMap::new();

        // every positive id is handed out exactly once
        let stream_ids: Vec<_> = std::iter::from_fn(|| map.try_next_stream_id()).collect();
        assert_eq!(
            stream_ids,
            (INITIAL_STREAM_ID..=StreamId::MAX).collect::<Vec<_>>()
        );

        let (handler, _receiver) = oneshot::channel();
        map.add_handler(5, handler, false);

        let next_stream_id = map.next_stream_id();
        tokio::pin!(next_stream_id);
        assert!(next_stream_id.as_mut().now_or_never().is_none());

        map.send_response(5, Err(Error::General("test".into())))
            .unwrap();

        assert_eq!(next_stream_id.await.unwrap(), 5);
        assert_eq!(map.try_next_stream_id(), None);
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use cassandra_protocol::frame::frame_decoder::LegacyFrameDecoder;
    use cassandra_protocol::frame::frame_encoder::LegacyFrameEncoder;
    use cassandra_protocol::frame::{Direction, Flags, ParseEnvelopeError, Version};
    use std::time::Duration;
    use tokio::io::{duplex, DuplexStream};
    use tokio::sync::watch;
    use tokio::time::timeout;

    // upper bound for awaiting outcomes which are expected to happen right away
    const TEST_GUARD_TIMEOUT: Duration = Duration::from_secs(5);

    struct TestConnection {
        transport: TransportTcp,
        server: FakeServer,
        keyspace_holder: Arc<KeyspaceHolder>,
        error_receiver: mpsc::Receiver<Error>,
    }

    fn connect() -> TestConnection {
        let (client_stream, server_stream) = duplex(1 << 20);
        let (keyspace_sender, _) = watch::channel(None);
        let keyspace_holder = Arc::new(KeyspaceHolder::new(keyspace_sender));
        let (error_sender, error_receiver) = mpsc::channel(1);

        let transport = TransportTcp::with_stream(
            client_stream,
            "127.0.0.1:9042".parse().unwrap(),
            keyspace_holder.clone(),
            None,
            Some(error_sender),
            Compression::None,
            Box::<LegacyFrameEncoder>::default(),
            Box::<LegacyFrameDecoder>::default(),
            64,
        )
        .unwrap();

        TestConnection {
            transport,
            server: FakeServer {
                stream: server_stream,
                buffer: vec![],
            },
            keyspace_holder,
            error_receiver,
        }
    }

    struct FakeServer {
        stream: DuplexStream,
        buffer: Vec<u8>,
    }

    impl FakeServer {
        async fn read_request(&mut self) -> Envelope {
            loop {
                match Envelope::from_buffer(&self.buffer, Compression::None) {
                    Ok(parsed) => {
                        self.buffer.drain(..parsed.envelope_len);
                        return parsed.envelope;
                    }
                    Err(ParseEnvelopeError::NotEnoughBytes) => {
                        let read = self.stream.read_buf(&mut self.buffer).await.unwrap();
                        assert_ne!(read, 0, "client closed the connection");
                    }
                    Err(error) => panic!("invalid request: {}", error),
                }
            }
        }

        async fn respond(&mut self, stream_id: StreamId, opcode: Opcode, body: Vec<u8>) {
            let envelope = Envelope::new(
                Version::V4,
                Direction::Response,
                Flags::empty(),
                opcode,
                stream_id,
                body,
                None,
                vec![],
            );

            self.stream
                .write_all(&envelope.encode_with(Compression::None).unwrap())
                .await
                .unwrap();
        }

        async fn respond_to_next(&mut self, opcode: Opcode, body: Vec<u8>) {
            let request = self.read_request().await;
            self.respond(request.stream_id, opcode, body).await;
        }
    }

    // Completes the handshake, after which responses are processed as regular frames.
    async fn handshake(connection: &mut TestConnection) {
        let startup = Envelope::new_req_startup(None, Version::V4);
        let (response, _) = tokio::join!(
            connection.transport.write_envelope(&startup, true),
            connection.server.respond_to_next(Opcode::Ready, vec![]),
        );

        assert_eq!(response.unwrap().opcode, Opcode::Ready);
    }

    #[tokio::test]
    async fn tcp_connections_enable_keepalive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let stream = connect_tcp(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        assert!(stream.nodelay().unwrap());

        let socket = TcpSocket::from_std_stream(stream.into_std().unwrap());
        assert!(socket.keepalive().unwrap());
    }

    #[tokio::test]
    async fn malformed_result_breaks_transport_and_notifies_error_handler() {
        let mut connection = connect();
        handshake(&mut connection).await;

        let options = Envelope::new_req_options(Version::V4);
        let (response, _) = tokio::join!(
            connection.transport.write_envelope(&options, false),
            connection
                .server
                .respond_to_next(Opcode::Result, vec![0, 0]),
        );

        assert!(response.is_err());
        assert!(connection.transport.is_broken());

        let error = timeout(TEST_GUARD_TIMEOUT, connection.error_receiver.recv())
            .await
            .expect("error handler was not notified");
        assert!(
            error.is_some(),
            "error handler was dropped without a notification"
        );
    }

    fn set_keyspace_result(keyspace: &str) -> Vec<u8> {
        let mut body = vec![0, 0, 0, 3];
        body.extend_from_slice(&(keyspace.len() as i16).to_be_bytes());
        body.extend_from_slice(keyspace.as_bytes());
        body
    }

    fn use_query(keyspace: &str) -> Envelope {
        Envelope::new_req_query(
            format!("USE {keyspace}"),
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
            Version::V4,
        )
    }

    #[tokio::test]
    async fn internal_requests_do_not_update_current_keyspace() {
        let mut connection = connect();
        handshake(&mut connection).await;

        let use_b = use_query("b");
        let (response, _) = tokio::join!(
            connection.transport.write_envelope(&use_b, false),
            connection
                .server
                .respond_to_next(Opcode::Result, set_keyspace_result("b")),
        );
        response.unwrap();
        assert_eq!(
            connection.keyspace_holder.current_keyspace().as_deref(),
            Some(&"b".to_string())
        );

        // a late response to keyspace propagation issued before the user switched to "b"
        let use_a = use_query("a");
        let (response, _) = tokio::join!(
            connection.transport.write_internal_envelope(&use_a, false),
            connection
                .server
                .respond_to_next(Opcode::Result, set_keyspace_result("a")),
        );
        response.unwrap();
        assert_eq!(
            connection.keyspace_holder.current_keyspace().as_deref(),
            Some(&"b".to_string())
        );
    }

    #[tokio::test]
    async fn beta_protocol_marks_every_envelope() {
        let TestConnection {
            transport,
            mut server,
            ..
        } = connect();
        let transport = transport.with_beta_protocol(true);

        let startup = Envelope::new_req_startup(None, Version::V4);
        let (response, startup_request) =
            tokio::join!(transport.write_envelope(&startup, true), async {
                let request = server.read_request().await;
                server
                    .respond(request.stream_id, Opcode::Ready, vec![])
                    .await;
                request
            });
        response.unwrap();
        assert!(startup_request.flags.contains(Flags::BETA));

        let query = use_query("a");
        let (response, query_request) =
            tokio::join!(transport.write_envelope(&query, false), async {
                let request = server.read_request().await;
                server
                    .respond(request.stream_id, Opcode::Result, set_keyspace_result("a"))
                    .await;
                request
            });
        response.unwrap();
        assert!(query_request.flags.contains(Flags::BETA));
    }

    #[tokio::test]
    async fn set_keyspace_result_with_custom_payload_updates_current_keyspace() {
        let TestConnection {
            transport,
            mut server,
            keyspace_holder,
            ..
        } = connect();

        let startup = Envelope::new_req_startup(None, Version::V4);
        let (response, _) = tokio::join!(
            transport.write_envelope(&startup, true),
            server.respond_to_next(Opcode::Ready, vec![]),
        );
        response.unwrap();

        let use_b = use_query("b");
        let (response, _) = tokio::join!(transport.write_envelope(&use_b, false), async {
            let request = server.read_request().await;

            let mut body = vec![0, 1, 0, 1, b'k', 0, 0, 0, 1, 42]; // custom payload {"k": [42]}
            body.extend_from_slice(&set_keyspace_result("b"));
            let envelope = Envelope::new(
                Version::V4,
                Direction::Response,
                Flags::CUSTOM_PAYLOAD,
                Opcode::Result,
                request.stream_id,
                body,
                None,
                vec![],
            );

            server
                .stream
                .write_all(&envelope.encode_with(Compression::None).unwrap())
                .await
                .unwrap();
        });
        response.unwrap();

        assert_eq!(
            keyspace_holder.current_keyspace().as_deref(),
            Some(&"b".to_string())
        );
    }

    fn numbered_request(number: u32) -> Envelope {
        Envelope::new(
            Version::V4,
            Direction::Request,
            Flags::empty(),
            Opcode::Options,
            0,
            number.to_be_bytes().to_vec(),
            None,
            vec![],
        )
    }

    #[tokio::test]
    async fn stream_ids_of_requests_in_flight_are_not_reused() {
        const OTHER_REQUESTS: u32 = i16::MAX as u32 + 100;

        let mut connection = connect();
        handshake(&mut connection).await;

        let TestConnection {
            transport,
            mut server,
            ..
        } = connection;
        let transport = Arc::new(transport);

        let held_response = tokio::spawn({
            let transport = transport.clone();
            async move { transport.write_envelope(&numbered_request(0), false).await }
        });
        let held_request = server.read_request().await;

        // the server answers every other request right away and the held one last
        let server = tokio::spawn(async move {
            for _ in 0..OTHER_REQUESTS {
                let request = server.read_request().await;
                assert_ne!(
                    request.stream_id, held_request.stream_id,
                    "stream id of a request in flight was reused"
                );

                server
                    .respond(request.stream_id, Opcode::Supported, request.body)
                    .await;
            }

            server
                .respond(held_request.stream_id, Opcode::Supported, held_request.body)
                .await;

            server
        });

        let mut client_result = Ok(());
        for number in 1..=OTHER_REQUESTS {
            match transport
                .write_envelope(&numbered_request(number), false)
                .await
            {
                Ok(response) => assert_eq!(response.body, number.to_be_bytes()),
                Err(error) => {
                    client_result = Err(error);
                    break;
                }
            }
        }

        let _server = match server.await {
            Ok(server) => server,
            Err(error) => std::panic::resume_unwind(error.into_panic()),
        };
        client_result.unwrap();

        let held_response = held_response.await.unwrap().unwrap();
        assert_eq!(held_response.body, 0u32.to_be_bytes());
    }

    #[tokio::test]
    async fn mark_broken_fails_requests_in_flight() {
        let mut connection = connect();
        handshake(&mut connection).await;

        let TestConnection {
            transport,
            mut server,
            mut error_receiver,
            ..
        } = connection;
        let transport = Arc::new(transport);

        let response = tokio::spawn({
            let transport = transport.clone();
            async move { transport.write_envelope(&numbered_request(0), false).await }
        });
        server.read_request().await;

        transport.mark_broken();

        let response = timeout(TEST_GUARD_TIMEOUT, response)
            .await
            .expect("request in flight was not failed")
            .unwrap();
        assert!(response.is_err());
        assert!(transport.is_broken());

        let response = timeout(
            TEST_GUARD_TIMEOUT,
            transport.write_envelope(&numbered_request(1), false),
        )
        .await
        .expect("request on a broken connection was not failed");
        assert!(response.is_err());

        // the caller of mark_broken handles the failure
        let error = timeout(TEST_GUARD_TIMEOUT, error_receiver.recv())
            .await
            .expect("error handler was not released");
        assert!(error.is_none(), "{:?}", error);
    }

    #[tokio::test]
    async fn request_waits_for_free_stream_id_when_all_are_in_flight() {
        const STREAM_IDS: u32 = StreamId::MAX as u32;

        let mut connection = connect();
        handshake(&mut connection).await;

        let TestConnection {
            transport,
            mut server,
            ..
        } = connection;
        let transport = Arc::new(transport);

        // one request more than there are stream ids
        let responses = tokio::spawn(futures::future::join_all((0..=STREAM_IDS).map(
            move |number| {
                let transport = transport.clone();
                async move {
                    transport
                        .write_envelope(&numbered_request(number), false)
                        .await
                }
            },
        )));

        let mut requests = Vec::with_capacity(STREAM_IDS as usize + 1);
        for _ in 0..STREAM_IDS {
            requests.push(server.read_request().await);
        }

        let answered = requests.remove(0);
        server
            .respond(answered.stream_id, Opcode::Supported, answered.body)
            .await;

        let waiting = server.read_request().await;
        assert_eq!(waiting.stream_id, answered.stream_id);
        requests.push(waiting);

        for request in requests {
            server
                .respond(request.stream_id, Opcode::Supported, request.body)
                .await;
        }

        let responses = timeout(TEST_GUARD_TIMEOUT, responses)
            .await
            .expect("not all responses arrived")
            .unwrap();
        for (number, response) in responses.into_iter().enumerate() {
            assert_eq!(response.unwrap().body, (number as u32).to_be_bytes());
        }
    }

    fn create_failure_handler() -> (
        ProcessingFailureHandler,
        Arc<AtomicBool>,
        mpsc::Receiver<Error>,
    ) {
        let is_broken = Arc::new(AtomicBool::new(false));
        let (error_sender, error_receiver) = mpsc::channel(1);

        (
            ProcessingFailureHandler {
                is_broken: is_broken.clone(),
                error_handler: Some(error_sender),
            },
            is_broken,
            error_receiver,
        )
    }

    #[tokio::test]
    async fn failure_handler_reports_panicking_task() {
        let (failure_handler, is_broken, mut error_receiver) = create_failure_handler();

        let result = tokio::spawn(async move {
            let _failure_handler = failure_handler;
            tokio::task::yield_now().await;
            panic!("simulated processing panic");
        })
        .await;

        assert!(result.unwrap_err().is_panic());
        assert!(is_broken.load(Ordering::Relaxed));
        assert!(error_receiver.recv().await.is_some());
    }

    #[tokio::test]
    async fn failure_handler_ignores_aborted_task() {
        let (failure_handler, _, mut error_receiver) = create_failure_handler();

        let handle = tokio::spawn(async move {
            let _failure_handler = failure_handler;
            futures::future::pending::<()>().await;
        });
        handle.abort();

        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(error_receiver.recv().await.is_none());
    }
}
