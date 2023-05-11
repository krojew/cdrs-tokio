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
use cassandra_protocol::frame::frame_decoder::FrameDecoder;
use cassandra_protocol::frame::frame_encoder::FrameEncoder;
use cassandra_protocol::frame::message_result::ResultKind;
use cassandra_protocol::frame::{Envelope, StreamId, MAX_FRAME_SIZE};
use cassandra_protocol::frame::{FromBytes, Opcode, EVENT_STREAM_ID};
use cassandra_protocol::types::INT_LEN;
use derive_more::Constructor;
use futures::FutureExt;
use fxhash::FxHashMap;
use itertools::Itertools;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI16, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{
    split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, ReadHalf,
    WriteHalf,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
#[cfg(feature = "rust-tls")]
use tokio_rustls::rustls::{ClientConfig, ServerName};
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

    /// Checks if the connection is broken (e.g. after read or write errors).
    fn is_broken(&self) -> bool;

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

        fn is_broken(&self) -> bool;

        fn address(&self) -> SocketAddr;
    }
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
        TcpStream::connect(addr).await.and_then(move |socket| {
            socket.set_nodelay(tcp_nodelay)?;
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
}

impl CdrsTransport for TransportTcp {
    //noinspection DuplicatedCode
    #[inline]
    fn write_envelope<'a>(
        &'a self,
        envelope: &'a Envelope,
        handshake: bool,
    ) -> BoxFuture<'a, Result<Envelope>> {
        self.inner.write_envelope(envelope, handshake).boxed()
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
#[derive(Debug)]
pub struct TransportRustls {
    inner: AsyncTransport,
}

#[cfg(feature = "rust-tls")]
impl TransportRustls {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        addr: SocketAddr,
        dns_name: ServerName,
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
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(tcp_nodelay)?;

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
        dns_name: ServerName,
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
        self.inner.write_envelope(envelope, handshake).boxed()
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

#[derive(Debug)]
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

    async fn write_envelope(&self, envelope: &Envelope, handshake: bool) -> Result<Envelope> {
        let (sender, receiver) = oneshot::channel();

        // leave stream id empty for now and generate it later

        // handshake messages are never compressed
        let data = if handshake {
            envelope.encode_with(Compression::None)?
        } else {
            envelope.encode_with(self.compression)?
        };

        self.write_sender
            .send(Request::new(data, sender, handshake))
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

            is_broken.store(true, Ordering::Relaxed);
            response_handler_map.signal_general_error(&error);

            if let Some(error_handler) = error_handler {
                let _ = error_handler.send(error).await;
            }
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
            read_half.read_buf(&mut buffer).await?;

            let envelopes = frame_decoder.consume(&mut buffer, compression)?;
            for envelope in envelopes {
                if envelope.stream_id >= 0 {
                    // in case we get a SetKeyspace result, we need to store current keyspace
                    // checks are done manually for speed
                    if envelope.opcode == Opcode::Result {
                        let result_kind = ResultKind::from_bytes(&envelope.body[..INT_LEN])?;
                        if result_kind == ResultKind::SetKeyspace {
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
                let stream_id = response_handler_map.next_stream_id();
                frame_stream_ids.push(stream_id);

                request.set_stream_id(stream_id);
                response_handler_map.add_handler(stream_id, request.handler);

                if request.handshake {
                    // handshake messages are not framed, so let's just write them directly
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

                        // flush previous frame or create a non-self-contained one
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
                        if frame_encoder.has_envelopes() {
                            Self::write_self_contained_frame(
                                &mut write_half,
                                response_handler_map,
                                &mut frame_stream_ids,
                                frame_encoder.as_mut(),
                            )
                            .await?;
                        }

                        if let Err(error) = write_half.flush().await {
                            Self::notify_error_handlers(
                                response_handler_map,
                                &mut frame_stream_ids,
                                error.into(),
                            )?;
                            return Err(Error::General("Write channel failure!".into()));
                        }

                        break;
                    }
                }
            }
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
        Ok(())
    }

    async fn write_frame(
        write_half: &mut (impl AsyncWrite + Unpin),
        response_handler_map: &ResponseHandlerMap,
        frame_stream_ids: &mut Vec<StreamId>,
        frame: &[u8],
    ) -> Result<()> {
        if let Err(error) = write_half.write_all(frame).await {
            Self::notify_error_handlers(response_handler_map, frame_stream_ids, error.into())?;
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

type ResponseHandler = oneshot::Sender<Result<Envelope>>;

struct ResponseHandlerMap {
    stream_handlers: Mutex<FxHashMap<StreamId, ResponseHandler>>,
    available_stream_id: AtomicI16,
}

impl ResponseHandlerMap {
    #[inline]
    pub fn new() -> Self {
        ResponseHandlerMap {
            stream_handlers: Default::default(),
            available_stream_id: AtomicI16::new(INITIAL_STREAM_ID),
        }
    }

    #[inline]
    pub fn add_handler(&self, stream_id: StreamId, handler: ResponseHandler) {
        self.stream_handlers
            .lock()
            .unwrap()
            .insert(stream_id, handler);
    }

    pub fn send_response(&self, stream_id: StreamId, response: Result<Envelope>) -> Result<()> {
        match self.stream_handlers.lock().unwrap().remove(&stream_id) {
            Some(handler) => {
                let _ = handler.send(response);
                Ok(())
            }
            // unmatched stream - probably a bug somewhere
            None => Err(Error::General(format!("Unmatched stream id: {stream_id}"))),
        }
    }

    pub fn signal_general_error(&self, error: &Error) {
        for (_, handler) in self.stream_handlers.lock().unwrap().drain() {
            let _ = handler.send(Err(error.clone()));
        }
    }

    pub fn next_stream_id(&self) -> StreamId {
        loop {
            let stream = self.available_stream_id.fetch_add(1, Ordering::Relaxed);
            if stream < 0 {
                match self.available_stream_id.compare_exchange(
                    stream,
                    INITIAL_STREAM_ID,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return INITIAL_STREAM_ID,
                    Err(_) => continue,
                }
            }

            return stream;
        }
    }
}

#[derive(Constructor)]
struct Request {
    data: Vec<u8>,
    handler: ResponseHandler,
    handshake: bool,
}

impl Request {
    #[inline]
    fn set_stream_id(&mut self, stream_d: StreamId) {
        self.data[2..4].copy_from_slice(&stream_d.to_be_bytes());
    }
}
