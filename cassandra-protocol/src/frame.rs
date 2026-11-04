use crate::compression::{Compression, CompressionError};
use crate::frame::message_request::RequestBody;
use crate::frame::message_response::ResponseBody;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::{
    cursor_next_value_ref, from_cursor_str, from_cursor_string_list, try_i16_from_bytes,
    try_i32_from_bytes, INT_LEN, SHORT_LEN, UUID_LEN,
};
use bitflags::bitflags;
use derivative::Derivative;
use derive_more::{Constructor, Display};
use std::convert::TryFrom;
use std::io::Cursor;
use thiserror::Error;
use uuid::Uuid;

pub use crate::frame::traits::*;

/// Number of bytes in the header
const ENVELOPE_HEADER_LEN: usize = 9;
/// Number of stream bytes in accordance to protocol.
pub const STREAM_LEN: usize = 2;
/// Number of body length bytes in accordance to protocol.
pub const LENGTH_LEN: usize = 4;

pub mod events;
pub mod frame_decoder;
pub mod frame_encoder;
pub mod message_auth_challenge;
pub mod message_auth_response;
pub mod message_auth_success;
pub mod message_authenticate;
pub mod message_batch;
pub mod message_error;
pub mod message_event;
pub mod message_execute;
pub mod message_options;
pub mod message_prepare;
pub mod message_query;
pub mod message_ready;
pub mod message_register;
pub mod message_request;
pub mod message_response;
pub mod message_result;
pub mod message_startup;
pub mod message_supported;
pub mod traits;

use crate::error;

pub const EVENT_STREAM_ID: i16 = -1;

const fn const_max(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}

/// Maximum size of frame payloads - aggregated envelopes or a part of a single envelope.
pub const PAYLOAD_SIZE_LIMIT: usize = 1 << 17;

const UNCOMPRESSED_FRAME_HEADER_LENGTH: usize = 6;
const COMPRESSED_FRAME_HEADER_LENGTH: usize = 8;
const FRAME_TRAILER_LENGTH: usize = 4;

/// Maximum size of an entire frame.
pub const MAX_FRAME_SIZE: usize = PAYLOAD_SIZE_LIMIT
    + const_max(
        UNCOMPRESSED_FRAME_HEADER_LENGTH,
        COMPRESSED_FRAME_HEADER_LENGTH,
    )
    + FRAME_TRAILER_LENGTH;

/// Maximum size of an envelope body accepted from the wire, both before and after decompression.
pub const MAX_ENVELOPE_BODY_SIZE: usize = 256 * 1024 * 1024;

/// Converts a body length read from an envelope header, rejecting negative values and values above
/// [`MAX_ENVELOPE_BODY_SIZE`].
#[inline]
pub(crate) fn checked_body_len(body_len: i32) -> Option<usize> {
    usize::try_from(body_len)
        .ok()
        .filter(|body_len| *body_len <= MAX_ENVELOPE_BODY_SIZE)
}

fn skip_bytes_map(cursor: &mut Cursor<&[u8]>) -> error::Result<()> {
    let mut buffer = [0; SHORT_LEN];
    std::io::Read::read_exact(cursor, &mut buffer)?;

    for _ in 0..u16::from_be_bytes(buffer) {
        from_cursor_str(cursor)?;

        let mut buffer = [0; INT_LEN];
        std::io::Read::read_exact(cursor, &mut buffer)?;

        // a negative length denotes a null value without any bytes
        if let Ok(len) = usize::try_from(i32::from_be_bytes(buffer)) {
            cursor_next_value_ref(cursor, len)?;
        }
    }

    Ok(())
}

/// Cassandra stream identifier.
pub type StreamId = i16;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Constructor)]
pub struct ParsedEnvelope {
    /// How many bytes from the buffer have been read.
    pub envelope_len: usize,
    /// The parsed envelope.
    pub envelope: Envelope,
}

#[derive(Derivative, Clone, PartialEq, Eq, Hash)]
#[derivative(Debug)]
pub struct Envelope {
    pub version: Version,
    pub direction: Direction,
    pub flags: Flags,
    pub opcode: Opcode,
    pub stream_id: StreamId,
    #[derivative(Debug = "ignore")]
    pub body: Vec<u8>,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

impl Envelope {
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: Version,
        direction: Direction,
        flags: Flags,
        opcode: Opcode,
        stream_id: StreamId,
        body: Vec<u8>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        Envelope {
            version,
            direction,
            flags,
            opcode,
            stream_id,
            body,
            tracing_id,
            warnings,
        }
    }

    #[inline]
    pub fn request_body(&self) -> error::Result<RequestBody> {
        RequestBody::try_from(self.body.as_slice(), self.opcode, self.version)
    }

    #[inline]
    pub fn response_body(&self) -> error::Result<ResponseBody> {
        ResponseBody::try_from(self.body.as_slice(), self.opcode, self.version)
    }

    #[inline]
    pub fn tracing_id(&self) -> &Option<Uuid> {
        &self.tracing_id
    }

    #[inline]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Parses the raw bytes of a cassandra envelope returning a [`ParsedEnvelope`] struct.
    /// The typical use case is reading from a buffer that may contain 0 or more envelopes and where
    /// the last envelope may be incomplete. The possible return values are:
    /// * `Ok(ParsedEnvelope)` - The first envelope in the buffer has been successfully parsed.
    /// * `Err(ParseEnvelopeError::NotEnoughBytes)` - There are not enough bytes to parse a single envelope, [`Envelope::from_buffer`] should be recalled when it is possible that there are more bytes.
    /// * `Err(_)` - The envelope is malformed and you should close the connection as this method does not provide a way to tell how many bytes to advance the buffer in this case.
    pub fn from_buffer(
        data: &[u8],
        compression: Compression,
    ) -> Result<ParsedEnvelope, ParseEnvelopeError> {
        if data.len() < ENVELOPE_HEADER_LEN {
            return Err(ParseEnvelopeError::NotEnoughBytes);
        }

        let raw_body_len = try_i32_from_bytes(&data[5..9]).unwrap();
        let body_len = checked_body_len(raw_body_len)
            .ok_or(ParseEnvelopeError::InvalidBodyLength(raw_body_len))?;
        let envelope_len = ENVELOPE_HEADER_LEN + body_len;
        if data.len() < envelope_len {
            return Err(ParseEnvelopeError::NotEnoughBytes);
        }

        let version = Version::try_from(data[0])
            .map_err(|_| ParseEnvelopeError::UnsupportedVersion(data[0] & 0x7f))?;
        let direction = Direction::from(data[0]);
        let mut flags = Flags::from_bits_truncate(data[1]);
        let stream_id = try_i16_from_bytes(&data[2..4]).unwrap();
        let opcode = Opcode::try_from(data[4])
            .map_err(|_| ParseEnvelopeError::UnsupportedOpcode(data[4]))?;

        let body_bytes = &data[ENVELOPE_HEADER_LEN..envelope_len];

        let full_body = if flags.contains(Flags::COMPRESSION) {
            compression.decode(body_bytes.to_vec())
        } else {
            Compression::None.decode(body_bytes.to_vec())
        }
        .map_err(ParseEnvelopeError::DecompressionError)?;

        let body_len = full_body.len();

        // Use cursor to get tracing id, warnings and actual body
        let mut body_cursor = Cursor::new(full_body.as_slice());

        let tracing_id = if flags.contains(Flags::TRACING) && direction == Direction::Response {
            let mut tracing_bytes = [0; UUID_LEN];
            std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes)
                .map_err(|error| ParseEnvelopeError::InvalidTracingId(error.into()))?;

            Some(decode_timeuuid(&tracing_bytes).map_err(ParseEnvelopeError::InvalidUuid)?)
        } else {
            None
        };

        let warnings = if flags.contains(Flags::WARNING) {
            from_cursor_string_list(&mut body_cursor)
                .map_err(ParseEnvelopeError::InvalidWarnings)?
        } else {
            vec![]
        };

        if flags.contains(Flags::CUSTOM_PAYLOAD) {
            skip_bytes_map(&mut body_cursor).map_err(ParseEnvelopeError::InvalidCustomPayload)?;
            // the payload is not kept, so the envelope must not claim to carry one
            flags.remove(Flags::CUSTOM_PAYLOAD);
        }

        let mut body = Vec::with_capacity(body_len - body_cursor.position() as usize);

        std::io::Read::read_to_end(&mut body_cursor, &mut body)
            .expect("Read cannot fail because cursor is backed by slice");

        Ok(ParsedEnvelope::new(
            envelope_len,
            Envelope {
                version,
                direction,
                flags,
                opcode,
                stream_id,
                body,
                tracing_id,
                warnings,
            },
        ))
    }

    pub fn check_envelope_size(data: &[u8]) -> Result<usize, CheckEnvelopeSizeError> {
        if data.len() < ENVELOPE_HEADER_LEN {
            return Err(CheckEnvelopeSizeError::NotEnoughBytes);
        }

        let raw_body_len = try_i32_from_bytes(&data[5..9]).unwrap();
        let body_len = checked_body_len(raw_body_len)
            .ok_or(CheckEnvelopeSizeError::InvalidBodyLength(raw_body_len))?;
        let envelope_len = ENVELOPE_HEADER_LEN + body_len;
        if data.len() < envelope_len {
            return Err(CheckEnvelopeSizeError::NotEnoughBytes);
        }
        let _ = Version::try_from(data[0])
            .map_err(|_| CheckEnvelopeSizeError::UnsupportedVersion(data[0] & 0x7f))?;

        Ok(envelope_len)
    }

    pub fn encode_with(&self, compressor: Compression) -> error::Result<Vec<u8>> {
        // compression is ignored since v5
        let is_compressed = self.version < Version::V5 && compressor.is_compressed();

        let combined_version_byte = u8::from(self.version) | u8::from(self.direction);
        let flag_byte = (if is_compressed {
            self.flags | Flags::COMPRESSION
        } else {
            self.flags.difference(Flags::COMPRESSION)
        })
        .bits();

        let opcode_byte = u8::from(self.opcode);

        let mut v = Vec::with_capacity(9);

        v.push(combined_version_byte);
        v.push(flag_byte);
        v.extend_from_slice(&self.stream_id.to_be_bytes());
        v.push(opcode_byte);

        let mut flags_buffer = vec![];

        if self.flags.contains(Flags::TRACING) && self.direction == Direction::Response {
            let mut tracing_id = self
                .tracing_id
                .ok_or_else(|| {
                    error::Error::Io(std::io::Error::other(
                        "Tracing flag was set but Envelope has no tracing_id",
                    ))
                })?
                .into_bytes()
                .to_vec();

            flags_buffer.append(&mut tracing_id);
        };

        if self.flags.contains(Flags::WARNING) && self.direction == Direction::Response {
            let warnings_len = self.warnings.len() as i16;
            flags_buffer.extend_from_slice(&warnings_len.to_be_bytes());

            for warning in &self.warnings {
                let warning_len = warning.len() as i16;
                flags_buffer.extend_from_slice(&warning_len.to_be_bytes());
                flags_buffer.append(&mut warning.as_bytes().to_vec());
            }
        }

        if is_compressed {
            // avoid having to copy the body if there is nothing in flags_buffer
            let encoded_body = if flags_buffer.is_empty() {
                compressor.encode(&self.body)?
            } else {
                flags_buffer.extend_from_slice(&self.body);
                compressor.encode(&flags_buffer)?
            };

            let body_len = encoded_body.len() as i32;
            v.extend_from_slice(&body_len.to_be_bytes());
            v.extend_from_slice(&encoded_body);
        } else {
            // avoid having to copy the body if there is nothing in flags_buffer
            if flags_buffer.is_empty() {
                let body_len = self.body.len() as i32;
                v.extend_from_slice(&body_len.to_be_bytes());
                v.extend_from_slice(&self.body);
            } else {
                let body_len = self.body.len() as i32 + flags_buffer.len() as i32;
                v.extend_from_slice(&body_len.to_be_bytes());
                flags_buffer.extend_from_slice(&self.body);
                v.append(&mut flags_buffer);
            }
        }

        Ok(v)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CheckEnvelopeSizeError {
    #[error("Not enough bytes!")]
    NotEnoughBytes,
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("Unsupported opcode: {0}")]
    UnsupportedOpcode(u8),
    /// The body length is negative or exceeds [`MAX_ENVELOPE_BODY_SIZE`].
    #[error("Invalid envelope body length: {0}")]
    InvalidBodyLength(i32),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParseEnvelopeError {
    /// There are not enough bytes to parse a single envelope, [`Envelope::from_buffer`] should be recalled when it is possible that there are more bytes.
    #[error("Not enough bytes!")]
    NotEnoughBytes,
    /// The version is not supported by cassandra-protocol, a server implementation should handle this by returning a server error with the message "Invalid or unsupported protocol version".
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("Unsupported opcode: {0}")]
    UnsupportedOpcode(u8),
    #[error("Decompression error: {0}")]
    DecompressionError(CompressionError),
    #[error("Invalid uuid: {0}")]
    InvalidUuid(uuid::Error),
    #[error("Invalid warnings: {0}")]
    InvalidWarnings(error::Error),
    /// The body length is negative or exceeds [`MAX_ENVELOPE_BODY_SIZE`].
    #[error("Invalid envelope body length: {0}")]
    InvalidBodyLength(i32),
    #[error("Invalid tracing id: {0}")]
    InvalidTracingId(error::Error),
    #[error("Invalid custom payload: {0}")]
    InvalidCustomPayload(error::Error),
}

/// Protocol version.
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash, Display)]
#[non_exhaustive]
pub enum Version {
    V3,
    V4,
    V5,
}

impl From<Version> for u8 {
    fn from(value: Version) -> Self {
        match value {
            Version::V3 => 3,
            Version::V4 => 4,
            Version::V5 => 5,
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = error::Error;

    fn try_from(version: u8) -> Result<Self, Self::Error> {
        match version & 0x7F {
            3 => Ok(Version::V3),
            4 => Ok(Version::V4),
            5 => Ok(Version::V5),
            v => Err(error::Error::General(format!(
                "Unknown cassandra version: {v}"
            ))),
        }
    }
}

impl Version {
    /// Number of bytes that represent Cassandra frame's version.
    pub const BYTE_LENGTH: usize = 1;
}

#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash, Display)]
pub enum Direction {
    Request,
    Response,
}

impl From<Direction> for u8 {
    fn from(value: Direction) -> u8 {
        match value {
            Direction::Request => 0x00,
            Direction::Response => 0x80,
        }
    }
}

impl From<u8> for Direction {
    fn from(value: u8) -> Self {
        match value & 0x80 {
            0 => Direction::Request,
            _ => Direction::Response,
        }
    }
}

bitflags! {
    /// Envelope flags
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Flags: u8 {
        const COMPRESSION = 0x01;
        const TRACING = 0x02;
        const CUSTOM_PAYLOAD = 0x04;
        const WARNING = 0x08;
        const BETA = 0x10;
    }
}

impl Default for Flags {
    #[inline]
    fn default() -> Self {
        Flags::empty()
    }
}

impl Flags {
    // Number of opcode bytes in accordance to protocol.
    pub const BYTE_LENGTH: usize = 1;
}

#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash, Display)]
#[non_exhaustive]
pub enum Opcode {
    Error,
    Startup,
    Ready,
    Authenticate,
    Options,
    Supported,
    Query,
    Result,
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge,
    AuthResponse,
    AuthSuccess,
}

impl Opcode {
    // Number of opcode bytes in accordance to protocol.
    pub const BYTE_LENGTH: usize = 1;
}

impl From<Opcode> for u8 {
    fn from(value: Opcode) -> Self {
        match value {
            Opcode::Error => 0x00,
            Opcode::Startup => 0x01,
            Opcode::Ready => 0x02,
            Opcode::Authenticate => 0x03,
            Opcode::Options => 0x05,
            Opcode::Supported => 0x06,
            Opcode::Query => 0x07,
            Opcode::Result => 0x08,
            Opcode::Prepare => 0x09,
            Opcode::Execute => 0x0A,
            Opcode::Register => 0x0B,
            Opcode::Event => 0x0C,
            Opcode::Batch => 0x0D,
            Opcode::AuthChallenge => 0x0E,
            Opcode::AuthResponse => 0x0F,
            Opcode::AuthSuccess => 0x10,
        }
    }
}

impl TryFrom<u8> for Opcode {
    type Error = error::Error;

    fn try_from(value: u8) -> Result<Self, <Opcode as TryFrom<u8>>::Error> {
        match value {
            0x00 => Ok(Opcode::Error),
            0x01 => Ok(Opcode::Startup),
            0x02 => Ok(Opcode::Ready),
            0x03 => Ok(Opcode::Authenticate),
            0x05 => Ok(Opcode::Options),
            0x06 => Ok(Opcode::Supported),
            0x07 => Ok(Opcode::Query),
            0x08 => Ok(Opcode::Result),
            0x09 => Ok(Opcode::Prepare),
            0x0A => Ok(Opcode::Execute),
            0x0B => Ok(Opcode::Register),
            0x0C => Ok(Opcode::Event),
            0x0D => Ok(Opcode::Batch),
            0x0E => Ok(Opcode::AuthChallenge),
            0x0F => Ok(Opcode::AuthResponse),
            0x10 => Ok(Opcode::AuthSuccess),
            _ => Err(error::Error::General(format!("Unknown opcode: {value}"))),
        }
    }
}

#[cfg(test)]
mod helpers {
    use super::*;

    pub fn test_encode_decode_roundtrip_response(
        raw_envelope: &[u8],
        envelope: Envelope,
        body: ResponseBody,
    ) {
        // test encode
        let encoded_body = body.serialize_to_vec(Version::V4);
        assert_eq!(
            &envelope.body, &encoded_body,
            "encoded body did not match envelope's body"
        );

        let encoded_envelope = envelope.encode_with(Compression::None).unwrap();
        assert_eq!(
            raw_envelope, &encoded_envelope,
            "encoded envelope did not match expected raw envelope"
        );

        // test decode
        let decoded_envelope = Envelope::from_buffer(raw_envelope, Compression::None)
            .unwrap()
            .envelope;
        assert_eq!(decoded_envelope, envelope);

        let decoded_body = envelope.response_body().unwrap();
        assert_eq!(
            body, decoded_body,
            "decoded envelope.body did not match body"
        )
    }

    pub fn test_encode_decode_roundtrip_request(
        raw_envelope: &[u8],
        envelope: Envelope,
        body: RequestBody,
    ) {
        // test encode
        let encoded_body = body.serialize_to_vec(Version::V4);
        assert_eq!(
            &envelope.body, &encoded_body,
            "encoded body did not match envelope's body"
        );

        let encoded_envelope = envelope.encode_with(Compression::None).unwrap();
        assert_eq!(
            raw_envelope, &encoded_envelope,
            "encoded envelope did not match expected raw envelope"
        );

        // test decode
        let decoded_envelope = Envelope::from_buffer(raw_envelope, Compression::None)
            .unwrap()
            .envelope;
        assert_eq!(envelope, decoded_envelope);

        let decoded_body = envelope.request_body().unwrap();
        assert_eq!(
            body, decoded_body,
            "decoded envelope.body did not match body"
        )
    }

    /// Use this when the body binary representation is nondeterministic but the body typed representation is deterministic
    pub fn test_encode_decode_roundtrip_nondeterministic_request(
        mut envelope: Envelope,
        body: RequestBody,
    ) {
        // test encode
        envelope.body = body.serialize_to_vec(Version::V4);

        // test decode
        let decoded_body = envelope.request_body().unwrap();
        assert_eq!(
            body, decoded_body,
            "decoded envelope.body did not match body"
        )
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::Consistency;
    use crate::frame::frame_decoder::{
        FrameDecoder, LegacyFrameDecoder, Lz4FrameDecoder, UncompressedFrameDecoder,
    };
    use crate::frame::frame_encoder::{
        FrameEncoder, LegacyFrameEncoder, Lz4FrameEncoder, UncompressedFrameEncoder,
    };
    use crate::frame::message_query::BodyReqQuery;
    use crate::query::query_params::QueryParams;
    use crate::query::query_values::QueryValues;
    use crate::types::value::Value;
    use crate::types::CBytes;

    #[test]
    fn max_frame_size_fits_largest_frame() {
        assert_eq!(const_max(6, 8), 8);
        assert_eq!(const_max(8, 6), 8);
        assert_eq!(
            MAX_FRAME_SIZE,
            PAYLOAD_SIZE_LIMIT + COMPRESSED_FRAME_HEADER_LENGTH + FRAME_TRAILER_LENGTH
        );
    }

    fn header_with_body_len(body_len: i32) -> Vec<u8> {
        let mut data = vec![
            u8::from(Version::V4) | u8::from(Direction::Response),
            0,
            0,
            0,
            u8::from(Opcode::Result),
        ];
        data.extend_from_slice(&body_len.to_be_bytes());
        data
    }

    #[test]
    fn from_buffer_rejects_invalid_body_length() {
        for body_len in [-1, -9, -10, i32::MIN, MAX_ENVELOPE_BODY_SIZE as i32 + 1] {
            let data = header_with_body_len(body_len);

            let result = Envelope::from_buffer(&data, Compression::None);
            assert!(
                matches!(result, Err(ParseEnvelopeError::InvalidBodyLength(len)) if len == body_len),
                "body length {}: {:?}",
                body_len,
                result
            );

            let result = Envelope::check_envelope_size(&data);
            assert!(
                matches!(result, Err(CheckEnvelopeSizeError::InvalidBodyLength(len)) if len == body_len),
                "body length {}: {:?}",
                body_len,
                result
            );
        }
    }

    #[test]
    fn from_buffer_accepts_body_length_up_to_limit() {
        let data = header_with_body_len(MAX_ENVELOPE_BODY_SIZE as i32);

        assert!(matches!(
            Envelope::from_buffer(&data, Compression::None),
            Err(ParseEnvelopeError::NotEnoughBytes)
        ));
        assert!(matches!(
            Envelope::check_envelope_size(&data),
            Err(CheckEnvelopeSizeError::NotEnoughBytes)
        ));
    }

    #[test]
    fn test_frame_version_as_byte() {
        assert_eq!(u8::from(Version::V3), 0x03);
        assert_eq!(u8::from(Version::V4), 0x04);
        assert_eq!(u8::from(Version::V5), 0x05);

        assert_eq!(u8::from(Direction::Request), 0x00);
        assert_eq!(u8::from(Direction::Response), 0x80);
    }

    #[test]
    fn test_frame_version_from() {
        assert_eq!(Version::try_from(0x03).unwrap(), Version::V3);
        assert_eq!(Version::try_from(0x83).unwrap(), Version::V3);
        assert_eq!(Version::try_from(0x04).unwrap(), Version::V4);
        assert_eq!(Version::try_from(0x84).unwrap(), Version::V4);
        assert_eq!(Version::try_from(0x05).unwrap(), Version::V5);
        assert_eq!(Version::try_from(0x85).unwrap(), Version::V5);

        assert_eq!(Direction::from(0x03), Direction::Request);
        assert_eq!(Direction::from(0x04), Direction::Request);
        assert_eq!(Direction::from(0x05), Direction::Request);
        assert_eq!(Direction::from(0x83), Direction::Response);
        assert_eq!(Direction::from(0x84), Direction::Response);
        assert_eq!(Direction::from(0x85), Direction::Response);
    }

    #[test]
    fn test_opcode_as_byte() {
        assert_eq!(u8::from(Opcode::Error), 0x00);
        assert_eq!(u8::from(Opcode::Startup), 0x01);
        assert_eq!(u8::from(Opcode::Ready), 0x02);
        assert_eq!(u8::from(Opcode::Authenticate), 0x03);
        assert_eq!(u8::from(Opcode::Options), 0x05);
        assert_eq!(u8::from(Opcode::Supported), 0x06);
        assert_eq!(u8::from(Opcode::Query), 0x07);
        assert_eq!(u8::from(Opcode::Result), 0x08);
        assert_eq!(u8::from(Opcode::Prepare), 0x09);
        assert_eq!(u8::from(Opcode::Execute), 0x0A);
        assert_eq!(u8::from(Opcode::Register), 0x0B);
        assert_eq!(u8::from(Opcode::Event), 0x0C);
        assert_eq!(u8::from(Opcode::Batch), 0x0D);
        assert_eq!(u8::from(Opcode::AuthChallenge), 0x0E);
        assert_eq!(u8::from(Opcode::AuthResponse), 0x0F);
        assert_eq!(u8::from(Opcode::AuthSuccess), 0x10);
    }

    #[test]
    fn test_opcode_from() {
        assert_eq!(Opcode::try_from(0x00).unwrap(), Opcode::Error);
        assert_eq!(Opcode::try_from(0x01).unwrap(), Opcode::Startup);
        assert_eq!(Opcode::try_from(0x02).unwrap(), Opcode::Ready);
        assert_eq!(Opcode::try_from(0x03).unwrap(), Opcode::Authenticate);
        assert_eq!(Opcode::try_from(0x05).unwrap(), Opcode::Options);
        assert_eq!(Opcode::try_from(0x06).unwrap(), Opcode::Supported);
        assert_eq!(Opcode::try_from(0x07).unwrap(), Opcode::Query);
        assert_eq!(Opcode::try_from(0x08).unwrap(), Opcode::Result);
        assert_eq!(Opcode::try_from(0x09).unwrap(), Opcode::Prepare);
        assert_eq!(Opcode::try_from(0x0A).unwrap(), Opcode::Execute);
        assert_eq!(Opcode::try_from(0x0B).unwrap(), Opcode::Register);
        assert_eq!(Opcode::try_from(0x0C).unwrap(), Opcode::Event);
        assert_eq!(Opcode::try_from(0x0D).unwrap(), Opcode::Batch);
        assert_eq!(Opcode::try_from(0x0E).unwrap(), Opcode::AuthChallenge);
        assert_eq!(Opcode::try_from(0x0F).unwrap(), Opcode::AuthResponse);
        assert_eq!(Opcode::try_from(0x10).unwrap(), Opcode::AuthSuccess);
    }

    #[test]
    fn test_ready() {
        let raw_envelope = vec![4, 0, 0, 0, 2, 0, 0, 0, 0];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Ready,
            stream_id: 0,
            body: vec![],
            tracing_id: None,
            warnings: vec![],
        };
        let body = ResponseBody::Ready;
        helpers::test_encode_decode_roundtrip_response(&raw_envelope, envelope, body);
    }

    #[test]
    fn test_query_minimal() {
        let raw_envelope = [
            4, 0, 0, 0, 7, 0, 0, 0, 11, 0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64,
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 0,
            body: vec![0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64],
            tracing_id: None,
            warnings: vec![],
        };
        let body = RequestBody::Query(BodyReqQuery {
            query: "blah".into(),
            query_params: QueryParams {
                consistency: Consistency::Any,
                with_names: true,
                values: None,
                page_size: None,
                paging_state: None,
                serial_consistency: None,
                timestamp: None,
                keyspace: None,
                now_in_seconds: None,
            },
        });
        helpers::test_encode_decode_roundtrip_request(&raw_envelope, envelope, body);
    }

    #[test]
    fn test_query_simple_values() {
        let raw_envelope = [
            4, 0, 0, 0, 7, 0, 0, 0, 30, 0, 0, 0, 10, 115, 111, 109, 101, 32, 113, 117, 101, 114,
            121, 0, 8, 1, 0, 2, 0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 255,
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 0,
            body: vec![
                0, 0, 0, 10, 115, 111, 109, 101, 32, 113, 117, 101, 114, 121, 0, 8, 1, 0, 2, 0, 0,
                0, 3, 1, 2, 3, 255, 255, 255, 255,
            ],
            tracing_id: None,
            warnings: vec![],
        };
        let body = RequestBody::Query(BodyReqQuery {
            query: "some query".into(),
            query_params: QueryParams {
                consistency: Consistency::Serial,
                with_names: false,
                values: Some(QueryValues::SimpleValues(vec![
                    Value::Some(vec![1, 2, 3]),
                    Value::Null,
                ])),
                page_size: None,
                paging_state: None,
                serial_consistency: None,
                timestamp: None,
                keyspace: None,
                now_in_seconds: None,
            },
        });
        helpers::test_encode_decode_roundtrip_request(&raw_envelope, envelope, body);
    }

    #[test]
    fn test_query_named_values() {
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 0,
            body: vec![],
            tracing_id: None,
            warnings: vec![],
        };
        let body = RequestBody::Query(BodyReqQuery {
            query: "another query".into(),
            query_params: QueryParams {
                consistency: Consistency::Three,
                with_names: true,
                values: Some(QueryValues::NamedValues(
                    vec![
                        ("foo".to_string(), Value::Some(vec![11, 12, 13])),
                        ("bar".to_string(), Value::NotSet),
                        ("baz".to_string(), Value::Some(vec![42, 10, 99, 100, 4])),
                    ]
                    .into_iter()
                    .collect(),
                )),
                page_size: Some(4),
                paging_state: Some(CBytes::new(vec![0, 1, 2, 3])),
                serial_consistency: Some(Consistency::One),
                timestamp: Some(2000),
                keyspace: None,
                now_in_seconds: None,
            },
        });
        helpers::test_encode_decode_roundtrip_nondeterministic_request(envelope, body);
    }

    #[test]
    fn test_result_prepared_statement() {
        use crate::frame::message_result::{
            BodyResResultPrepared, ColSpec, ColType, ColTypeOption, PreparedMetadata,
            ResResultBody, RowsMetadata, RowsMetadataFlags, TableSpec,
        };
        use crate::types::CBytesShort;

        let raw_envelope = [
            132, 0, 0, 0, 8, 0, 0, 0, 97, // cassandra header
            0, 0, 0, 4, // prepared statement result
            0, 16, 195, 165, 42, 38, 120, 170, 232, 144, 214, 187, 158, 200, 160, 226, 27,
            73, // id
            0, 0, 0, 1, // prepared metadata flags
            0, 0, 0, 3, // columns count
            0, 0, 0, 1, // pk count
            0, 0, // pk index 1
            0, 23, 116, 101, 115, 116, 95, 112, 114, 101, 112, 97, 114, 101, 95, 115, 116, 97, 116,
            101, 109, 101, 110, 116,
            115, // global_table_spec.ks_name = test_prepare_statements
            0, 7, 116, 97, 98, 108, 101, 95, 49, // global_table_spec.table_name = table_1
            0, 2, 105, 100, // ColSpec.name = "id"
            0, 9, // ColSpec.col_type = Int
            0, 1, 120, // ColSpec.name = "x"
            0, 9, // ColSpec.col_type = Int
            0, 4, 110, 97, 109, 101, // ColSpec.name = "name"
            0, 13, // ColSpec.col_type = VarChar
            0, 0, 0, 4, // row metadata flags
            0, 0, 0, 0, // columns count
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Response,
            flags: Flags::empty(),
            opcode: Opcode::Result,
            stream_id: 0,
            body: vec![
                0, 0, 0, 4, // prepared statement result
                0, 16, 195, 165, 42, 38, 120, 170, 232, 144, 214, 187, 158, 200, 160, 226, 27,
                73, // id
                0, 0, 0, 1, // prepared metadata flags
                0, 0, 0, 3, // columns count
                0, 0, 0, 1, // pk count
                0, 0, // pk index 1
                0, 23, 116, 101, 115, 116, 95, 112, 114, 101, 112, 97, 114, 101, 95, 115, 116, 97,
                116, 101, 109, 101, 110, 116,
                115, // global_table_spec.ks_name = test_prepare_statements
                0, 7, 116, 97, 98, 108, 101, 95, 49, // global_table_spec.table_name = table_1
                0, 2, 105, 100, // ColSpec.name = "id"
                0, 9, // ColSpec.col_type = Int
                0, 1, 120, // ColSpec.name = "x"
                0, 9, // ColSpec.col_type = Int
                0, 4, 110, 97, 109, 101, // ColSpec.name = "name"
                0, 13, // ColSpec.col_type = VarChar
                0, 0, 0, 4, // row metadata flags
                0, 0, 0, 0, // columns count
            ],
            tracing_id: None,
            warnings: vec![],
        };
        let body = ResponseBody::Result(ResResultBody::Prepared(BodyResResultPrepared {
            id: CBytesShort::new(vec![
                195, 165, 42, 38, 120, 170, 232, 144, 214, 187, 158, 200, 160, 226, 27, 73,
            ]),
            result_metadata_id: None,
            metadata: PreparedMetadata {
                pk_indexes: vec![0],
                global_table_spec: Some(TableSpec {
                    ks_name: "test_prepare_statements".into(),
                    table_name: "table_1".into(),
                }),
                col_specs: vec![
                    ColSpec {
                        table_spec: None,
                        name: "id".into(),
                        col_type: ColTypeOption {
                            id: ColType::Int,
                            value: None,
                        },
                    },
                    ColSpec {
                        table_spec: None,
                        name: "x".into(),
                        col_type: ColTypeOption {
                            id: ColType::Int,
                            value: None,
                        },
                    },
                    ColSpec {
                        table_spec: None,
                        name: "name".into(),
                        col_type: ColTypeOption {
                            id: ColType::Varchar,
                            value: None,
                        },
                    },
                ],
            },
            result_metadata: RowsMetadata {
                flags: RowsMetadataFlags::NO_METADATA,
                columns_count: 0,
                paging_state: None,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![],
            },
        }));

        helpers::test_encode_decode_roundtrip_response(&raw_envelope, envelope, body);
    }

    fn create_small_envelope_data() -> (Envelope, Vec<u8>) {
        let raw_envelope = vec![
            4, 0, 0, 0, 7, 0, 0, 0, 30, 0, 0, 0, 10, 115, 111, 109, 101, 32, 113, 117, 101, 114,
            121, 0, 8, 1, 0, 2, 0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 255,
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 0,
            body: vec![
                0, 0, 0, 10, 115, 111, 109, 101, 32, 113, 117, 101, 114, 121, 0, 8, 1, 0, 2, 0, 0,
                0, 3, 1, 2, 3, 255, 255, 255, 255,
            ],
            tracing_id: None,
            warnings: vec![],
        };

        (envelope, raw_envelope)
    }

    fn create_large_envelope_data() -> (Envelope, Vec<u8>) {
        let body: Vec<u8> = (0..262144).map(|value| (value % 256) as u8).collect();

        let mut raw_envelope = vec![4, 0, 0, 0, 7, 0, 4, 0, 0];
        raw_envelope.append(&mut body.clone());

        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 0,
            body,
            tracing_id: None,
            warnings: vec![],
        };

        (envelope, raw_envelope)
    }

    #[test]
    fn should_encode_and_decode_legacy_frames() {
        let (envelope, raw_envelope) = create_small_envelope_data();

        let mut encoder = LegacyFrameEncoder::default();
        assert!(encoder.can_fit(raw_envelope.len()));

        encoder.add_envelope(raw_envelope.clone());
        assert!(!encoder.can_fit(1));

        let mut frame = encoder.finalize_self_contained().to_vec();
        assert_eq!(frame, raw_envelope);

        let mut decoder = LegacyFrameDecoder::default();

        let envelopes = decoder.consume(&mut frame, Compression::None).unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0], envelope);

        encoder.reset();
        assert!(encoder.can_fit(raw_envelope.len()));
    }

    #[test]
    fn should_encode_and_decode_uncompressed_self_contained_frames() {
        let (envelope, raw_envelope) = create_small_envelope_data();

        let mut encoder = UncompressedFrameEncoder::default();
        assert!(encoder.can_fit(raw_envelope.len()));

        encoder.add_envelope(raw_envelope.clone());
        assert!(encoder.can_fit(raw_envelope.len()));

        encoder.add_envelope(raw_envelope);

        let mut buffer1 = encoder.finalize_self_contained().to_vec();
        let mut buffer2 = buffer1.split_off(5);

        let mut decoder = UncompressedFrameDecoder::default();

        let envelopes = decoder.consume(&mut buffer1, Compression::None).unwrap();
        assert!(buffer1.is_empty());
        assert!(envelopes.is_empty());

        let envelopes = decoder.consume(&mut buffer2, Compression::None).unwrap();
        assert!(buffer2.is_empty());
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0], envelope);
        assert_eq!(envelopes[1], envelope);
    }

    #[test]
    fn should_encode_and_decode_uncompressed_non_self_contained_frames() {
        let (envelope, raw_envelope) = create_large_envelope_data();

        let mut encoder = UncompressedFrameEncoder::default();
        assert!(!encoder.can_fit(raw_envelope.len()));

        let data_len = raw_envelope.len();
        let mut data_start = 0;
        let mut buffer1 = vec![];

        while data_start < data_len {
            let (data_start_offset, frame) =
                encoder.finalize_non_self_contained(&raw_envelope[data_start..]);

            data_start += data_start_offset;

            buffer1.extend_from_slice(frame);

            encoder.reset();
        }

        let mut buffer2 = buffer1.split_off(PAYLOAD_SIZE_LIMIT);

        let mut decoder = UncompressedFrameDecoder::default();

        let envelopes = decoder.consume(&mut buffer1, Compression::None).unwrap();
        assert!(buffer1.is_empty());
        assert!(envelopes.is_empty());

        let envelopes = decoder.consume(&mut buffer2, Compression::None).unwrap();
        assert!(buffer2.is_empty());
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0], envelope);
    }

    #[test]
    fn should_encode_and_decode_compressed_self_contained_frames() {
        let (envelope, raw_envelope) = create_small_envelope_data();

        let mut encoder = Lz4FrameEncoder::default();
        assert!(encoder.can_fit(raw_envelope.len()));

        encoder.add_envelope(raw_envelope.clone());
        assert!(encoder.can_fit(raw_envelope.len()));

        encoder.add_envelope(raw_envelope);

        let mut buffer1 = encoder.finalize_self_contained().to_vec();
        let mut buffer2 = buffer1.split_off(5);

        let mut decoder = Lz4FrameDecoder::default();

        let envelopes = decoder.consume(&mut buffer1, Compression::None).unwrap();
        assert!(buffer1.is_empty());
        assert!(envelopes.is_empty());

        let envelopes = decoder.consume(&mut buffer2, Compression::None).unwrap();
        assert!(buffer2.is_empty());
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0], envelope);
        assert_eq!(envelopes[1], envelope);
    }

    #[test]
    fn should_encode_and_decode_compressed_non_self_contained_frames() {
        let (envelope, raw_envelope) = create_large_envelope_data();

        let mut encoder = Lz4FrameEncoder::default();
        assert!(!encoder.can_fit(raw_envelope.len()));

        let data_len = raw_envelope.len();
        let mut data_start = 0;
        let mut buffer1 = vec![];

        while data_start < data_len {
            let (data_start_offset, frame) =
                encoder.finalize_non_self_contained(&raw_envelope[data_start..]);

            data_start += data_start_offset;

            buffer1.extend_from_slice(frame);

            encoder.reset();
        }

        let mut buffer2 = buffer1.split_off(1000);

        let mut decoder = Lz4FrameDecoder::default();

        let envelopes = decoder.consume(&mut buffer1, Compression::None).unwrap();
        assert!(buffer1.is_empty());
        assert!(envelopes.is_empty());

        let envelopes = decoder.consume(&mut buffer2, Compression::None).unwrap();
        assert!(buffer2.is_empty());
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0], envelope);
    }
}

#[cfg(test)]
mod flags {
    use super::*;
    use crate::consistency::Consistency;
    use crate::frame::message_query::BodyReqQuery;
    use crate::frame::message_result::ResResultBody;
    use crate::query::query_params::QueryParams;

    #[test]
    fn test_tracing_id_request() {
        let raw_envelope = [
            4, // version
            2, // flags
            0, 12, // stream id
            7,  // opcode
            0, 0, 0, 11, //length
            0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64, // body
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::TRACING,
            opcode: Opcode::Query,
            stream_id: 12,
            body: vec![0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64],
            tracing_id: None,
            warnings: vec![],
        };

        let body = RequestBody::Query(BodyReqQuery {
            query: "blah".into(),
            query_params: QueryParams {
                consistency: Consistency::Any,
                with_names: true,
                values: None,
                page_size: None,
                paging_state: None,
                serial_consistency: None,
                timestamp: None,
                keyspace: None,
                now_in_seconds: None,
            },
        });
        helpers::test_encode_decode_roundtrip_request(&raw_envelope, envelope, body);
    }

    #[test]
    fn test_tracing_id_response() {
        let raw_envelope = [
            132, //version
            2,   // flags
            0, 12, // stream id
            8,  //opcode
            0, 0, 0, 20, // length
            4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87, // tracing_id
            0, 0, 0, 1, // body
        ];
        let envelope = Envelope {
            version: Version::V4,
            direction: Direction::Response,
            flags: Flags::TRACING,
            opcode: Opcode::Result,
            stream_id: 12,
            body: vec![0, 0, 0, 1],
            tracing_id: Some(uuid::Uuid::from_bytes([
                4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87,
            ])),
            warnings: vec![],
        };

        let body = ResponseBody::Result(ResResultBody::Void);

        helpers::test_encode_decode_roundtrip_response(&raw_envelope, envelope, body);
    }

    #[test]
    fn test_truncated_tracing_id_response() {
        let raw_envelope = [
            132, //version
            2,   // flags
            0, 12, // stream id
            8,  //opcode
            0, 0, 0, 4, // length
            0, 0, 0, 1, // body without a tracing id
        ];

        let result = Envelope::from_buffer(&raw_envelope, Compression::None);
        assert!(
            matches!(result, Err(ParseEnvelopeError::InvalidTracingId(_))),
            "{:?}",
            result
        );
    }

    #[test]
    fn test_warnings_response() {
        let raw_envelope = [
            132, // version
            8,   // flags
            5, 64, // stream id
            8,  // opcode
            0, 0, 0, 19, // length
            // warnings
            0, 1, 0, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, // warnings
            0, 0, 0, 1, // body
        ];

        let body = ResponseBody::Result(ResResultBody::Void);

        let envelope = Envelope {
            version: Version::V4,
            opcode: Opcode::Result,
            flags: Flags::WARNING,
            direction: Direction::Response,
            stream_id: 1344,
            tracing_id: None,
            body: vec![0, 0, 0, 1],
            warnings: vec!["Hello World".into()],
        };

        helpers::test_encode_decode_roundtrip_response(&raw_envelope, envelope, body);
    }

    // [bytes map] {"k": [1, 2, 3], "n": null}
    const CUSTOM_PAYLOAD: [u8; 19] = [
        0, 2, // entries
        0, 1, 107, 0, 0, 0, 3, 1, 2, 3, // "k" => [1, 2, 3]
        0, 1, 110, 255, 255, 255, 255, // "n" => null
    ];

    fn response_with_body(flags: Flags, body: &[u8]) -> Vec<u8> {
        let mut raw_envelope = vec![132, flags.bits(), 0, 12, 8];
        raw_envelope.extend_from_slice(&(body.len() as i32).to_be_bytes());
        raw_envelope.extend_from_slice(body);
        raw_envelope
    }

    #[test]
    fn test_custom_payload_response() {
        let mut body = CUSTOM_PAYLOAD.to_vec();
        body.extend_from_slice(&[0, 0, 0, 1]);

        let raw_envelope = response_with_body(Flags::CUSTOM_PAYLOAD, &body);
        let envelope = Envelope::from_buffer(&raw_envelope, Compression::None)
            .unwrap()
            .envelope;

        assert_eq!(envelope.body, vec![0, 0, 0, 1]);
        assert_eq!(
            envelope.response_body().unwrap(),
            ResponseBody::Result(ResResultBody::Void)
        );
    }

    #[test]
    fn test_custom_payload_response_reencodes_consistently() {
        let mut body = CUSTOM_PAYLOAD.to_vec();
        body.extend_from_slice(&[0, 0, 0, 1]);

        let raw_envelope = response_with_body(Flags::CUSTOM_PAYLOAD, &body);
        let envelope = Envelope::from_buffer(&raw_envelope, Compression::None)
            .unwrap()
            .envelope;

        let encoded = envelope.encode_with(Compression::None).unwrap();
        let reparsed = Envelope::from_buffer(&encoded, Compression::None)
            .unwrap()
            .envelope;

        assert_eq!(reparsed.body, vec![0, 0, 0, 1]);
    }

    #[test]
    fn test_custom_payload_after_tracing_id_and_warnings_response() {
        let tracing_id = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];

        let mut body = tracing_id.to_vec();
        body.extend_from_slice(&[0, 1, 0, 2, 104, 105]); // warnings ["hi"]
        body.extend_from_slice(&CUSTOM_PAYLOAD);
        body.extend_from_slice(&[0, 0, 0, 1]);

        let raw_envelope = response_with_body(
            Flags::TRACING | Flags::WARNING | Flags::CUSTOM_PAYLOAD,
            &body,
        );
        let envelope = Envelope::from_buffer(&raw_envelope, Compression::None)
            .unwrap()
            .envelope;

        assert_eq!(envelope.tracing_id, Some(Uuid::from_bytes(tracing_id)));
        assert_eq!(envelope.warnings, vec!["hi".to_string()]);
        assert_eq!(envelope.body, vec![0, 0, 0, 1]);
    }

    #[test]
    fn test_custom_payload_request() {
        let query_body = [0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64];

        let mut body = CUSTOM_PAYLOAD.to_vec();
        body.extend_from_slice(&query_body);

        let mut raw_envelope = vec![4, Flags::CUSTOM_PAYLOAD.bits(), 0, 12, 7];
        raw_envelope.extend_from_slice(&(body.len() as i32).to_be_bytes());
        raw_envelope.extend_from_slice(&body);

        let envelope = Envelope::from_buffer(&raw_envelope, Compression::None)
            .unwrap()
            .envelope;

        assert_eq!(envelope.body, query_body.to_vec());
        assert!(matches!(
            envelope.request_body().unwrap(),
            RequestBody::Query(BodyReqQuery { query, .. }) if query == "blah"
        ));
    }

    #[test]
    fn test_truncated_custom_payload_response() {
        let raw_envelope = response_with_body(Flags::CUSTOM_PAYLOAD, &CUSTOM_PAYLOAD[..15]);

        let result = Envelope::from_buffer(&raw_envelope, Compression::None);
        assert!(
            matches!(result, Err(ParseEnvelopeError::InvalidCustomPayload(_))),
            "{:?}",
            result
        );
    }
}
