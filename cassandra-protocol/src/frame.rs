//! `frame` module contains general Frame functionality.
use bitflags::bitflags;
use derivative::Derivative;
use derive_more::{Constructor, Display};
use std::convert::TryFrom;
use std::io::Cursor;
use uuid::Uuid;

use crate::compression::{Compression, CompressionError};
use crate::frame::frame_request::RequestBody;
use crate::frame::frame_response::ResponseBody;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::{from_cursor_string_list, try_i16_from_bytes, try_i32_from_bytes, UUID_LEN};

pub use crate::frame::traits::*;

/// Number of bytes in the header
const HEADER_LEN: usize = 9;
/// Number of stream bytes in accordance to protocol.
pub const STREAM_LEN: usize = 2;
/// Number of body length bytes in accordance to protocol.
pub const LENGTH_LEN: usize = 4;

pub mod events;
pub mod frame_auth_challenge;
pub mod frame_auth_response;
pub mod frame_auth_success;
pub mod frame_authenticate;
pub mod frame_batch;
pub mod frame_error;
pub mod frame_event;
pub mod frame_execute;
pub mod frame_options;
pub mod frame_prepare;
pub mod frame_query;
pub mod frame_ready;
pub mod frame_register;
pub mod frame_request;
pub mod frame_response;
pub mod frame_result;
pub mod frame_startup;
pub mod frame_supported;
pub mod traits;

use crate::error;

pub const EVENT_STREAM_ID: i16 = -1;

pub type StreamId = i16;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Constructor)]
pub struct ParsedFrame {
    /// How many bytes from the buffer have been read.
    pub frame_len: usize,
    /// Stream id associated with given frame
    pub stream_id: StreamId,
    /// The parsed frame.
    pub frame: Frame,
}

#[derive(Derivative, Clone, PartialEq, Ord, PartialOrd, Eq, Hash, Constructor)]
#[derivative(Debug)]
pub struct Frame {
    pub version: Version,
    pub direction: Direction,
    pub flags: Flags,
    pub opcode: Opcode,
    #[derivative(Debug = "ignore")]
    pub body: Vec<u8>,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

impl Frame {
    #[inline]
    pub fn request_body(&self) -> error::Result<RequestBody> {
        RequestBody::try_from(self.body.as_slice(), self.opcode)
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

    /// Parses the raw bytes of a cassandra frame returning a [`Frame`] struct.
    /// The typical use case is reading from a buffer that may contain 0 or more frames and where the last frame may be incomplete.
    /// The possible return values are:
    /// * `Ok(ParsedFrame)` - The first frame in the buffer has been successfully parsed.
    /// * `Err(ParseFrameError::NotEnoughBytes)` - There are not enough bytes to parse a single frame, [`Frame::from_buffer`] should be recalled when it is possible that there are more bytes.
    /// * `Err(_)` - The frame is malformed and you should close the connection as this method does not provide a way to tell how many bytes to advance the buffer in this case.
    pub fn from_buffer(
        data: &[u8],
        compressor: Compression,
    ) -> Result<ParsedFrame, ParseFrameError> {
        if data.len() < HEADER_LEN {
            return Err(ParseFrameError::NotEnoughBytes);
        }

        let body_len = try_i32_from_bytes(&data[5..9]).unwrap() as usize;
        let frame_len = HEADER_LEN + body_len;
        if data.len() < frame_len {
            return Err(ParseFrameError::NotEnoughBytes);
        }

        let version = Version::try_from(data[0])
            .map_err(|_| ParseFrameError::UnsupportedVersion(data[0] & 0x7f))?;
        let direction = Direction::from(data[0]);
        let flags = Flags::from_bits_truncate(data[1]);
        let stream_id = try_i16_from_bytes(&data[2..4]).unwrap();
        let opcode =
            Opcode::try_from(data[4]).map_err(|_| ParseFrameError::UnsupportedOpcode(data[4]))?;

        let body_bytes = &data[HEADER_LEN..frame_len];

        let full_body = if flags.contains(Flags::COMPRESSION) {
            compressor.decode(body_bytes.to_vec())
        } else {
            Compression::None.decode(body_bytes.to_vec())
        }
        .map_err(ParseFrameError::DecompressionError)?;

        let body_len = full_body.len();

        // Use cursor to get tracing id, warnings and actual body
        let mut body_cursor = Cursor::new(full_body.as_slice());

        let tracing_id = if flags.contains(Flags::TRACING) {
            let mut tracing_bytes = [0; UUID_LEN];
            std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes).unwrap();

            Some(decode_timeuuid(&tracing_bytes).map_err(ParseFrameError::InvalidUuid)?)
        } else {
            None
        };

        let warnings = if flags.contains(Flags::WARNING) {
            from_cursor_string_list(&mut body_cursor).map_err(ParseFrameError::InvalidWarnings)?
        } else {
            vec![]
        };

        let mut body = Vec::with_capacity(body_len - body_cursor.position() as usize);

        std::io::Read::read_to_end(&mut body_cursor, &mut body)
            .expect("Read cannot fail because cursor is backed by slice");

        Ok(ParsedFrame::new(
            frame_len,
            stream_id,
            Frame {
                version,
                direction,
                flags,
                opcode,
                body,
                tracing_id,
                warnings,
            },
        ))
    }

    pub fn encode_with(
        &self,
        stream_id: StreamId,
        compressor: Compression,
    ) -> error::Result<Vec<u8>> {
        let combined_version_byte = u8::from(self.version) | u8::from(self.direction);
        let flag_byte = self.flags.bits();
        let opcode_byte = u8::from(self.opcode);

        let mut v = Vec::with_capacity(9);

        v.push(combined_version_byte);
        v.push(flag_byte);
        v.extend_from_slice(&stream_id.to_be_bytes());
        v.push(opcode_byte);

        if compressor.is_compressed() {
            let mut encoded_body = compressor.encode(&self.body)?;

            let body_len = encoded_body.len() as i32;
            v.extend_from_slice(&body_len.to_be_bytes());
            v.append(&mut encoded_body);
        } else {
            let body_len = self.body.len() as i32;
            v.extend_from_slice(&body_len.to_be_bytes());
            v.extend_from_slice(&self.body);
        }

        Ok(v)
    }
}

#[derive(Debug)]
pub enum ParseFrameError {
    /// There are not enough bytes to parse a single frame, [`Frame::from_buffer`] should be recalled when it is possible that there are more bytes.
    NotEnoughBytes,
    /// The version is not supported by cassandra-protocol, a server implementation should handle this by returning a server error with the message "Invalid or unsupported protocol version".
    UnsupportedVersion(u8),
    UnsupportedOpcode(u8),
    DecompressionError(CompressionError),
    InvalidUuid(uuid::Error),
    InvalidWarnings(error::Error),
}

#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash, Display)]
pub enum Version {
    V3,
    V4,
    // enable v5 feature when it's actually implemented
    //V5,
}

impl From<Version> for u8 {
    fn from(value: Version) -> Self {
        match value {
            Version::V3 => 3,
            Version::V4 => 4,
            //Version::V5 => 5,
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = error::Error;

    fn try_from(version: u8) -> Result<Self, Self::Error> {
        match version & 0x7F {
            3 => Ok(Version::V3),
            4 => Ok(Version::V4),
            //5 => Ok(Version::V5),
            v => Err(error::Error::General(format!(
                "Unknown cassandra version: {}",
                v
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
    /// Frame's flags
    pub struct Flags: u8 {
        const COMPRESSION = 0x01;
        const TRACING = 0x02;
        const CUSTOM_PAYLOAD = 0x04;
        const WARNING = 0x08;
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
            _ => Err(error::Error::General(format!("Unknown opcode: {}", value))),
        }
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::Consistency;
    use crate::frame::frame_query::BodyReqQuery;
    use crate::query::query_params::QueryParams;
    use crate::query::query_values::QueryValues;
    use crate::types::value::Value;
    use crate::types::CBytes;

    #[test]
    fn test_frame_version_as_byte() {
        assert_eq!(u8::from(Version::V3), 0x03);
        assert_eq!(u8::from(Version::V4), 0x04);
        //assert_eq!(u8::from(Version::V5), 0x05);

        assert_eq!(u8::from(Direction::Request), 0x00);
        assert_eq!(u8::from(Direction::Response), 0x80);
    }

    #[test]
    fn test_frame_version_from() {
        assert_eq!(Version::try_from(0x03).unwrap(), Version::V3);
        assert_eq!(Version::try_from(0x83).unwrap(), Version::V3);
        assert_eq!(Version::try_from(0x04).unwrap(), Version::V4);
        assert_eq!(Version::try_from(0x84).unwrap(), Version::V4);
        //assert_eq!(Version::try_from(0x05).unwrap(), Version::V5);
        //assert_eq!(Version::try_from(0x85).unwrap(), Version::V5);

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

    fn test_encode_decode_roundtrip_response(raw_frame: &[u8], frame: Frame, body: ResponseBody) {
        // test encode
        let encoded_body = body.serialize_to_vec();
        assert_eq!(
            &frame.body, &encoded_body,
            "encoded body did not match frames body"
        );

        let encoded_frame = frame.encode_with(0, Compression::None).unwrap();
        assert_eq!(
            raw_frame, &encoded_frame,
            "encoded frame did not match expected raw frame"
        );

        // test decode

        // TODO: implement once we have a sync parse_frame impl
        //let decoded_frame = parse_frame(raw_frame).unwrap();
        //assert_eq!(frame, decoded_frame);

        let decoded_body = frame.response_body().unwrap();
        assert_eq!(body, decoded_body, "decoded frame.body did not match body")
    }

    fn test_encode_decode_roundtrip_request(raw_frame: &[u8], frame: Frame, body: RequestBody) {
        // test encode
        let encoded_body = body.serialize_to_vec();
        assert_eq!(
            &frame.body, &encoded_body,
            "encoded body did not match frames body"
        );

        let encoded_frame = frame.encode_with(0, Compression::None).unwrap();
        assert_eq!(
            raw_frame, &encoded_frame,
            "encoded frame did not match expected raw frame"
        );

        // test decode

        // TODO: implement once we have a sync parse_frame impl
        //let decoded_frame = parse_frame(raw_frame).unwrap();
        //assert_eq!(frame, decoded_frame);

        let decoded_body = frame.request_body().unwrap();
        assert_eq!(body, decoded_body, "decoded frame.body did not match body")
    }

    /// Use this when the body binary representation is nondeterministic but the body typed representation is deterministic
    fn test_encode_decode_roundtrip_nondeterministic_request(mut frame: Frame, body: RequestBody) {
        // test encode
        frame.body = body.serialize_to_vec();

        // test decode
        let decoded_body = frame.request_body().unwrap();
        assert_eq!(body, decoded_body, "decoded frame.body did not match body")
    }

    #[test]
    fn test_ready() {
        let raw_frame = vec![4, 0, 0, 0, 2, 0, 0, 0, 0];
        let frame = Frame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Ready,
            body: vec![],
            tracing_id: None,
            warnings: vec![],
        };
        let body = ResponseBody::Ready;
        test_encode_decode_roundtrip_response(&raw_frame, frame, body);
    }

    #[test]
    fn test_query_minimal() {
        let raw_frame = [
            4, 0, 0, 0, 7, 0, 0, 0, 11, 0, 0, 0, 4, 98, 108, 97, 104, 0, 0, 64,
        ];
        let frame = Frame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
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
            },
        });
        test_encode_decode_roundtrip_request(&raw_frame, frame, body);
    }

    #[test]
    fn test_query_simple_values() {
        let raw_frame = [
            4, 0, 0, 0, 7, 0, 0, 0, 30, 0, 0, 0, 10, 115, 111, 109, 101, 32, 113, 117, 101, 114,
            121, 0, 8, 1, 0, 2, 0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 255,
        ];
        let frame = Frame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
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
            },
        });
        test_encode_decode_roundtrip_request(&raw_frame, frame, body);
    }

    #[test]
    fn test_query_named_values() {
        let frame = Frame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
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
            },
        });
        test_encode_decode_roundtrip_nondeterministic_request(frame, body);
    }
}
