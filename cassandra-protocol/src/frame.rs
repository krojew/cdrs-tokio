//! `frame` module contains general Frame functionality.
use bitflags::bitflags;
use derive_more::Display;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicI16, Ordering};
use uuid::Uuid;

use crate::compression::Compression;
use crate::frame::frame_response::ResponseBody;
pub use crate::frame::traits::*;

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
pub mod frame_response;
pub mod frame_result;
pub mod frame_startup;
pub mod frame_supported;
pub mod traits;

use crate::error;

const INITIAL_STREAM_ID: i16 = 1;
pub const EVENT_STREAM_ID: i16 = -1;

static STREAM_ID: AtomicI16 = AtomicI16::new(INITIAL_STREAM_ID);

pub type StreamId = i16;

fn next_stream_id() -> StreamId {
    loop {
        let stream = STREAM_ID.fetch_add(1, Ordering::SeqCst);
        if stream < 0 {
            match STREAM_ID.compare_exchange_weak(
                stream,
                INITIAL_STREAM_ID,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return INITIAL_STREAM_ID,
                Err(_) => continue,
            }
        }

        return stream;
    }
}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct Frame {
    pub version: Version,
    pub direction: Direction,
    pub flags: Flags,
    pub opcode: Opcode,
    pub stream: StreamId,
    pub body: Vec<u8>,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

impl Frame {
    pub fn new(
        version: Version,
        direction: Direction,
        flags: Flags,
        opcode: Opcode,
        body: Vec<u8>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        let stream = next_stream_id();
        Frame {
            version,
            direction,
            flags,
            opcode,
            stream,
            body,
            tracing_id,
            warnings,
        }
    }

    pub fn body(&self) -> error::Result<ResponseBody> {
        ResponseBody::try_from(self.body.as_slice(), self.opcode, self.version)
    }

    #[inline]
    pub fn tracing_id(&self) -> &Option<Uuid> {
        &self.tracing_id
    }

    #[inline]
    pub fn warnings(&self) -> &Vec<String> {
        &self.warnings
    }

    pub fn encode_with(&self, compressor: Compression) -> error::Result<Vec<u8>> {
        let combined_version_byte = u8::from(self.version) | u8::from(self.direction);
        let flag_byte = self.flags.bits();
        let opcode_byte = u8::from(self.opcode);

        let mut v = Vec::with_capacity(9);

        v.push(combined_version_byte);
        v.push(flag_byte);
        v.extend_from_slice(&self.stream.to_be_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::frame_result::BodyResResultVoid;

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

    fn test_encode_decode_roundtrip(raw_frame: &[u8], frame: Frame, body: ResponseBody) {
        // test encode
        let encoded_body = body.serialize_to_vec();
        assert_eq!(&frame.body, &encoded_body);

        let encoded_frame = frame.encode_with(Compression::None).unwrap();
        assert_eq!(raw_frame, &encoded_frame);

        // TODO: implement once we have a sync parse_frame impl
        // test decode
        //let decoded_frame = parse_frame(raw_frame).unwrap();
        //assert_eq!(frame, decoded_frame);

        let decoded_body = frame.body().unwrap();
        assert_eq!(body, decoded_body)
    }

    #[test]
    fn test_ready() {
        let bytes = vec![4, 0, 0, 0, 2, 0, 0, 0, 0];
        let frame = Frame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Ready,
            stream: 0,
            body: vec![],
            tracing_id: None,
            warnings: vec![],
        };
        let body = ResponseBody::Ready(BodyResResultVoid);
        test_encode_decode_roundtrip(&bytes, frame, body);
    }
}
