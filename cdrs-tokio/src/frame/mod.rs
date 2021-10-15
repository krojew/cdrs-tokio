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
pub mod parser;
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

#[derive(Debug, Clone)]
pub struct Frame {
    pub version: Version,
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
        flags: Flags,
        opcode: Opcode,
        body: Vec<u8>,
        tracing_id: Option<Uuid>,
        warnings: Vec<String>,
    ) -> Self {
        let stream = next_stream_id();
        Frame {
            version,
            flags,
            opcode,
            stream,
            body,
            tracing_id,
            warnings,
        }
    }

    pub fn body(&self) -> error::Result<ResponseBody> {
        ResponseBody::from(self.body.as_slice(), &self.opcode)
    }

    pub fn tracing_id(&self) -> &Option<Uuid> {
        &self.tracing_id
    }

    pub fn warnings(&self) -> &Vec<String> {
        &self.warnings
    }

    pub fn encode_with(&self, compressor: Compression) -> error::Result<Vec<u8>> {
        let version_byte = u8::from(self.version);
        let flag_byte = self.flags.bits();
        let opcode_byte = u8::from(self.opcode);

        let mut v = Vec::with_capacity(9);

        v.push(version_byte);
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

/// Frame's version
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash, Display)]
pub enum Version {
    Request,
    Response,
}

impl Version {
    /// Number of bytes that represent Cassandra frame's version.
    pub const BYTE_LENGTH: usize = 1;

    /// It returns an actual Cassandra request frame version that CDRS can work with.
    /// This version is based on selected feature - on of `v3`, `v4` or `v5`.
    fn request_version() -> u8 {
        if cfg!(feature = "v3") {
            0x03
        } else if cfg!(feature = "v4") || cfg!(feature = "v5") {
            0x04
        } else {
            panic!(
                "{}",
                "Protocol version is not supported. CDRS should be run with protocol feature \
                 set to v3, v4 or v5"
            );
        }
    }

    /// It returns an actual Cassandra response frame version that CDRS can work with.
    /// This version is based on selected feature - on of `v3`, `v4` or `v5`.
    fn response_version() -> u8 {
        if cfg!(feature = "v3") {
            0x83
        } else if cfg!(feature = "v4") || cfg!(feature = "v5") {
            0x84
        } else {
            panic!(
                "{}",
                "Protocol version is not supported. CDRS should be run with protocol feature \
                 set to v3, v4 or v5"
            );
        }
    }
}

impl From<Version> for u8 {
    fn from(value: Version) -> Self {
        match value {
            Version::Request => Version::request_version(),
            Version::Response => Version::response_version(),
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = error::Error;

    fn try_from(version: u8) -> Result<Self, Self::Error> {
        let req = Version::request_version();
        let res = Version::response_version();

        if version == req {
            Ok(Version::Request)
        } else if version == res {
            Ok(Version::Response)
        } else {
            Err(format!("Unexpected Cassandra version: {}", version).into())
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

    #[test]
    #[cfg(not(feature = "v3"))]
    fn test_frame_version_as_byte() {
        let request_version = Version::Request;
        assert_eq!(u8::from(request_version), 0x04);
        let response_version = Version::Response;
        assert_eq!(u8::from(response_version), 0x84);
    }

    #[test]
    #[cfg(feature = "v3")]
    fn test_frame_version_as_byte_v3() {
        let request_version = Version::Request;
        assert_eq!(u8::from(request_version), 0x03);
        let response_version = Version::Response;
        assert_eq!(u8::from(response_version), 0x83);
    }

    #[test]
    #[cfg(not(feature = "v3"))]
    fn test_frame_version_from() {
        assert_eq!(Version::try_from(0x04).unwrap(), Version::Request);
        assert_eq!(Version::try_from(0x84).unwrap(), Version::Response);
    }

    #[test]
    #[cfg(feature = "v3")]
    fn test_frame_version_from_v3() {
        assert_eq!(Version::try_from(0x03).unwrap(), Version::Request);
        assert_eq!(Version::try_from(0x83).unwrap(), Version::Response);
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
}
