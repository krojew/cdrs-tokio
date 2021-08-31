//! `frame` module contains general Frame functionality.
use std::convert::TryFrom;
use std::sync::atomic::{AtomicI16, Ordering};

use crate::compression::Compression;
use crate::frame::frame_response::ResponseBody;
pub use crate::frame::traits::*;
use uuid::Uuid;

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
    pub flags: Vec<Flag>,
    pub opcode: Opcode,
    pub stream: StreamId,
    pub body: Vec<u8>,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

impl Frame {
    pub fn new(
        version: Version,
        flags: Vec<Flag>,
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

    pub fn encode_with(self, compressor: Compression) -> error::Result<Vec<u8>> {
        let version_byte = self.version.as_byte();
        let flag_byte = Flag::many_to_cbytes(&self.flags);
        let opcode_byte = self.opcode.as_byte();
        let mut encoded_body = compressor.encode(self.body)?;
        let body_len = encoded_body.len();

        let mut v = Vec::with_capacity(9 + body_len);

        v.push(version_byte);
        v.push(flag_byte);
        v.extend_from_slice(&self.stream.to_be_bytes());
        v.push(opcode_byte);

        let body_len = body_len as i32;
        v.extend_from_slice(&body_len.to_be_bytes());
        v.append(&mut encoded_body);

        Ok(v)
    }
}

/// Frame's version
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash)]
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

impl AsByte for Version {
    fn as_byte(&self) -> u8 {
        match self {
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

/// Frame's flags
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash)]
pub enum Flag {
    Compression,
    Tracing,
    CustomPayload,
    Warning,
    Ignore,
}

impl Flag {
    const BYTE_LENGTH: usize = 1;

    /// It returns selected flags collection.
    pub fn collection(flags: u8) -> Vec<Flag> {
        let mut found_flags: Vec<Flag> = vec![];

        if Flag::has_compression(flags) {
            found_flags.push(Flag::Compression);
        }

        if Flag::has_tracing(flags) {
            found_flags.push(Flag::Tracing);
        }

        if Flag::has_custom_payload(flags) {
            found_flags.push(Flag::CustomPayload);
        }

        if Flag::has_warning(flags) {
            found_flags.push(Flag::Warning);
        }

        found_flags
    }

    /// The method converts a series of `Flag`-s into a single byte.
    pub fn many_to_cbytes(flags: &[Flag]) -> u8 {
        flags
            .iter()
            .fold(Flag::Ignore.as_byte(), |acc, f| acc | f.as_byte())
    }

    /// Indicates if flags contains `Flag::Compression`
    pub fn has_compression(flags: u8) -> bool {
        (flags & Flag::Compression.as_byte()) > 0
    }

    /// Indicates if flags contains `Flag::Tracing`
    pub fn has_tracing(flags: u8) -> bool {
        (flags & Flag::Tracing.as_byte()) > 0
    }

    /// Indicates if flags contains `Flag::CustomPayload`
    pub fn has_custom_payload(flags: u8) -> bool {
        (flags & Flag::CustomPayload.as_byte()) > 0
    }

    /// Indicates if flags contains `Flag::Warning`
    pub fn has_warning(flags: u8) -> bool {
        (flags & Flag::Warning.as_byte()) > 0
    }
}

impl AsByte for Flag {
    fn as_byte(&self) -> u8 {
        match self {
            Flag::Compression => 0x01,
            Flag::Tracing => 0x02,
            Flag::CustomPayload => 0x04,
            Flag::Warning => 0x08,
            Flag::Ignore => 0x00,
            // assuming that ingoing value would be other than [0x01, 0x02, 0x04, 0x08]
        }
    }
}

impl From<u8> for Flag {
    fn from(f: u8) -> Flag {
        match f {
            0x01 => Flag::Compression,
            0x02 => Flag::Tracing,
            0x04 => Flag::CustomPayload,
            0x08 => Flag::Warning,
            _ => Flag::Ignore, // ignore by specification
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash)]
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

impl AsByte for Opcode {
    fn as_byte(&self) -> u8 {
        match self {
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
    use crate::frame::traits::AsByte;

    #[test]
    #[cfg(not(feature = "v3"))]
    fn test_frame_version_as_byte() {
        let request_version = Version::Request;
        assert_eq!(request_version.as_byte(), 0x04);
        let response_version = Version::Response;
        assert_eq!(response_version.as_byte(), 0x84);
    }

    #[test]
    #[cfg(feature = "v3")]
    fn test_frame_version_as_byte_v3() {
        let request_version = Version::Request;
        assert_eq!(request_version.as_byte(), 0x03);
        let response_version = Version::Response;
        assert_eq!(response_version.as_byte(), 0x83);
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
    fn test_flag_from() {
        assert_eq!(Flag::from(0x01_u8), Flag::Compression);
        assert_eq!(Flag::from(0x02_u8), Flag::Tracing);
        assert_eq!(Flag::from(0x04_u8), Flag::CustomPayload);
        assert_eq!(Flag::from(0x08_u8), Flag::Warning);
        // rest should be interpreted as Ignore
        assert_eq!(Flag::from(0x10_u8), Flag::Ignore);
        assert_eq!(Flag::from(0x31_u8), Flag::Ignore);
    }

    #[test]
    fn test_flag_as_byte() {
        assert_eq!(Flag::Compression.as_byte(), 0x01);
        assert_eq!(Flag::Tracing.as_byte(), 0x02);
        assert_eq!(Flag::CustomPayload.as_byte(), 0x04);
        assert_eq!(Flag::Warning.as_byte(), 0x08);
    }

    #[test]
    fn test_flag_has_x() {
        assert!(Flag::has_compression(0x01));
        assert!(!Flag::has_compression(0x02));

        assert!(Flag::has_tracing(0x02));
        assert!(!Flag::has_tracing(0x01));

        assert!(Flag::has_custom_payload(0x04));
        assert!(!Flag::has_custom_payload(0x02));

        assert!(Flag::has_warning(0x08));
        assert!(!Flag::has_warning(0x01));
    }

    #[test]
    fn test_flag_many_to_cbytes() {
        let all = vec![
            Flag::Compression,
            Flag::Tracing,
            Flag::CustomPayload,
            Flag::Warning,
        ];
        assert_eq!(Flag::many_to_cbytes(&all), 1 | 2 | 4 | 8);
        let some = vec![Flag::Compression, Flag::Warning];
        assert_eq!(Flag::many_to_cbytes(&some), 1 | 8);
        let one = vec![Flag::Compression];
        assert_eq!(Flag::many_to_cbytes(&one), 1);
    }

    #[test]
    fn test_flag_collection() {
        let all = vec![
            Flag::Compression,
            Flag::Tracing,
            Flag::CustomPayload,
            Flag::Warning,
        ];
        assert_eq!(Flag::collection(1 | 2 | 4 | 8), all);
        let some = vec![Flag::Compression, Flag::Warning];
        assert_eq!(Flag::collection(1 | 8), some);
        let one = vec![Flag::Compression];
        assert_eq!(Flag::collection(1), one);
    }

    #[test]
    fn test_opcode_as_byte() {
        assert_eq!(Opcode::Error.as_byte(), 0x00);
        assert_eq!(Opcode::Startup.as_byte(), 0x01);
        assert_eq!(Opcode::Ready.as_byte(), 0x02);
        assert_eq!(Opcode::Authenticate.as_byte(), 0x03);
        assert_eq!(Opcode::Options.as_byte(), 0x05);
        assert_eq!(Opcode::Supported.as_byte(), 0x06);
        assert_eq!(Opcode::Query.as_byte(), 0x07);
        assert_eq!(Opcode::Result.as_byte(), 0x08);
        assert_eq!(Opcode::Prepare.as_byte(), 0x09);
        assert_eq!(Opcode::Execute.as_byte(), 0x0A);
        assert_eq!(Opcode::Register.as_byte(), 0x0B);
        assert_eq!(Opcode::Event.as_byte(), 0x0C);
        assert_eq!(Opcode::Batch.as_byte(), 0x0D);
        assert_eq!(Opcode::AuthChallenge.as_byte(), 0x0E);
        assert_eq!(Opcode::AuthResponse.as_byte(), 0x0F);
        assert_eq!(Opcode::AuthSuccess.as_byte(), 0x10);
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
