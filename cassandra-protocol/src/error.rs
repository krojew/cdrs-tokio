use crate::compression::CompressionError;
use crate::frame::message_error::ErrorBody;
use crate::frame::Opcode;
use crate::types::{CInt, CIntShort};
use std::fmt::{Debug, Display};
use std::io;
use std::net::SocketAddr;
use std::result;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error as ThisError;
use uuid::Error as UuidError;

pub type Result<T> = result::Result<T, Error>;

/// CDRS custom error type. CDRS expects two types of error - errors returned by Server
/// and internal errors occurred within the driver itself. Occasionally `io::Error`
/// is a type that represent internal error because due to implementation IO errors only
/// can be raised by CDRS driver. `Server` error is an error which are ones returned by
/// a Server via result error frames.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Internal IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// Internal error that may be raised during `uuid::Uuid::from_bytes`
    #[error("Uuid parse error: {0}")]
    UuidParse(#[from] UuidError),
    /// General error
    #[error("General error: {0}")]
    General(String),
    /// Internal error that may be raised during `String::from_utf8`
    #[error("FromUtf8 error: {0}")]
    FromUtf8(#[from] FromUtf8Error),
    /// Internal error that may be raised during `str::from_utf8`
    #[error("Utf8 error: {0}")]
    Utf8(#[from] Utf8Error),
    /// Internal Compression/Decompression error.
    #[error("Compressor error: {0}")]
    Compression(#[from] CompressionError),
    /// Server error.
    #[error("Server {addr} error: {body:?}")]
    Server { body: ErrorBody, addr: SocketAddr },
    /// Timed out waiting for an operation to complete.
    #[error("Timeout: {0}")]
    Timeout(String),
    /// Unknown consistency.
    #[error("Unknown consistency: {0}")]
    UnknownConsistency(CIntShort),
    /// Unknown server event.
    #[error("Unknown server event: {0}")]
    UnknownServerEvent(String),
    /// Unexpected topology change event type.
    #[error("Unexpected topology change type: {0}")]
    UnexpectedTopologyChangeType(String),
    /// Unexpected status change event type.
    #[error("Unexpected status change type: {0}")]
    UnexpectedStatusChangeType(String),
    /// Unexpected schema change event type.
    #[error("Unexpected schema change type: {0}")]
    UnexpectedSchemaChangeType(String),
    /// Unexpected schema change event target.
    #[error("Unexpected schema change target: {0}")]
    UnexpectedSchemaChangeTarget(String),
    /// Unexpected additional error info.
    #[error("Unexpected error code: {0}")]
    UnexpectedErrorCode(CInt),
    /// Unexpected write type.
    #[error("Unexpected write type: {0}")]
    UnexpectedWriteType(String),
    /// Expected a request opcode, got something else.
    #[error("Opcode is not a request: {0}")]
    NonRequestOpcode(Opcode),
    /// Expected a response opcode, got something else.
    #[error("Opcode is not a response: {0}")]
    NonResponseOpcode(Opcode),
    /// Unexpected result kind.
    #[error("Unexpected result kind: {0}")]
    UnexpectedResultKind(CInt),
    /// Unexpected column type.
    #[error("Unexpected column type: {0}")]
    UnexpectedColumnType(CIntShort),
    /// Invalid format found for given keyspace replication strategy.
    #[error("Invalid replication format for: {keyspace}")]
    InvalidReplicationFormat { keyspace: String },
    /// Unexpected response to auth message.
    #[error("Unexpected auth response: {0}")]
    UnexpectedAuthResponse(Opcode),
    /// Unexpected startup response.
    #[error("Unexpected startup response: {0}")]
    UnexpectedStartupResponse(Opcode),
}

pub fn column_is_empty_err<T: Display>(column_name: T) -> Error {
    Error::General(format!("Column or Udt property '{}' is empty", column_name))
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::General(err)
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Error {
        Error::General(err.to_string())
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::Io(error) => Error::Io(io::Error::new(
                error.kind(),
                error
                    .get_ref()
                    .map(|error| error.to_string())
                    .unwrap_or_default(),
            )),
            Error::UuidParse(error) => Error::UuidParse(error.clone()),
            Error::General(error) => Error::General(error.clone()),
            Error::FromUtf8(error) => Error::FromUtf8(error.clone()),
            Error::Utf8(error) => Error::Utf8(*error),
            Error::Compression(error) => Error::Compression(error.clone()),
            Error::Server { body, addr } => Error::Server {
                body: body.clone(),
                addr: *addr,
            },
            Error::Timeout(error) => Error::Timeout(error.clone()),
            Error::UnknownConsistency(value) => Error::UnknownConsistency(*value),
            Error::UnknownServerEvent(value) => Error::UnknownServerEvent(value.clone()),
            Error::UnexpectedTopologyChangeType(value) => {
                Error::UnexpectedTopologyChangeType(value.clone())
            }
            Error::UnexpectedStatusChangeType(value) => {
                Error::UnexpectedStatusChangeType(value.clone())
            }
            Error::UnexpectedSchemaChangeType(value) => {
                Error::UnexpectedSchemaChangeType(value.clone())
            }
            Error::UnexpectedSchemaChangeTarget(value) => {
                Error::UnexpectedSchemaChangeTarget(value.clone())
            }
            Error::UnexpectedErrorCode(value) => Error::UnexpectedErrorCode(*value),
            Error::UnexpectedWriteType(value) => Error::UnexpectedWriteType(value.clone()),
            Error::NonRequestOpcode(value) => Error::NonRequestOpcode(*value),
            Error::NonResponseOpcode(value) => Error::NonResponseOpcode(*value),
            Error::UnexpectedResultKind(value) => Error::UnexpectedResultKind(*value),
            Error::UnexpectedColumnType(value) => Error::UnexpectedColumnType(*value),
            Error::InvalidReplicationFormat { keyspace } => Error::InvalidReplicationFormat {
                keyspace: keyspace.clone(),
            },
            Error::UnexpectedAuthResponse(value) => Error::UnexpectedAuthResponse(*value),
            Error::UnexpectedStartupResponse(value) => Error::UnexpectedStartupResponse(*value),
        }
    }
}
