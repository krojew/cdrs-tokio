use std::fmt::{Debug, Display};
use std::io;
use std::result;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error as ThisError;
use uuid::Error as UuidError;

use crate::compression::CompressionError;
use crate::frame::message_error::ErrorBody;

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
    /// Internal Compression/Decompression error
    #[error("Compressor error: {0}")]
    Compression(#[from] CompressionError),
    /// Server error.
    #[error("Server error: {0:?}")]
    Server(ErrorBody),
    /// Timed out waiting for an operation to complete.
    #[error("Timeout: {0}")]
    Timeout(String),
}

pub fn column_is_empty_err<T: Display>(column_name: T) -> Error {
    Error::General(format!("Column or Udt property '{}' is empty", column_name))
}

impl From<ErrorBody> for Error {
    fn from(err: ErrorBody) -> Error {
        Error::Server(err)
    }
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
            Error::Server(error) => Error::Server(error.clone()),
            Error::Timeout(error) => Error::Timeout(error.clone()),
        }
    }
}
