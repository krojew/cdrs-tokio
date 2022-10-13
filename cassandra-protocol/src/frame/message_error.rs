use super::Serialize;
use crate::consistency::Consistency;
use crate::frame::traits::FromCursor;
use crate::frame::Version;
use crate::types::*;
use crate::{error, Error};
/// This modules contains [Cassandra's errors](<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>)
/// which server could respond to client.
use derive_more::Display;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::net::SocketAddr;

/// CDRS error which could be returned by Cassandra server as a response. As in the specification,
/// it contains an error code and an error message. Apart of those depending of type of error,
/// it could contain additional information represented by `additional_info` property.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ErrorBody {
    /// Error message.
    pub message: String,
    /// The type of error, possibly including type specific additional information.
    pub ty: ErrorType,
}

impl Serialize for ErrorBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.ty.to_error_code().serialize(cursor, version);
        serialize_str(cursor, &self.message, version);
        self.ty.serialize(cursor, version);
    }
}

impl FromCursor for ErrorBody {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<ErrorBody> {
        let error_code = CInt::from_cursor(cursor, version)?;
        let message = from_cursor_str(cursor)?.to_string();
        let ty = ErrorType::from_cursor_with_code(cursor, error_code, version)?;

        Ok(ErrorBody { message, ty })
    }
}

/// Protocol-dependent failure information. V5 contains a map of endpoint->code entries, while
/// previous versions contain only error count.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FailureInfo {
    /// Represents the number of nodes that experience a failure while executing the request.
    NumFailures(CInt),
    /// Error code map for affected nodes.
    ReasonMap(HashMap<SocketAddr, CIntShort>),
}

impl Serialize for FailureInfo {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            FailureInfo::NumFailures(count) => count.serialize(cursor, version),
            FailureInfo::ReasonMap(map) => {
                let num_failures = map.len() as CInt;
                num_failures.serialize(cursor, version);

                for (endpoint, error_code) in map {
                    endpoint.serialize(cursor, version);
                    error_code.serialize(cursor, version);
                }
            }
        }
    }
}

impl FromCursor for FailureInfo {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        Ok(match version {
            Version::V3 | Version::V4 => Self::NumFailures(CInt::from_cursor(cursor, version)?),
            Version::V5 => {
                let num_failures = CInt::from_cursor(cursor, version)?;
                let mut map = HashMap::with_capacity(num_failures as usize);

                for _ in 0..num_failures {
                    let endpoint = SocketAddr::from_cursor(cursor, version)?;
                    let error_code = CIntShort::from_cursor(cursor, version)?;
                    map.insert(endpoint, error_code);
                }

                Self::ReasonMap(map)
            }
        })
    }
}

/// Additional error info in accordance to
/// [Cassandra protocol v4](<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>).
#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum ErrorType {
    Server,
    Protocol,
    Authentication,
    Unavailable(UnavailableError),
    Overloaded,
    IsBootstrapping,
    Truncate,
    WriteTimeout(WriteTimeoutError),
    ReadTimeout(ReadTimeoutError),
    ReadFailure(ReadFailureError),
    FunctionFailure(FunctionFailureError),
    WriteFailure(WriteFailureError),
    Syntax,
    Unauthorized,
    Invalid,
    Config,
    AlreadyExists(AlreadyExistsError),
    Unprepared(UnpreparedError),
}

impl Serialize for ErrorType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            ErrorType::Unavailable(unavailable) => unavailable.serialize(cursor, version),
            ErrorType::WriteTimeout(write_timeout) => write_timeout.serialize(cursor, version),
            ErrorType::ReadTimeout(read_timeout) => read_timeout.serialize(cursor, version),
            ErrorType::ReadFailure(read_failure) => read_failure.serialize(cursor, version),
            ErrorType::FunctionFailure(function_failure) => {
                function_failure.serialize(cursor, version)
            }
            ErrorType::WriteFailure(write_failure) => write_failure.serialize(cursor, version),
            ErrorType::AlreadyExists(already_exists) => already_exists.serialize(cursor, version),
            ErrorType::Unprepared(unprepared) => unprepared.serialize(cursor, version),
            _ => {}
        }
    }
}

impl ErrorType {
    pub fn from_cursor_with_code(
        cursor: &mut Cursor<&[u8]>,
        error_code: CInt,
        version: Version,
    ) -> error::Result<ErrorType> {
        match error_code {
            0x0000 => Ok(ErrorType::Server),
            0x000A => Ok(ErrorType::Protocol),
            0x0100 => Ok(ErrorType::Authentication),
            0x1000 => UnavailableError::from_cursor(cursor, version).map(ErrorType::Unavailable),
            0x1001 => Ok(ErrorType::Overloaded),
            0x1002 => Ok(ErrorType::IsBootstrapping),
            0x1003 => Ok(ErrorType::Truncate),
            0x1100 => WriteTimeoutError::from_cursor(cursor, version).map(ErrorType::WriteTimeout),
            0x1200 => ReadTimeoutError::from_cursor(cursor, version).map(ErrorType::ReadTimeout),
            0x1300 => ReadFailureError::from_cursor(cursor, version).map(ErrorType::ReadFailure),
            0x1400 => {
                FunctionFailureError::from_cursor(cursor, version).map(ErrorType::FunctionFailure)
            }
            0x1500 => WriteFailureError::from_cursor(cursor, version).map(ErrorType::WriteFailure),
            0x2000 => Ok(ErrorType::Syntax),
            0x2100 => Ok(ErrorType::Unauthorized),
            0x2200 => Ok(ErrorType::Invalid),
            0x2300 => Ok(ErrorType::Config),
            0x2400 => {
                AlreadyExistsError::from_cursor(cursor, version).map(ErrorType::AlreadyExists)
            }
            0x2500 => UnpreparedError::from_cursor(cursor, version).map(ErrorType::Unprepared),
            _ => Err(Error::UnexpectedErrorCode(error_code)),
        }
    }

    pub fn to_error_code(&self) -> CInt {
        match self {
            ErrorType::Server => 0x0000,
            ErrorType::Protocol => 0x000A,
            ErrorType::Authentication => 0x0100,
            ErrorType::Unavailable(_) => 0x1000,
            ErrorType::Overloaded => 0x1001,
            ErrorType::IsBootstrapping => 0x1002,
            ErrorType::Truncate => 0x1003,
            ErrorType::WriteTimeout(_) => 0x1100,
            ErrorType::ReadTimeout(_) => 0x1200,
            ErrorType::ReadFailure(_) => 0x1300,
            ErrorType::FunctionFailure(_) => 0x1400,
            ErrorType::WriteFailure(_) => 0x1500,
            ErrorType::Syntax => 0x2000,
            ErrorType::Unauthorized => 0x2100,
            ErrorType::Invalid => 0x2200,
            ErrorType::Config => 0x2300,
            ErrorType::AlreadyExists(_) => 0x2400,
            ErrorType::Unprepared(_) => 0x2500,
        }
    }
}

/// Additional info about
/// [unavailable exception](<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>)
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Copy, Clone, Hash)]
pub struct UnavailableError {
    /// Consistency level of query.
    pub cl: Consistency,
    /// Number of nodes that should be available to respect `cl`.
    pub required: CInt,
    /// Number of replicas that we were know to be alive.
    pub alive: CInt,
}

impl Serialize for UnavailableError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.required.serialize(cursor, version);
        self.alive.serialize(cursor, version);
    }
}

impl FromCursor for UnavailableError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<UnavailableError> {
        let cl = Consistency::from_cursor(cursor, version)?;
        let required = CInt::from_cursor(cursor, version)?;
        let alive = CInt::from_cursor(cursor, version)?;

        Ok(UnavailableError {
            cl,
            required,
            alive,
        })
    }
}

/// Timeout exception during a write request.
#[derive(Debug, PartialEq, Clone, Ord, PartialOrd, Eq, Hash)]
pub struct WriteTimeoutError {
    /// Consistency level of query.
    pub cl: Consistency,
    /// `i32` representing the number of nodes having acknowledged the request.
    pub received: CInt,
    /// `i32` representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: CInt,
    /// Describes the type of the write that timed out.
    pub write_type: WriteType,
    /// The number of contentions occurred during the CAS operation. The field only presents when
    /// the `write_type` is `Cas`.
    pub contentions: Option<CIntShort>,
}

impl Serialize for WriteTimeoutError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.received.serialize(cursor, version);
        self.block_for.serialize(cursor, version);
        self.write_type.serialize(cursor, version);

        if let Some(contentions) = self.contentions {
            contentions.serialize(cursor, version);
        }
    }
}

impl FromCursor for WriteTimeoutError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<WriteTimeoutError> {
        let cl = Consistency::from_cursor(cursor, version)?;
        let received = CInt::from_cursor(cursor, version)?;
        let block_for = CInt::from_cursor(cursor, version)?;
        let write_type = WriteType::from_cursor(cursor, version)?;
        let contentions = if write_type == WriteType::Cas {
            Some(CIntShort::from_cursor(cursor, version)?)
        } else {
            None
        };

        Ok(WriteTimeoutError {
            cl,
            received,
            block_for,
            write_type,
            contentions,
        })
    }
}

/// Timeout exception during a read request.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Copy, Clone, Hash)]
pub struct ReadTimeoutError {
    /// Consistency level of query.
    pub cl: Consistency,
    /// `i32` representing the number of nodes having acknowledged the request.
    pub received: CInt,
    /// `i32` representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: CInt,
    data_present: u8,
}

impl Serialize for ReadTimeoutError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.received.serialize(cursor, version);
        self.block_for.serialize(cursor, version);
        self.data_present.serialize(cursor, version);
    }
}

impl ReadTimeoutError {
    /// Shows if a replica has responded to a query.
    #[inline]
    pub fn replica_has_responded(&self) -> bool {
        self.data_present != 0
    }
}

impl FromCursor for ReadTimeoutError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<ReadTimeoutError> {
        let cl = Consistency::from_cursor(cursor, version)?;
        let received = CInt::from_cursor(cursor, version)?;
        let block_for = CInt::from_cursor(cursor, version)?;

        let mut buff = [0];
        cursor.read_exact(&mut buff)?;

        let data_present = buff[0];

        Ok(ReadTimeoutError {
            cl,
            received,
            block_for,
            data_present,
        })
    }
}

/// A non-timeout exception during a read request.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReadFailureError {
    /// Consistency level of query.
    pub cl: Consistency,
    /// The number of nodes having acknowledged the request.
    pub received: CInt,
    /// The number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: CInt,
    /// Failure information.
    pub failure_info: FailureInfo,
    data_present: u8,
}

impl Serialize for ReadFailureError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.received.serialize(cursor, version);
        self.block_for.serialize(cursor, version);
        self.failure_info.serialize(cursor, version);
        self.data_present.serialize(cursor, version);
    }
}

impl ReadFailureError {
    /// Shows if replica has responded to a query.
    #[inline]
    pub fn replica_has_responded(&self) -> bool {
        self.data_present != 0
    }
}

impl FromCursor for ReadFailureError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<ReadFailureError> {
        let cl = Consistency::from_cursor(cursor, version)?;
        let received = CInt::from_cursor(cursor, version)?;
        let block_for = CInt::from_cursor(cursor, version)?;
        let failure_info = FailureInfo::from_cursor(cursor, version)?;

        let mut buff = [0];
        cursor.read_exact(&mut buff)?;

        let data_present = buff[0];

        Ok(ReadFailureError {
            cl,
            received,
            block_for,
            failure_info,
            data_present,
        })
    }
}

/// A (user defined) function failed during execution.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct FunctionFailureError {
    /// The keyspace of the failed function.
    pub keyspace: String,
    /// The name of the failed function
    pub function: String,
    /// One string for each argument type (as CQL type) of the failed function.
    pub arg_types: Vec<String>,
}

impl Serialize for FunctionFailureError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str(cursor, &self.keyspace, version);
        serialize_str(cursor, &self.function, version);
        serialize_str_list(cursor, self.arg_types.iter().map(|x| x.as_str()), version);
    }
}

impl FromCursor for FunctionFailureError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        _version: Version,
    ) -> error::Result<FunctionFailureError> {
        let keyspace = from_cursor_str(cursor)?.to_string();
        let function = from_cursor_str(cursor)?.to_string();
        let arg_types = from_cursor_string_list(cursor)?;

        Ok(FunctionFailureError {
            keyspace,
            function,
            arg_types,
        })
    }
}

/// A non-timeout exception during a write request.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WriteFailureError {
    /// Consistency of the query having triggered the exception.
    pub cl: Consistency,
    /// The number of nodes having answered the request.
    pub received: CInt,
    /// The number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: CInt,
    /// Failure information.
    pub failure_info: FailureInfo,
    /// describes the type of the write that failed.
    pub write_type: WriteType,
}

impl Serialize for WriteFailureError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.received.serialize(cursor, version);
        self.block_for.serialize(cursor, version);
        self.failure_info.serialize(cursor, version);
        self.write_type.serialize(cursor, version);
    }
}

impl FromCursor for WriteFailureError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<WriteFailureError> {
        let cl = Consistency::from_cursor(cursor, version)?;
        let received = CInt::from_cursor(cursor, version)?;
        let block_for = CInt::from_cursor(cursor, version)?;
        let failure_info = FailureInfo::from_cursor(cursor, version)?;
        let write_type = WriteType::from_cursor(cursor, version)?;

        Ok(WriteFailureError {
            cl,
            received,
            block_for,
            failure_info,
            write_type,
        })
    }
}

/// Describes the type of the write that failed.
/// [Read more...](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1118)
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Display)]
#[non_exhaustive]
pub enum WriteType {
    /// The write was a non-batched non-counter write.
    Simple,
    /// The write was a (logged) batch write. If this type is received, it means the batch log
    /// has been successfully written.
    Batch,
    /// The write was an unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// The write was a counter write (batched or not).
    Counter,
    /// The failure occurred during the write to the batch log when a (logged) batch
    /// write was requested.
    BatchLog,
    /// The timeout occurred during the Compare And Set write/update.
    Cas,
    /// The timeout occurred when a write involves VIEW update and failure to acquire local view(MV)
    /// lock for key within timeout.
    View,
    /// The timeout occurred when cdc_total_space is exceeded when doing a write to data tracked by
    /// cdc.
    Cdc,
    /// Unknown write type.
    Unknown(String),
}

impl Serialize for WriteType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            WriteType::Simple => serialize_str(cursor, "SIMPLE", version),
            WriteType::Batch => serialize_str(cursor, "BATCH", version),
            WriteType::UnloggedBatch => serialize_str(cursor, "UNLOGGED_BATCH", version),
            WriteType::Counter => serialize_str(cursor, "COUNTER", version),
            WriteType::BatchLog => serialize_str(cursor, "BATCH_LOG", version),
            WriteType::Cas => serialize_str(cursor, "CAS", version),
            WriteType::View => serialize_str(cursor, "VIEW", version),
            WriteType::Cdc => serialize_str(cursor, "CDC", version),
            WriteType::Unknown(write_type) => serialize_str(cursor, write_type, version),
        }
    }
}

impl FromCursor for WriteType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<WriteType> {
        match from_cursor_str(cursor)? {
            "SIMPLE" => Ok(WriteType::Simple),
            "BATCH" => Ok(WriteType::Batch),
            "UNLOGGED_BATCH" => Ok(WriteType::UnloggedBatch),
            "COUNTER" => Ok(WriteType::Counter),
            "BATCH_LOG" => Ok(WriteType::BatchLog),
            "CAS" => Ok(WriteType::Cas),
            "VIEW" => Ok(WriteType::View),
            "CDC" => Ok(WriteType::Cdc),
            wt => Ok(WriteType::Unknown(wt.into())),
        }
    }
}

/// The query attempted to create a keyspace or a table that was already existing.
/// [Read more...](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1140)
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct AlreadyExistsError {
    /// Represents either the keyspace that already exists,
    /// or the keyspace in which the table that already exists is.
    pub ks: String,
    /// Represents the name of the table that already exists.
    pub table: String,
}

impl Serialize for AlreadyExistsError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str(cursor, &self.ks, version);
        serialize_str(cursor, &self.table, version);
    }
}

impl FromCursor for AlreadyExistsError {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        _version: Version,
    ) -> error::Result<AlreadyExistsError> {
        let ks = from_cursor_str(cursor)?.to_string();
        let table = from_cursor_str(cursor)?.to_string();

        Ok(AlreadyExistsError { ks, table })
    }
}

/// Can be thrown while a prepared statement tries to be
/// executed if the provided prepared statement ID is not known by
/// this host. [Read more...](<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>)
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct UnpreparedError {
    /// Unknown ID.
    pub id: CBytesShort,
}

impl Serialize for UnpreparedError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.id.serialize(cursor, version);
    }
}

impl FromCursor for UnpreparedError {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<UnpreparedError> {
        let id = CBytesShort::from_cursor(cursor, version)?;
        Ok(UnpreparedError { id })
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
fn test_encode_decode(bytes: &[u8], expected: ErrorBody) {
    {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        let result = ErrorBody::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(expected, result);
    }

    {
        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);
        expected.serialize(&mut cursor, Version::V4);
        assert_eq!(buffer, bytes);
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn server() {
        let bytes = &[
            0, 0, 0, 0, // server
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Server,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn protocol() {
        let bytes = &[
            0, 0, 0, 10, // protocol
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Protocol,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn authentication() {
        let bytes = &[
            0, 0, 1, 0, // authentication error
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Authentication,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn unavailable() {
        let bytes = &[
            0, 0, 16, 0, // unavailable
            0, 3, 102, 111, 111, // message - foo
            //
            // unavailable error
            0, 0, // consistency any
            0, 0, 0, 1, // required
            0, 0, 0, 1, // alive
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Unavailable(UnavailableError {
                cl: Consistency::Any,
                required: 1,
                alive: 1,
            }),
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn overloaded() {
        let bytes = &[
            0, 0, 16, 1, // authentication error
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Overloaded,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn is_bootstrapping() {
        let bytes = &[
            0, 0, 16, 2, // is bootstrapping
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::IsBootstrapping,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn truncate() {
        let bytes = &[
            0, 0, 16, 3, // truncate
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Truncate,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn write_timeout() {
        let bytes = &[
            0, 0, 17, 0, // write timeout
            0, 3, 102, 111, 111, // message - foo
            //
            // timeout error
            0, 0, // consistency any
            0, 0, 0, 1, // received
            0, 0, 0, 1, // block_for
            0, 6, 83, 73, 77, 80, 76, 69, // Write type simple
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::WriteTimeout(WriteTimeoutError {
                cl: Consistency::Any,
                received: 1,
                block_for: 1,
                write_type: WriteType::Simple,
                contentions: None,
            }),
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn read_timeout() {
        let bytes = &[
            0, 0, 18, 0, // read timeout
            0, 3, 102, 111, 111, // message - foo
            //
            // read timeout
            0, 0, // consistency any
            0, 0, 0, 1, // received
            0, 0, 0, 1, // block_for
            0, // data present
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::ReadTimeout(ReadTimeoutError {
                cl: Consistency::Any,
                received: 1,
                block_for: 1,
                data_present: 0,
            }),
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn read_failure() {
        let bytes = &[
            0, 0, 19, 0, // read failure
            0, 3, 102, 111, 111, // message - foo
            //
            // read timeout
            0, 0, // consistency any
            0, 0, 0, 1, // received
            0, 0, 0, 1, // block_for
            0, 0, 0, 1, // num failure
            0, // data present
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::ReadFailure(ReadFailureError {
                cl: Consistency::Any,
                received: 1,
                block_for: 1,
                failure_info: FailureInfo::NumFailures(1),
                data_present: 0,
            }),
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn syntax() {
        let bytes = &[
            0, 0, 32, 0, // syntax
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Syntax,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn unauthorized() {
        let bytes = &[
            0, 0, 33, 0, // unauthorized
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Unauthorized,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn invalid() {
        let bytes = &[
            0, 0, 34, 0, // invalid
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Invalid,
        };
        test_encode_decode(bytes, expected);
    }

    #[test]
    fn config() {
        let bytes = &[
            0, 0, 35, 0, // config
            0, 3, 102, 111, 111, // message - foo
        ];
        let expected = ErrorBody {
            message: "foo".into(),
            ty: ErrorType::Config,
        };
        test_encode_decode(bytes, expected);
    }
}
