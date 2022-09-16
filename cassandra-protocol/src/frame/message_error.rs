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
    /// `CInt` that points to a type of error.
    pub error_code: CInt,
    /// Error message string.
    pub message: String,
    /// Additional information.
    pub additional_info: AdditionalErrorInfo,
}

impl Serialize for ErrorBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.error_code.serialize(cursor, version);
        serialize_str(cursor, &self.message, version);
        self.additional_info.serialize(cursor, version);
    }
}

impl FromCursor for ErrorBody {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<ErrorBody> {
        let error_code = CInt::from_cursor(cursor, version)?;
        let message = from_cursor_str(cursor)?.to_string();
        let additional_info =
            AdditionalErrorInfo::from_cursor_with_code(cursor, error_code, version)?;

        Ok(ErrorBody {
            error_code,
            message,
            additional_info,
        })
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
/// [Cassandra protocol v4]
/// (<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AdditionalErrorInfo {
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

impl Serialize for AdditionalErrorInfo {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            AdditionalErrorInfo::Unavailable(unavailable) => unavailable.serialize(cursor, version),
            AdditionalErrorInfo::WriteTimeout(write_timeout) => {
                write_timeout.serialize(cursor, version)
            }
            AdditionalErrorInfo::ReadTimeout(read_timeout) => {
                read_timeout.serialize(cursor, version)
            }
            AdditionalErrorInfo::ReadFailure(read_failure) => {
                read_failure.serialize(cursor, version)
            }
            AdditionalErrorInfo::FunctionFailure(function_failure) => {
                function_failure.serialize(cursor, version)
            }
            AdditionalErrorInfo::WriteFailure(write_failure) => {
                write_failure.serialize(cursor, version)
            }
            AdditionalErrorInfo::AlreadyExists(already_exists) => {
                already_exists.serialize(cursor, version)
            }
            AdditionalErrorInfo::Unprepared(unprepared) => unprepared.serialize(cursor, version),
            _ => {}
        }
    }
}

impl AdditionalErrorInfo {
    pub fn from_cursor_with_code(
        cursor: &mut Cursor<&[u8]>,
        error_code: CInt,
        version: Version,
    ) -> error::Result<AdditionalErrorInfo> {
        match error_code {
            0x0000 => Ok(AdditionalErrorInfo::Server),
            0x000A => Ok(AdditionalErrorInfo::Protocol),
            0x0100 => Ok(AdditionalErrorInfo::Authentication),
            0x1000 => {
                UnavailableError::from_cursor(cursor, version).map(AdditionalErrorInfo::Unavailable)
            }
            0x1001 => Ok(AdditionalErrorInfo::Overloaded),
            0x1002 => Ok(AdditionalErrorInfo::IsBootstrapping),
            0x1003 => Ok(AdditionalErrorInfo::Truncate),
            0x1100 => WriteTimeoutError::from_cursor(cursor, version)
                .map(AdditionalErrorInfo::WriteTimeout),
            0x1200 => {
                ReadTimeoutError::from_cursor(cursor, version).map(AdditionalErrorInfo::ReadTimeout)
            }
            0x1300 => {
                ReadFailureError::from_cursor(cursor, version).map(AdditionalErrorInfo::ReadFailure)
            }
            0x1400 => FunctionFailureError::from_cursor(cursor, version)
                .map(AdditionalErrorInfo::FunctionFailure),
            0x1500 => WriteFailureError::from_cursor(cursor, version)
                .map(AdditionalErrorInfo::WriteFailure),
            0x2000 => Ok(AdditionalErrorInfo::Syntax),
            0x2100 => Ok(AdditionalErrorInfo::Unauthorized),
            0x2200 => Ok(AdditionalErrorInfo::Invalid),
            0x2300 => Ok(AdditionalErrorInfo::Config),
            0x2400 => AlreadyExistsError::from_cursor(cursor, version)
                .map(AdditionalErrorInfo::AlreadyExists),
            0x2500 => {
                UnpreparedError::from_cursor(cursor, version).map(AdditionalErrorInfo::Unprepared)
            }
            _ => Err(Error::UnexpectedAdditionalErrorInfo(error_code)),
        }
    }
}

/// Additional info about
/// [unavailable exception]
/// (<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>)
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
#[derive(Debug, PartialEq, Copy, Clone, Ord, PartialOrd, Eq, Hash)]
pub struct WriteTimeoutError {
    /// Consistency level of query.
    pub cl: Consistency,
    /// `i32` representing the number of nodes having acknowledged the request.
    pub received: CInt,
    /// `i32` representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: CInt,
    /// Describes the type of the write that timed out
    pub write_type: WriteType,
}

impl Serialize for WriteTimeoutError {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.cl.serialize(cursor, version);
        self.received.serialize(cursor, version);
        self.block_for.serialize(cursor, version);
        self.write_type.serialize(cursor, version);
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

        Ok(WriteTimeoutError {
            cl,
            received,
            block_for,
            write_type,
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
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone, Display)]
pub enum WriteType {
    /// The write was a non-batched non-counter write
    Simple,
    /// The write was a (logged) batch write.
    /// If this type is received, it means the batch log
    /// has been successfully written
    Batch,
    /// The write was an unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// The write was a counter write (batched or not)
    Counter,
    /// The failure occurred during the write to the batch log when a (logged) batch
    /// write was requested.
    BatchLog,
}

impl Serialize for WriteType {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            WriteType::Simple => serialize_str(cursor, "SIMPLE", version),
            WriteType::Batch => serialize_str(cursor, "BATCH", version),
            WriteType::UnloggedBatch => serialize_str(cursor, "UNLOGGED_BATCH", version),
            WriteType::Counter => serialize_str(cursor, "COUNTER", version),
            WriteType::BatchLog => serialize_str(cursor, "BATCH_LOG", version),
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
            wt => Err(Error::UnexpectedWriteType(wt.into())),
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
/// this host. [Read more...]
/// (<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>)
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
            error_code: 0x0000,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Server,
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
            error_code: 0x000A,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Protocol,
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
            error_code: 0x0100,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Authentication,
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
            error_code: 0x1000,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Unavailable(UnavailableError {
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
            error_code: 0x1001,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Overloaded,
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
            error_code: 0x1002,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::IsBootstrapping,
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
            error_code: 0x1003,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Truncate,
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
            error_code: 0x1100,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::WriteTimeout(WriteTimeoutError {
                cl: Consistency::Any,
                received: 1,
                block_for: 1,
                write_type: WriteType::Simple,
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
            error_code: 0x1200,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::ReadTimeout(ReadTimeoutError {
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
            error_code: 0x1300,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::ReadFailure(ReadFailureError {
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
            error_code: 0x2000,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Syntax,
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
            error_code: 0x2100,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Unauthorized,
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
            error_code: 0x2200,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Invalid,
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
            error_code: 0x2300,
            message: "foo".into(),
            additional_info: AdditionalErrorInfo::Config,
        };
        test_encode_decode(bytes, expected);
    }
}
