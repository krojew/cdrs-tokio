use derive_more::{Constructor, Display};
use std::convert::{TryFrom, TryInto};
use std::io::{Cursor, Read};

use crate::consistency::Consistency;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::query::QueryFlags;
use crate::query::QueryValues;
use crate::types::value::Value;
use crate::types::{
    from_cursor_str, from_cursor_str_long, serialize_str, serialize_str_long, CBytesShort, CInt,
    CIntShort, CLong,
};
use crate::{error, Error};

#[derive(Debug, Clone, Constructor, PartialEq, Eq)]
pub struct BodyReqBatch {
    pub batch_type: BatchType,
    pub queries: Vec<BatchQuery>,
    pub consistency: Consistency,
    pub serial_consistency: Option<Consistency>,
    pub timestamp: Option<CLong>,
    pub keyspace: Option<String>,
    pub now_in_seconds: Option<CInt>,
}

impl Serialize for BodyReqBatch {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        let batch_type = u8::from(self.batch_type);
        batch_type.serialize(cursor, version);

        let len = self.queries.len() as CIntShort;
        len.serialize(cursor, version);

        for query in &self.queries {
            query.serialize(cursor, version);
        }

        let consistency: CIntShort = self.consistency.into();
        consistency.serialize(cursor, version);

        let mut flags = QueryFlags::empty();
        if self.serial_consistency.is_some() {
            flags.insert(QueryFlags::WITH_SERIAL_CONSISTENCY)
        }

        if self.timestamp.is_some() {
            flags.insert(QueryFlags::WITH_DEFAULT_TIMESTAMP)
        }

        if self.keyspace.is_some() {
            flags.insert(QueryFlags::WITH_KEYSPACE)
        }

        if self.now_in_seconds.is_some() {
            flags.insert(QueryFlags::WITH_NOW_IN_SECONDS)
        }

        flags.serialize(cursor, version);

        if let Some(serial_consistency) = self.serial_consistency {
            let serial_consistency: CIntShort = serial_consistency.into();
            serial_consistency.serialize(cursor, version);
        }

        if let Some(timestamp) = self.timestamp {
            timestamp.serialize(cursor, version);
        }

        if let Some(keyspace) = &self.keyspace {
            serialize_str(cursor, keyspace.as_str(), version);
        }

        if let Some(now_in_seconds) = self.now_in_seconds {
            now_in_seconds.serialize(cursor, version);
        }
    }
}

impl FromCursor for BodyReqBatch {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        let mut batch_type = [0];
        cursor.read_exact(&mut batch_type)?;

        let batch_type = BatchType::try_from(batch_type[0])?;
        let len = CIntShort::from_cursor(cursor, version)?;

        let mut queries = Vec::with_capacity(len as usize);
        for _ in 0..len {
            queries.push(BatchQuery::from_cursor(cursor, version)?);
        }

        let consistency = CIntShort::from_cursor(cursor, version).and_then(TryInto::try_into)?;
        let query_flags = QueryFlags::from_cursor(cursor, version)?;

        let serial_consistency = if query_flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            Some(CIntShort::from_cursor(cursor, version).and_then(TryInto::try_into)?)
        } else {
            None
        };

        let timestamp = if query_flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            Some(CLong::from_cursor(cursor, version)?)
        } else {
            None
        };

        let keyspace = if query_flags.contains(QueryFlags::WITH_KEYSPACE) {
            Some(from_cursor_str(cursor).map(|keyspace| keyspace.to_string())?)
        } else {
            None
        };

        let now_in_seconds = if query_flags.contains(QueryFlags::WITH_NOW_IN_SECONDS) {
            Some(CInt::from_cursor(cursor, version)?)
        } else {
            None
        };

        Ok(BodyReqBatch::new(
            batch_type,
            queries,
            consistency,
            serial_consistency,
            timestamp,
            keyspace,
            now_in_seconds,
        ))
    }
}

/// Batch type
#[derive(Debug, Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Hash, Display)]
pub enum BatchType {
    /// The batch will be "logged". This is equivalent to a
    /// normal CQL3 batch statement.
    Logged,
    /// The batch will be "unlogged".
    Unlogged,
    /// The batch will be a "counter" batch (and non-counter
    /// statements will be rejected).
    Counter,
}

impl TryFrom<u8> for BatchType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BatchType::Logged),
            1 => Ok(BatchType::Unlogged),
            2 => Ok(BatchType::Counter),
            _ => Err(Error::General(format!("Unknown batch type: {}", value))),
        }
    }
}

impl From<BatchType> for u8 {
    fn from(value: BatchType) -> Self {
        match value {
            BatchType::Logged => 0,
            BatchType::Unlogged => 1,
            BatchType::Counter => 2,
        }
    }
}

/// Contains either an id of prepared query or CQL string.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum BatchQuerySubj {
    PreparedId(CBytesShort),
    QueryString(String),
}

/// The structure that represents a query to be batched.
#[derive(Debug, Clone, Constructor, PartialEq, Eq)]
pub struct BatchQuery {
    /// Contains either id of prepared query or a query itself.
    pub subject: BatchQuerySubj,
    /// **Important note:** QueryValues::NamedValues does not work and should not be
    /// used for batches. It is specified in a way that makes it impossible for the server
    /// to implement. This will be fixed in a future version of the native
    /// protocol. See <https://issues.apache.org/jira/browse/CASSANDRA-10246> for
    /// more details
    pub values: QueryValues,
}

impl Serialize for BatchQuery {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match &self.subject {
            BatchQuerySubj::PreparedId(id) => {
                1u8.serialize(cursor, version);
                id.serialize(cursor, version);
            }
            BatchQuerySubj::QueryString(s) => {
                0u8.serialize(cursor, version);
                serialize_str_long(cursor, s, version);
            }
        }

        let len = self.values.len() as CIntShort;
        len.serialize(cursor, version);

        self.values.serialize(cursor, version);
    }
}

impl FromCursor for BatchQuery {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        let mut is_prepared = [0];
        cursor.read_exact(&mut is_prepared)?;

        let is_prepared = is_prepared[0] != 0;

        let subject = if is_prepared {
            BatchQuerySubj::PreparedId(CBytesShort::from_cursor(cursor, version)?)
        } else {
            BatchQuerySubj::QueryString(from_cursor_str_long(cursor).map(Into::into)?)
        };

        let len = CIntShort::from_cursor(cursor, version)?;

        // assuming names are not present due to
        // https://issues.apache.org/jira/browse/CASSANDRA-10246
        let mut values = Vec::with_capacity(len as usize);
        for _ in 0..len {
            values.push(Value::from_cursor(cursor, version)?);
        }

        Ok(BatchQuery::new(subject, QueryValues::SimpleValues(values)))
    }
}

impl Envelope {
    pub fn new_req_batch(query: BodyReqBatch, flags: Flags, version: Version) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Batch;

        Envelope::new(
            version,
            direction,
            flags,
            opcode,
            0,
            query.serialize_to_vec(version),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::consistency::Consistency;
    use crate::frame::message_batch::{BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch};
    use crate::frame::traits::Serialize;
    use crate::frame::{FromCursor, Version};
    use crate::query::QueryValues;
    use crate::types::prelude::Value;

    #[test]
    fn should_deserialize_query() {
        let data = [0, 0, 0, 0, 1, 65, 0, 1, 0xff, 0xff, 0xff, 0xfe];
        let mut cursor = Cursor::new(data.as_slice());

        let query = BatchQuery::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(query.subject, BatchQuerySubj::QueryString("A".into()));
        assert_eq!(query.values, QueryValues::SimpleValues(vec![Value::NotSet]));
    }

    #[test]
    fn should_deserialize_body() {
        let data = [0, 0, 0, 0, 0, 0x10 | 0x20, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqBatch::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(body.batch_type, BatchType::Logged);
        assert!(body.queries.is_empty());
        assert_eq!(body.consistency, Consistency::Any);
        assert_eq!(body.serial_consistency, Some(Consistency::One));
        assert_eq!(body.timestamp, Some(0x0102030405060708));
    }

    #[test]
    fn should_support_keyspace() {
        let keyspace = "abc";
        let body = BodyReqBatch::new(
            BatchType::Logged,
            vec![],
            Consistency::Any,
            None,
            None,
            Some(keyspace.into()),
            None,
        );

        let data = body.serialize_to_vec(Version::V5);
        let body =
            BodyReqBatch::from_cursor(&mut Cursor::new(data.as_slice()), Version::V5).unwrap();
        assert_eq!(body.keyspace, Some(keyspace.to_string()));
    }

    #[test]
    fn should_support_now_in_seconds() {
        let now_in_seconds = 4;
        let body = BodyReqBatch::new(
            BatchType::Logged,
            vec![],
            Consistency::Any,
            None,
            None,
            None,
            Some(now_in_seconds),
        );

        let data = body.serialize_to_vec(Version::V5);
        let body =
            BodyReqBatch::from_cursor(&mut Cursor::new(data.as_slice()), Version::V5).unwrap();
        assert_eq!(body.now_in_seconds, Some(now_in_seconds));
    }
}
