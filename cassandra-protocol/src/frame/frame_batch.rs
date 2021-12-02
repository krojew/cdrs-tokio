use std::io::Cursor;

use crate::consistency::Consistency;
use crate::frame::*;
use crate::query::QueryValues;
use crate::query::{PreparedQuery, QueryFlags};
use crate::types::*;
use crate::Error;

/// `BodyResReady`
#[derive(Debug, Clone)]
pub struct BodyReqBatch {
    pub batch_type: BatchType,
    pub queries: Vec<BatchQuery>,
    pub consistency: Consistency,
    // **IMPORTANT NOTE:** with names flag does not work and should not be used.
    pub query_flags: QueryFlags,
    pub serial_consistency: Option<Consistency>,
    pub timestamp: Option<i64>,
    pub is_idempotent: bool,
    pub keyspace: Option<String>,
}

impl Serialize for BodyReqBatch {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let batch_type = u8::from(self.batch_type);
        batch_type.serialize(cursor);

        let len = self.queries.len() as CIntShort;
        len.serialize(cursor);

        for query in &self.queries {
            query.serialize(cursor);
        }

        let consistency: i16 = self.consistency.into();
        consistency.serialize(cursor);

        let flag_byte = self.query_flags.bits();

        flag_byte.serialize(cursor);

        if let Some(serial_consistency) = self.serial_consistency {
            let serial_consistency: i16 = serial_consistency.into();
            serial_consistency.serialize(cursor);
        }

        if let Some(timestamp) = self.timestamp {
            timestamp.serialize(cursor);
        }
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

/// The structure that represents a query to be batched.
#[derive(Debug, Clone)]
pub struct BatchQuery {
    /// It indicates if a query was prepared.
    pub is_prepared: bool,
    /// It contains either id of prepared query of a query itself.
    pub subject: BatchQuerySubj,
    /// It is the optional name of the following <value_i>. It must be present
    /// if and only if the 0x40 flag is provided for the batch.
    /// **Important note:** this feature does not work and should not be
    /// used. It is specified in a way that makes it impossible for the server
    /// to implement. This will be fixed in a future version of the native
    /// protocol. See <https://issues.apache.org/jira/browse/CASSANDRA-10246> for
    /// more details
    pub values: QueryValues,
}

/// It contains either an id of prepared query or CQL string.
#[derive(Debug, Clone)]
pub enum BatchQuerySubj {
    PreparedId(PreparedQuery),
    QueryString(String),
}

impl Serialize for BatchQuery {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        // kind
        if self.is_prepared {
            1u8.serialize(cursor);
        } else {
            0u8.serialize(cursor);
        }

        match &self.subject {
            BatchQuerySubj::PreparedId(s) => s.id.serialize(cursor),
            BatchQuerySubj::QueryString(s) => serialize_str_long(cursor, s),
        }

        let len = self.values.len() as CIntShort;
        len.serialize(cursor);
        self.values.serialize(cursor);
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_batch(query: BodyReqBatch, flags: Flags, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Batch;

        Frame::new(
            version,
            direction,
            flags,
            opcode,
            query.serialize_to_vec(),
            None,
            None,
        )
    }
}
