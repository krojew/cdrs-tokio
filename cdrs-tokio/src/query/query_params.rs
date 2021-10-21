use std::io::Cursor;

use crate::cluster::Murmur3Token;
use crate::consistency::Consistency;
use crate::frame::Serialize;
use crate::query::query_flags::QueryFlags;
use crate::query::query_values::QueryValues;
use crate::types::value::Value;
use crate::types::{CBytes, CIntShort};

/// Parameters of Query for query operation.
#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    /// Cassandra consistency level.
    pub consistency: Consistency,
    /// Query flags.
    pub flags: QueryFlags,
    /// Were values provided with names
    pub with_names: Option<bool>,
    /// Array of values.
    pub values: Option<QueryValues>,
    /// Page size.
    pub page_size: Option<i32>,
    /// Array of bytes which represents paging state.
    pub paging_state: Option<CBytes>,
    /// Serial `Consistency`.
    pub serial_consistency: Option<Consistency>,
    /// Timestamp.
    pub timestamp: Option<i64>,
    /// Is the query idempotent.
    pub is_idempotent: bool,
    /// Query keyspace. If not using a global one, setting it explicitly might help the load
    /// balancer use more appropriate nodes. Note: prepared statements with keyspace information
    /// take precedence over this field.
    pub keyspace: Option<String>,
    /// The token to use for token-aware routing. A load balancer may use this information to
    /// determine which nodes to contact. Takes precedence over `routing_key`.
    pub token: Option<Murmur3Token>,
    /// The partition key to use for token-aware routing. A load balancer may use this information
    /// to determine which nodes to contact. Alternative to `token`. Note: prepared statements
    /// with bound primary key values take precedence over this field.
    pub routing_key: Option<Vec<Value>>,
}

impl QueryParams {
    /// Sets values of Query request params.
    pub fn set_values(&mut self, values: QueryValues) {
        self.flags.insert(QueryFlags::VALUE);
        self.values = Some(values);
    }
}

impl Serialize for QueryParams {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let consistency: CIntShort = self.consistency.into();
        consistency.serialize(cursor);

        let flag_bits = self.flags.bits();
        flag_bits.serialize(cursor);

        if self.flags.contains(QueryFlags::VALUE) {
            if let Some(values) = &self.values {
                let len = values.len() as CIntShort;
                len.serialize(cursor);
                values.serialize(cursor);
            }
        }

        if self.flags.contains(QueryFlags::PAGE_SIZE) {
            if let Some(page_size) = self.page_size {
                page_size.serialize(cursor);
            }
        }

        if self.flags.contains(QueryFlags::WITH_PAGING_STATE) {
            if let Some(paging_state) = &self.paging_state {
                paging_state.serialize(cursor);
            }
        }

        if self.flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            if let Some(serial_consistency) = self.serial_consistency {
                let serial_consistency: CIntShort = serial_consistency.into();
                serial_consistency.serialize(cursor);
            }
        }

        if self.flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            if let Some(timestamp) = self.timestamp {
                timestamp.serialize(cursor);
            }
        }
    }
}
