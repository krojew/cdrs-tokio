use derive_more::Constructor;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Read};

use crate::consistency::Consistency;
use crate::frame::traits::FromCursor;
use crate::frame::Serialize;
use crate::query::query_flags::QueryFlags;
use crate::query::query_values::QueryValues;
use crate::types::CBytes;
use crate::types::{from_cursor_str, value::Value, CIntShort};
use crate::Error;

/// Parameters of Query for query operation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QueryParams {
    /// Cassandra consistency level.
    pub consistency: Consistency,
    /// Were values provided with names
    pub with_names: bool,
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
    fn flags(&self) -> QueryFlags {
        let mut flags = QueryFlags::empty();

        if self.values.is_some() {
            flags.insert(QueryFlags::VALUE);
        }

        if self.with_names {
            flags.insert(QueryFlags::WITH_NAMES_FOR_VALUES);
        }

        if self.page_size.is_some() {
            flags.insert(QueryFlags::PAGE_SIZE);
        }

        if self.paging_state.is_some() {
            flags.insert(QueryFlags::WITH_PAGING_STATE);
        }

        if self.serial_consistency.is_some() {
            flags.insert(QueryFlags::WITH_SERIAL_CONSISTENCY);
        }

        if self.timestamp.is_some() {
            flags.insert(QueryFlags::WITH_DEFAULT_TIMESTAMP);
        }

        flags
    }
}

impl Serialize for QueryParams {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let consistency: CIntShort = self.consistency.into();
        consistency.serialize(cursor);

        let flag_bits = self.flags().bits();
        flag_bits.serialize(cursor);

        if let Some(values) = &self.values {
            let len = values.len() as CIntShort;
            len.serialize(cursor);
            values.serialize(cursor);
        }

        if let Some(page_size) = self.page_size {
            page_size.serialize(cursor);
        }

        if let Some(paging_state) = &self.paging_state {
            paging_state.serialize(cursor);
        }

        if let Some(serial_consistency) = self.serial_consistency {
            let serial_consistency: CIntShort = serial_consistency.into();
            serial_consistency.serialize(cursor);
        }

        if let Some(timestamp) = self.timestamp {
            timestamp.serialize(cursor);
        }
    }
}

impl FromCursor for QueryParams {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<QueryParams, Error> {
        let consistency = Consistency::from_cursor(cursor)?;
        let flags = {
            let mut buff = [0];
            cursor.read_exact(&mut buff)?;
            QueryFlags::from_bits_truncate(buff[0])
        };

        let values = if flags.contains(QueryFlags::VALUE) {
            let number_of_values = {
                let mut buff = [0; 2];
                cursor.read_exact(&mut buff)?;
                i16::from_be_bytes(buff)
            };
            if flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES) {
                let mut map = HashMap::with_capacity(number_of_values as usize);
                for _ in 0..number_of_values {
                    map.insert(
                        from_cursor_str(cursor)?.to_string(),
                        Value::from_cursor(cursor)?,
                    );
                }
                Some(QueryValues::NamedValues(map))
            } else {
                let mut vec = Vec::with_capacity(number_of_values as usize);
                for _ in 0..number_of_values {
                    vec.push(Value::from_cursor(cursor)?);
                }
                Some(QueryValues::SimpleValues(vec))
            }
        } else {
            None
        };

        let page_size = if flags.contains(QueryFlags::PAGE_SIZE) {
            Some({
                let mut buff = [0; 4];
                cursor.read_exact(&mut buff)?;
                i32::from_be_bytes(buff)
            })
        } else {
            None
        };

        let paging_state = if flags.contains(QueryFlags::WITH_PAGING_STATE) {
            Some(CBytes::from_cursor(cursor)?)
        } else {
            None
        };

        let serial_consistency = if flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            Some(Consistency::from_cursor(cursor)?)
        } else {
            None
        };

        let timestamp = if flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            Some({
                let mut buff = [0; 8];
                cursor.read_exact(&mut buff)?;
                i64::from_be_bytes(buff)
            })
        } else {
            None
        };

        let with_names = flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES);

        // We set these to default values as they arent actually part of the cassandra protocol
        let is_idempotent = false;
        let keyspace = None;
        let token = None;
        let routing_key = None;

        Ok(QueryParams {
            consistency,
            with_names,
            values,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            is_idempotent,
            keyspace,
            token,
            routing_key,
        })
    }
}

/// A token on the ring. Only Murmur3 tokens are supported for now.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Constructor)]
pub struct Murmur3Token {
    pub value: i64,
}

impl TryFrom<String> for Murmur3Token {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse()
            .map_err(|error| format!("Error parsing token: {}", error).into())
            .map(Murmur3Token::new)
    }
}

impl From<i64> for Murmur3Token {
    fn from(value: i64) -> Self {
        Murmur3Token::new(value)
    }
}
