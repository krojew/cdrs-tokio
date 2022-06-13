use std::collections::HashMap;
use std::io::Cursor;

use crate::consistency::Consistency;
use crate::frame::traits::FromCursor;
use crate::frame::{Serialize, Version};
use crate::query::query_flags::QueryFlags;
use crate::query::query_values::QueryValues;
use crate::types::{from_cursor_str, serialize_str, value::Value, CInt, CIntShort};
use crate::types::{CBytes, CLong};
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
    pub page_size: Option<CInt>,
    /// Array of bytes which represents paging state.
    pub paging_state: Option<CBytes>,
    /// Serial `Consistency`.
    pub serial_consistency: Option<Consistency>,
    /// Timestamp.
    pub timestamp: Option<CLong>,
    /// Keyspace indicating the keyspace that the query should be executed in. It supersedes the
    /// keyspace that the connection is bound to, if any.
    pub keyspace: Option<String>,
    /// Represents the current time (now) for the query. Affects TTL cell liveness in read queries
    /// and local deletion time for tombstones and TTL cells in update requests.
    pub now_in_seconds: Option<CInt>,
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

        if self.keyspace.is_some() {
            flags.insert(QueryFlags::WITH_KEYSPACE);
        }

        if self.now_in_seconds.is_some() {
            flags.insert(QueryFlags::WITH_NOW_IN_SECONDS);
        }

        flags
    }
}

impl Serialize for QueryParams {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        let consistency: CIntShort = self.consistency.into();
        consistency.serialize(cursor, version);

        let flag_bits = self.flags().bits();
        if version >= Version::V5 {
            flag_bits.serialize(cursor, version);
        } else {
            (flag_bits as u8).serialize(cursor, version);
        };

        if let Some(values) = &self.values {
            let len = values.len() as CIntShort;
            len.serialize(cursor, version);
            values.serialize(cursor, version);
        }

        if let Some(page_size) = self.page_size {
            page_size.serialize(cursor, version);
        }

        if let Some(paging_state) = &self.paging_state {
            paging_state.serialize(cursor, version);
        }

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

impl FromCursor for QueryParams {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> Result<QueryParams, Error> {
        let consistency = Consistency::from_cursor(cursor, version)?;
        let flags = QueryFlags::from_cursor(cursor, version)?;

        let values = if flags.contains(QueryFlags::VALUE) {
            let number_of_values = CIntShort::from_cursor(cursor, version)?;

            if flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES) {
                let mut map = HashMap::with_capacity(number_of_values as usize);
                for _ in 0..number_of_values {
                    map.insert(
                        from_cursor_str(cursor)?.to_string(),
                        Value::from_cursor(cursor, version)?,
                    );
                }
                Some(QueryValues::NamedValues(map))
            } else {
                let mut vec = Vec::with_capacity(number_of_values as usize);
                for _ in 0..number_of_values {
                    vec.push(Value::from_cursor(cursor, version)?);
                }
                Some(QueryValues::SimpleValues(vec))
            }
        } else {
            None
        };

        let page_size = if flags.contains(QueryFlags::PAGE_SIZE) {
            Some(CInt::from_cursor(cursor, version)?)
        } else {
            None
        };

        let paging_state = if flags.contains(QueryFlags::WITH_PAGING_STATE) {
            Some(CBytes::from_cursor(cursor, version)?)
        } else {
            None
        };

        let serial_consistency = if flags.contains(QueryFlags::WITH_SERIAL_CONSISTENCY) {
            Some(Consistency::from_cursor(cursor, version)?)
        } else {
            None
        };

        let timestamp = if flags.contains(QueryFlags::WITH_DEFAULT_TIMESTAMP) {
            Some(CLong::from_cursor(cursor, version)?)
        } else {
            None
        };

        let keyspace = if flags.contains(QueryFlags::WITH_KEYSPACE) {
            Some(from_cursor_str(cursor)?.to_string())
        } else {
            None
        };

        let now_in_seconds = if flags.contains(QueryFlags::WITH_NOW_IN_SECONDS) {
            Some(CInt::from_cursor(cursor, version)?)
        } else {
            None
        };

        let with_names = flags.contains(QueryFlags::WITH_NAMES_FOR_VALUES);

        Ok(QueryParams {
            consistency,
            with_names,
            values,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            keyspace,
            now_in_seconds,
        })
    }
}
