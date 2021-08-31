use std::io::Cursor;

use crate::consistency::Consistency;
use crate::frame::{AsByte, Serialize};
use crate::query::query_flags::QueryFlags;
use crate::query::query_values::QueryValues;
use crate::types::{CBytes, CIntShort};

/// Parameters of Query for query operation.
#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    /// Cassandra consistency level.
    pub consistency: Consistency,
    /// Array of query flags.
    pub flags: Vec<QueryFlags>,
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
}

impl QueryParams {
    /// Sets values of Query request params.
    pub fn set_values(&mut self, values: QueryValues) {
        self.flags.push(QueryFlags::Value);
        self.values = Some(values);
    }

    fn flags_as_byte(&self) -> u8 {
        self.flags.iter().fold(0, |acc, flag| acc | flag.as_byte())
    }

    #[allow(dead_code)]
    fn parse_query_flags(byte: u8) -> Vec<QueryFlags> {
        let mut flags: Vec<QueryFlags> = vec![];

        if QueryFlags::has_value(byte) {
            flags.push(QueryFlags::Value);
        }
        if QueryFlags::has_skip_metadata(byte) {
            flags.push(QueryFlags::SkipMetadata);
        }
        if QueryFlags::has_page_size(byte) {
            flags.push(QueryFlags::PageSize);
        }
        if QueryFlags::has_with_paging_state(byte) {
            flags.push(QueryFlags::WithPagingState);
        }
        if QueryFlags::has_with_serial_consistency(byte) {
            flags.push(QueryFlags::WithSerialConsistency);
        }
        if QueryFlags::has_with_default_timestamp(byte) {
            flags.push(QueryFlags::WithDefaultTimestamp);
        }
        if QueryFlags::has_with_names_for_values(byte) {
            flags.push(QueryFlags::WithNamesForValues);
        }

        flags
    }
}

impl Serialize for QueryParams {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let consistency: CIntShort = self.consistency.into();
        consistency.serialize(cursor);

        let flags = self.flags_as_byte();
        flags.serialize(cursor);

        if QueryFlags::has_value(flags) {
            if let Some(values) = &self.values {
                let len = values.len() as CIntShort;
                len.serialize(cursor);
                values.serialize(cursor);
            }
        }

        if QueryFlags::has_page_size(flags) {
            if let Some(page_size) = self.page_size {
                page_size.serialize(cursor);
            }
        }

        if QueryFlags::has_with_paging_state(flags) {
            if let Some(paging_state) = &self.paging_state {
                paging_state.serialize(cursor);
            }
        }

        if QueryFlags::has_with_serial_consistency(flags) {
            if let Some(serial_consistency) = self.serial_consistency {
                let serial_consistency: CIntShort = serial_consistency.into();
                serial_consistency.serialize(cursor);
            }
        }

        if QueryFlags::has_with_default_timestamp(flags) {
            if let Some(timestamp) = self.timestamp {
                timestamp.serialize(cursor);
            }
        }
    }
}
