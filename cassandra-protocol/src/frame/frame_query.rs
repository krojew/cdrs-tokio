use crate::consistency::Consistency;
use crate::frame::traits::FromCursor;
use crate::frame::*;
use crate::query::{Query, QueryParams, QueryValues};
use crate::types::*;
use std::io::Cursor;

/// Structure which represents body of Query request
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BodyReqQuery {
    /// Query string.
    pub query: String,
    /// Query parameters.
    pub query_params: QueryParams,
}

impl BodyReqQuery {
    #[allow(clippy::too_many_arguments)]
    fn new(
        query: String,
        consistency: Consistency,
        values: Option<QueryValues>,
        with_names: bool,
        page_size: Option<i32>,
        paging_state: Option<CBytes>,
        serial_consistency: Option<Consistency>,
        timestamp: Option<i64>,
    ) -> BodyReqQuery {
        BodyReqQuery {
            query,
            query_params: QueryParams {
                consistency,
                with_names,
                values,
                page_size,
                paging_state,
                serial_consistency,
                timestamp,
            },
        }
    }
}

impl FromCursor for BodyReqQuery {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyReqQuery> {
        let query = from_cursor_str_long(cursor)?.to_string();
        let query_params = QueryParams::from_cursor(cursor)?;

        Ok(BodyReqQuery {
            query,
            query_params,
        })
    }
}

impl Serialize for BodyReqQuery {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        serialize_str_long(cursor, &self.query);
        self.query_params.serialize(cursor);
    }

    #[inline]
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(INT_LEN + self.query.len());

        // ignore error, since it can only happen when going over 2^64 bytes size
        let _ = self.serialize(&mut Cursor::new(&mut buf));
        buf
    }
}

impl Frame {
    #[allow(clippy::too_many_arguments)]
    pub fn new_req_query(
        query: String,
        consistency: Consistency,
        values: Option<QueryValues>,
        with_names: bool,
        page_size: Option<i32>,
        paging_state: Option<CBytes>,
        serial_consistency: Option<Consistency>,
        timestamp: Option<i64>,
        flags: Flags,
        version: Version,
    ) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Query;
        let body = BodyReqQuery::new(
            query,
            consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
        );

        Frame::new(
            version,
            direction,
            flags,
            opcode,
            body.serialize_to_vec(),
            None,
            vec![],
        )
    }

    #[inline]
    pub fn new_query(query: Query, flags: Flags, version: Version) -> Frame {
        Frame::new_req_query(
            query.query,
            query.params.consistency,
            query.params.values,
            query.params.with_names,
            query.params.page_size,
            query.params.paging_state,
            query.params.serial_consistency,
            query.params.timestamp,
            flags,
            version,
        )
    }
}
