use crate::consistency::Consistency;
use crate::error;
use crate::frame::traits::FromCursor;
use crate::frame::{Direction, Envelope, Flags, Opcode, Serialize, Version};
use crate::query::{QueryParams, QueryValues};
use crate::types::{from_cursor_str_long, serialize_str_long, CBytes, CInt, CLong, INT_LEN};
use std::io::Cursor;

/// Structure which represents body of Query request
#[derive(Debug, PartialEq, Eq, Clone, Default)]
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
        page_size: Option<CInt>,
        paging_state: Option<CBytes>,
        serial_consistency: Option<Consistency>,
        timestamp: Option<CLong>,
        keyspace: Option<String>,
        now_in_seconds: Option<CInt>,
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
                keyspace,
                now_in_seconds,
            },
        }
    }
}

impl FromCursor for BodyReqQuery {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<BodyReqQuery> {
        let query = from_cursor_str_long(cursor)?.to_string();
        let query_params = QueryParams::from_cursor(cursor, version)?;

        Ok(BodyReqQuery {
            query,
            query_params,
        })
    }
}

impl Serialize for BodyReqQuery {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str_long(cursor, &self.query, version);
        self.query_params.serialize(cursor, version);
    }

    #[inline]
    fn serialize_to_vec(&self, version: Version) -> Vec<u8> {
        let mut buf = Vec::with_capacity(INT_LEN + self.query.len());

        self.serialize(&mut Cursor::new(&mut buf), version);
        buf
    }
}

impl Envelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new_req_query(
        query: String,
        consistency: Consistency,
        values: Option<QueryValues>,
        with_names: bool,
        page_size: Option<CInt>,
        paging_state: Option<CBytes>,
        serial_consistency: Option<Consistency>,
        timestamp: Option<CLong>,
        keyspace: Option<String>,
        now_in_seconds: Option<CInt>,
        flags: Flags,
        version: Version,
    ) -> Envelope {
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
            keyspace,
            now_in_seconds,
        );

        Envelope::new(
            version,
            direction,
            flags,
            opcode,
            0,
            body.serialize_to_vec(version),
            None,
            vec![],
        )
    }

    #[inline]
    pub fn new_query(query: BodyReqQuery, flags: Flags, version: Version) -> Envelope {
        Envelope::new_req_query(
            query.query,
            query.query_params.consistency,
            query.query_params.values,
            query.query_params.with_names,
            query.query_params.page_size,
            query.query_params.paging_state,
            query.query_params.serial_consistency,
            query.query_params.timestamp,
            query.query_params.keyspace,
            query.query_params.now_in_seconds,
            flags,
            version,
        )
    }
}
