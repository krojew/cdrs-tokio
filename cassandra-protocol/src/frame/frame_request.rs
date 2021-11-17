use std::io::Cursor;

use crate::error;
use crate::frame::frame_query::BodyReqQuery;
use crate::frame::{FromCursor, Opcode};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RequestBody {
    Query(BodyReqQuery),
}

use crate::frame::Serialize;
impl Serialize for RequestBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            RequestBody::Query(query) => query.serialize(cursor),
        }
    }
}

impl RequestBody {
    pub fn try_from(bytes: &[u8], response_type: Opcode) -> error::Result<RequestBody> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        match response_type {
            Opcode::Query => Ok(RequestBody::Query(BodyReqQuery::from_cursor(&mut cursor)?)),
            _ => Err(format!("opcode {} is not a request", response_type).into()),
        }
    }
}
