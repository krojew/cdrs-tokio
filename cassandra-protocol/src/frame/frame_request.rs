use std::io::Cursor;

use crate::error;
use crate::frame::frame_query::BodyReqQuery;
use crate::frame::{FromCursor, Opcode, Serialize};

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RequestBody {
    Startup,
    Options,
    Query(BodyReqQuery),
    Prepare,
    Execute,
    Register,
    Batch,
    AuthResponse,
}

impl Serialize for RequestBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            RequestBody::Query(query) => query.serialize(cursor),
            _ => {
                // TODO: add remaining
                todo!()
            }
        }
    }
}

impl RequestBody {
    pub fn try_from(bytes: &[u8], response_type: Opcode) -> error::Result<RequestBody> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        match response_type {
            Opcode::Startup => Ok(RequestBody::Startup),
            Opcode::Options => Ok(RequestBody::Options),
            Opcode::Query => Ok(RequestBody::Query(BodyReqQuery::from_cursor(&mut cursor)?)),
            Opcode::Prepare => Ok(RequestBody::Prepare),
            Opcode::Execute => Ok(RequestBody::Execute),
            Opcode::Register => Ok(RequestBody::Register),
            Opcode::Batch => Ok(RequestBody::Batch),
            Opcode::AuthResponse => Ok(RequestBody::AuthResponse),
            _ => Err(format!("opcode {} is not a request", response_type).into()),
        }
    }
}
