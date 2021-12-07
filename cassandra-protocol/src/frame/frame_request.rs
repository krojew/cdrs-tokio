use std::io::Cursor;

use crate::error;
use crate::frame::frame_auth_response::BodyReqAuthResponse;
use crate::frame::frame_batch::BodyReqBatch;
use crate::frame::frame_execute::BodyReqExecuteOwned;
use crate::frame::frame_options::BodyReqOptions;
use crate::frame::frame_prepare::BodyReqPrepare;
use crate::frame::frame_query::BodyReqQuery;
use crate::frame::frame_register::BodyReqRegister;
use crate::frame::frame_startup::BodyReqStartup;
use crate::frame::{FromCursor, Opcode, Serialize};

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RequestBody {
    Startup(BodyReqStartup),
    Options(BodyReqOptions),
    Query(BodyReqQuery),
    Prepare(BodyReqPrepare),
    Execute(BodyReqExecuteOwned),
    Register(BodyReqRegister),
    Batch(BodyReqBatch),
    AuthResponse(BodyReqAuthResponse),
}

impl Serialize for RequestBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            RequestBody::Query(body) => body.serialize(cursor),
            RequestBody::Startup(body) => body.serialize(cursor),
            RequestBody::Options(body) => body.serialize(cursor),
            RequestBody::Prepare(body) => body.serialize(cursor),
            RequestBody::Execute(body) => body.serialize(cursor),
            RequestBody::Register(body) => body.serialize(cursor),
            RequestBody::Batch(body) => body.serialize(cursor),
            RequestBody::AuthResponse(body) => body.serialize(cursor),
        }
    }
}

impl RequestBody {
    pub fn try_from(bytes: &[u8], response_type: Opcode) -> error::Result<RequestBody> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        match response_type {
            Opcode::Startup => Ok(RequestBody::Startup(BodyReqStartup::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Options => Ok(RequestBody::Options(BodyReqOptions::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Query => Ok(RequestBody::Query(BodyReqQuery::from_cursor(&mut cursor)?)),
            Opcode::Prepare => Ok(RequestBody::Prepare(BodyReqPrepare::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Execute => Ok(RequestBody::Execute(BodyReqExecuteOwned::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Register => Ok(RequestBody::Register(BodyReqRegister::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Batch => Ok(RequestBody::Batch(BodyReqBatch::from_cursor(&mut cursor)?)),
            Opcode::AuthResponse => Ok(RequestBody::AuthResponse(
                BodyReqAuthResponse::from_cursor(&mut cursor)?,
            )),
            _ => Err(format!("opcode {} is not a request", response_type).into()),
        }
    }
}
