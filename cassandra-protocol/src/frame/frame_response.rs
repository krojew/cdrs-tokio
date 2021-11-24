use std::io::Cursor;

use crate::error;
use crate::frame::frame_auth_challenge::*;
use crate::frame::frame_authenticate::BodyResAuthenticate;
use crate::frame::frame_error::ErrorBody;
use crate::frame::frame_event::BodyResEvent;
use crate::frame::frame_result::{
    BodyResResultPrepared, BodyResResultRows, BodyResResultSetKeyspace, ResResultBody, RowsMetadata,
};
use crate::frame::frame_supported::*;
use crate::frame::{FromCursor, Opcode, Version};
use crate::types::rows::Row;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ResponseBody {
    Error(ErrorBody),
    Ready,
    Authenticate(BodyResAuthenticate),
    Supported(BodyResSupported),
    Result(ResResultBody),
    Event(BodyResEvent),
    AuthChallenge(BodyResAuthChallenge),
    AuthSuccess,
}

// This implementation is incomplete so only enable in tests
#[cfg(test)]
use crate::frame::Serialize;
#[cfg(test)]
impl Serialize for ResponseBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            ResponseBody::Error(error_body) => {
                todo!();
                //error_body.serialize(cursor);
            }
            ResponseBody::Ready | ResponseBody::AuthSuccess => {}
            ResponseBody::Authenticate(auth) => {
                todo!();
                //auth.serialize(cursor);
            }
            ResponseBody::Supported(supported) => {
                todo!();
                //supported.serialize(supported);
            }
            ResponseBody::Result(result) => {
                result.serialize(cursor);
            }
            ResponseBody::Event(event) => {
                event.serialize(cursor);
            }
            ResponseBody::AuthChallenge(auth_challenge) => {
                auth_challenge.serialize(cursor);
            }
        }
    }
}

impl ResponseBody {
    pub fn try_from(
        bytes: &[u8],
        response_type: Opcode,
        version: Version,
    ) -> error::Result<ResponseBody> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        match response_type {
            Opcode::Error => Ok(ResponseBody::Error(ErrorBody::from_cursor(&mut cursor)?)),
            Opcode::Ready => Ok(ResponseBody::Ready),
            Opcode::Authenticate => Ok(ResponseBody::Authenticate(
                BodyResAuthenticate::from_cursor(&mut cursor)?,
            )),
            Opcode::Supported => Ok(ResponseBody::Supported(BodyResSupported::from_cursor(
                &mut cursor,
            )?)),
            Opcode::Result => Ok(ResponseBody::Result(ResResultBody::from_cursor(
                &mut cursor,
                version,
            )?)),
            Opcode::Event => Ok(ResponseBody::Event(BodyResEvent::from_cursor(&mut cursor)?)),
            Opcode::AuthChallenge => Ok(ResponseBody::AuthChallenge(
                BodyResAuthChallenge::from_cursor(&mut cursor)?,
            )),
            Opcode::AuthSuccess => Ok(ResponseBody::AuthSuccess),
            _ => Err(format!("opcode {} is not a response", response_type).into()),
        }
    }

    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResponseBody::Result(res) => res.into_rows(),
            _ => None,
        }
    }

    pub fn as_rows_metadata(&self) -> Option<RowsMetadata> {
        match *self {
            ResponseBody::Result(ref res) => res.as_rows_metadata(),
            _ => None,
        }
    }

    pub fn as_cols(&self) -> Option<&BodyResResultRows> {
        match *self {
            ResponseBody::Result(ResResultBody::Rows(ref rows)) => Some(rows),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// PREPARE query. If frame body is not of type `Result` this method returns `None`.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResponseBody::Result(res) => res.into_prepared(),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// use keyspace query. If frame body is not of type `Result` this method returns `None`.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResponseBody::Result(res) => res.into_set_keyspace(),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResEvent.
    /// If frame body is not of type `Result` this method returns `None`.
    pub fn into_server_event(self) -> Option<BodyResEvent> {
        match self {
            ResponseBody::Event(event) => Some(event),
            _ => None,
        }
    }

    pub fn authenticator(&self) -> Option<&str> {
        match *self {
            ResponseBody::Authenticate(ref auth) => Some(auth.data.as_str()),
            _ => None,
        }
    }
}
