use std::io::Cursor;

use crate::frame::message_auth_challenge::BodyResAuthChallenge;
use crate::frame::message_auth_success::BodyReqAuthSuccess;
use crate::frame::message_authenticate::BodyResAuthenticate;
use crate::frame::message_error::ErrorBody;
use crate::frame::message_event::BodyResEvent;
use crate::frame::message_result::{
    BodyResResultPrepared, BodyResResultRows, BodyResResultSetKeyspace, ResResultBody, RowsMetadata,
};
use crate::frame::message_supported::BodyResSupported;
use crate::frame::{FromCursor, Opcode, Version};
use crate::types::rows::Row;
use crate::{error, Error};

#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum ResponseBody {
    Error(ErrorBody),
    Ready,
    Authenticate(BodyResAuthenticate),
    Supported(BodyResSupported),
    Result(ResResultBody),
    Event(BodyResEvent),
    AuthChallenge(BodyResAuthChallenge),
    AuthSuccess(BodyReqAuthSuccess),
}

// This implementation is incomplete so only enable in tests
#[cfg(test)]
use crate::frame::Serialize;
#[cfg(test)]
impl Serialize for ResponseBody {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            ResponseBody::Error(error_body) => {
                error_body.serialize(cursor, version);
            }
            ResponseBody::Ready => {}
            ResponseBody::Authenticate(auth) => {
                auth.serialize(cursor, version);
            }
            ResponseBody::Supported(supported) => {
                supported.serialize(cursor, version);
            }
            ResponseBody::Result(result) => {
                result.serialize(cursor, version);
            }
            ResponseBody::Event(event) => {
                event.serialize(cursor, version);
            }
            ResponseBody::AuthChallenge(auth_challenge) => {
                auth_challenge.serialize(cursor, version);
            }
            ResponseBody::AuthSuccess(auth_success) => {
                auth_success.serialize(cursor, version);
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
            Opcode::Error => ErrorBody::from_cursor(&mut cursor, version).map(ResponseBody::Error),
            Opcode::Ready => Ok(ResponseBody::Ready),
            Opcode::Authenticate => BodyResAuthenticate::from_cursor(&mut cursor, version)
                .map(ResponseBody::Authenticate),
            Opcode::Supported => {
                BodyResSupported::from_cursor(&mut cursor, version).map(ResponseBody::Supported)
            }
            Opcode::Result => {
                ResResultBody::from_cursor(&mut cursor, version).map(ResponseBody::Result)
            }
            Opcode::Event => {
                BodyResEvent::from_cursor(&mut cursor, version).map(ResponseBody::Event)
            }
            Opcode::AuthChallenge => BodyResAuthChallenge::from_cursor(&mut cursor, version)
                .map(ResponseBody::AuthChallenge),
            Opcode::AuthSuccess => {
                BodyReqAuthSuccess::from_cursor(&mut cursor, version).map(ResponseBody::AuthSuccess)
            }
            _ => Err(Error::NonResponseOpcode(response_type)),
        }
    }

    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResponseBody::Result(res) => res.into_rows(),
            _ => None,
        }
    }

    pub fn as_rows_metadata(&self) -> Option<&RowsMetadata> {
        match self {
            ResponseBody::Result(res) => res.as_rows_metadata(),
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
    /// PREPARE query.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResponseBody::Result(res) => res.into_prepared(),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// use keyspace query.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResponseBody::Result(res) => res.into_set_keyspace(),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResEvent.
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

    pub fn into_error(self) -> Option<ErrorBody> {
        match self {
            ResponseBody::Error(err) => Some(err),
            _ => None,
        }
    }
}
