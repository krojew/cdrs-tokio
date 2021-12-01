use derive_more::Constructor;
use std::io::Cursor;

use crate::frame::*;
use crate::query::QueryParams;
use crate::types::*;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug, Constructor)]
pub struct BodyReqExecute<'a> {
    id: &'a CBytesShort,
    query_parameters: &'a QueryParams,
}

impl<'a> Serialize for BodyReqExecute<'a> {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.id.serialize(cursor);
        self.query_parameters.serialize(cursor);
    }

    #[inline]
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.id.serialized_len());

        // ignore error, since it can only happen when going over 2^64 bytes size
        let _ = self.serialize(&mut Cursor::new(&mut buf));
        buf
    }
}

impl Frame {
    pub fn new_req_execute(
        id: &CBytesShort,
        query_parameters: &QueryParams,
        flags: Flags,
        version: Version,
    ) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Execute;

        let body = BodyReqExecute::new(id, query_parameters);

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
}
