use std::io::Cursor;

use crate::frame::*;
use crate::query::QueryParams;
use crate::types::*;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug)]
pub struct BodyReqExecute<'a> {
    id: &'a CBytesShort,
    query_parameters: &'a QueryParams,
}

impl<'a> BodyReqExecute<'a> {
    pub fn new<'b>(id: &'b CBytesShort, query_parameters: &'b QueryParams) -> BodyReqExecute<'b> {
        BodyReqExecute {
            id,
            query_parameters,
        }
    }
}

impl<'a> Serialize for BodyReqExecute<'a> {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.id.serialize(cursor);
        self.query_parameters.serialize(cursor);
    }
}

impl Frame {
    pub(crate) fn new_req_execute(
        id: &CBytesShort,
        query_parameters: &QueryParams,
        flags: Vec<Flag>,
    ) -> Frame {
        let version = Version::Request;
        let opcode = Opcode::Execute;

        let body = BodyReqExecute::new(id, query_parameters);

        Frame::new(
            version,
            flags,
            opcode,
            body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}
