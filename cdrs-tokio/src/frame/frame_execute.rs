use log::*;

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

impl<'a> AsBytes for BodyReqExecute<'a> {
    fn as_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.id.serialized_len());
        v.append(&mut self.id.as_bytes());
        v.append(&mut self.query_parameters.as_bytes());
        v
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

        trace!(
            "prepared statement id{:?} getting executed with parameters {:?}",
            id,
            query_parameters
        );

        let body = BodyReqExecute::new(id, query_parameters);

        Frame::new(version, flags, opcode, body.as_bytes(), None, vec![])
    }
}
