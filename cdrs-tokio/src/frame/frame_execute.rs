use log::*;

use crate::frame::*;
use crate::query::QueryParams;
use crate::types::*;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug)]
pub struct BodyReqExecute<'a> {
    /// Id of prepared query
    id: &'a CBytesShort,
    /// Query parameters which have the same meaning as one for `query`
    /// TODO: clarify if it is QueryParams or its shortened variant
    query_parameters: &'a QueryParams,
}

impl<'a> BodyReqExecute<'a> {
    /// The method which creates new instance of `BodyReqExecute`
    pub fn new<'b>(id: &'b CBytesShort, query_parameters: &'b QueryParams) -> BodyReqExecute<'b> {
        BodyReqExecute {
            id,
            query_parameters,
        }
    }
}

impl<'a> AsBytes for BodyReqExecute<'a> {
    fn as_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.id.as_bytes().as_slice());
        v.extend_from_slice(self.query_parameters.as_bytes().as_slice());
        v
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_execute(
        id: &CBytesShort,
        query_parameters: &QueryParams,
        flags: Vec<Flag>,
    ) -> Frame {
        let version = Version::Request;
        let opcode = Opcode::Execute;
        debug!(
            "prepared statement id{:?} getting executed with parameters {:?}",
            id, query_parameters
        );
        let body = BodyReqExecute::new(id, query_parameters);

        Frame::new(version, flags, opcode, body.as_bytes(), None, vec![])
    }
}
