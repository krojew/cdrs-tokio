use crate::frame::*;
use crate::types::*;

/// Struct that represents a body of a frame of type `prepare`
#[derive(Debug)]
pub struct BodyReqPrepare {
    query: CStringLong,
}

impl BodyReqPrepare {
    /// Creates new body of a frame of type `prepare` that prepares query `query`.
    pub fn new(query: String) -> BodyReqPrepare {
        BodyReqPrepare {
            query: CStringLong::new(query),
        }
    }
}

impl AsBytes for BodyReqPrepare {
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        self.query.as_bytes()
    }
}

impl Frame {
    pub(crate) fn new_req_prepare(query: String, flags: Vec<Flag>) -> Frame {
        let version = Version::Request;
        let opcode = Opcode::Prepare;
        let body = BodyReqPrepare::new(query);

        Frame::new(version, flags, opcode, body.as_bytes(), None, vec![])
    }
}
