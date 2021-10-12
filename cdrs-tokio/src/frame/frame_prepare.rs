use std::io::Cursor;

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

impl Serialize for BodyReqPrepare {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.query.serialize(cursor);
    }
}

impl Frame {
    pub(crate) fn new_req_prepare(query: String, flags: Flags) -> Frame {
        let version = Version::Request;
        let opcode = Opcode::Prepare;
        let body = BodyReqPrepare::new(query);

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
