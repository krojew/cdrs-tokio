use std::io::Cursor;

use crate::frame::*;
use crate::types::*;

/// Struct that represents a body of a frame of type `prepare`
#[derive(Debug)]
pub struct BodyReqPrepare {
    query: String,
}

impl BodyReqPrepare {
    /// Creates new body of a frame of type `prepare` that prepares query `query`.
    pub fn new(query: String) -> BodyReqPrepare {
        BodyReqPrepare { query }
    }
}

impl Serialize for BodyReqPrepare {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        serialize_str_long(cursor, &self.query);
    }
}

impl Frame {
    pub fn new_req_prepare(query: String, flags: Flags, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Prepare;
        let body = BodyReqPrepare::new(query);

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
