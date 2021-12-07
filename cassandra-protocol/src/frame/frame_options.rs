use std::io::Cursor;

use crate::frame::*;

/// The structure which represents a body of a frame of type `options`.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone)]
pub struct BodyReqOptions;

impl Serialize for BodyReqOptions {
    #[inline(always)]
    fn serialize(&self, _cursor: &mut Cursor<&mut Vec<u8>>) {}
}

impl FromCursor for BodyReqOptions {
    #[inline(always)]
    fn from_cursor(_cursor: &mut Cursor<&[u8]>) -> error::Result<Self> {
        Ok(BodyReqOptions)
    }
}

impl Frame {
    /// Creates new frame of type `options`.
    pub fn new_req_options(version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Options;
        let body: BodyReqOptions = Default::default();

        Frame::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_options() {
        let frame = Frame::new_req_options(Version::V4);
        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.opcode, Opcode::Options);
        assert!(frame.body.is_empty());
    }
}
