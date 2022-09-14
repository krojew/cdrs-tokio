use crate::error;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use std::io::Cursor;

/// The structure which represents a body of a envelope of type `options`.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone)]
pub struct BodyReqOptions;

impl Serialize for BodyReqOptions {
    #[inline(always)]
    fn serialize(&self, _cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {}
}

impl FromCursor for BodyReqOptions {
    #[inline(always)]
    fn from_cursor(_cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<Self> {
        Ok(BodyReqOptions)
    }
}

impl Envelope {
    /// Creates new envelope of type `options`.
    pub fn new_req_options(version: Version) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Options;
        let body: BodyReqOptions = Default::default();

        Envelope::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            0,
            body.serialize_to_vec(version),
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
        let frame = Envelope::new_req_options(Version::V4);
        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.opcode, Opcode::Options);
        assert!(frame.body.is_empty());
    }
}
