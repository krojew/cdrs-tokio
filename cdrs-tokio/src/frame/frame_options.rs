use std::io::Cursor;

use crate::frame::*;

/// The structure which represents a body of a frame of type `options`.
#[derive(Debug, Default)]
pub struct BodyReqOptions;

impl Serialize for BodyReqOptions {
    #[inline]
    fn serialize(&self, _cursor: &mut Cursor<&mut Vec<u8>>) {}
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `options`.
    pub fn new_req_options() -> Frame {
        let version = Version::Request;
        let opcode = Opcode::Options;
        let body: BodyReqOptions = Default::default();

        Frame::new(
            version,
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
        let frame = Frame::new_req_options();
        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.opcode, Opcode::Options);
        assert!(frame.body.is_empty());
    }
}
