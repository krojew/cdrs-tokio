use derive_more::Constructor;
use std::io::Cursor;

use crate::frame::*;
use crate::types::CBytes;

#[derive(Debug, Constructor)]
pub struct BodyReqAuthResponse {
    data: CBytes,
}

impl Serialize for BodyReqAuthResponse {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.data.serialize(cursor);
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `AuthResponse`.
    pub fn new_req_auth_response(token_bytes: CBytes, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(token_bytes);

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
    use crate::types::CBytes;

    #[test]
    fn body_req_auth_response() {
        let bytes = CBytes::new(vec![1, 2, 3]);
        let body = BodyReqAuthResponse::new(bytes);
        assert_eq!(body.serialize_to_vec(), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn frame_body_req_auth_response() {
        let bytes = vec![1, 2, 3];
        let frame = Frame::new_req_auth_response(CBytes::new(bytes), Version::V4);

        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.opcode, Opcode::AuthResponse);
        assert_eq!(frame.body, &[0, 0, 0, 3, 1, 2, 3]);
        assert_eq!(frame.tracing_id, None);
        assert!(frame.warnings.is_empty());
    }
}
