use crate::error;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::types::CBytes;
use derive_more::Constructor;
use std::fmt;
use std::io::Cursor;

#[derive(Constructor, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BodyReqAuthResponse {
    pub data: CBytes,
}

// The token carries credentials, e.g. the password in SASL PLAIN, so only its length is shown.
impl fmt::Debug for BodyReqAuthResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BodyReqAuthResponse");
        match self.data.as_slice() {
            Some(token) => debug.field("data", &format_args!("<{} bytes redacted>", token.len())),
            None => debug.field("data", &format_args!("null")),
        };
        debug.finish()
    }
}

impl Serialize for BodyReqAuthResponse {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.data.serialize(cursor, version);
    }
}

impl FromCursor for BodyReqAuthResponse {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        CBytes::from_cursor(cursor, version).map(BodyReqAuthResponse::new)
    }
}

impl Envelope {
    /// Creates new envelope of type `AuthResponse`.
    pub fn new_req_auth_response(token_bytes: CBytes, version: Version) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::AuthResponse;
        let body = BodyReqAuthResponse::new(token_bytes);

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
    use crate::types::CBytes;

    #[test]
    fn body_req_auth_response() {
        let bytes = CBytes::new(vec![1, 2, 3]);
        let body = BodyReqAuthResponse::new(bytes);
        assert_eq!(
            body.serialize_to_vec(Version::V4),
            vec![0, 0, 0, 3, 1, 2, 3]
        );
    }

    #[test]
    fn body_req_auth_response_debug_redacts_token() {
        let password = b"hunter2-secret";
        let token = [b"\0user\0".as_slice(), password].concat();
        let body = BodyReqAuthResponse::new(CBytes::new(token));

        let debug = format!("{:?}", body);
        let password_byte_list = password
            .iter()
            .map(|byte| byte.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        assert_eq!(debug, "BodyReqAuthResponse { data: <20 bytes redacted> }");
        assert!(!debug.contains("hunter2-secret"), "{}", debug);
        assert!(!debug.contains(&password_byte_list), "{}", debug);
    }

    #[test]
    fn frame_body_req_auth_response() {
        let bytes = vec![1, 2, 3];
        let frame = Envelope::new_req_auth_response(CBytes::new(bytes), Version::V4);

        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.opcode, Opcode::AuthResponse);
        assert_eq!(frame.body, &[0, 0, 0, 3, 1, 2, 3]);
        assert_eq!(frame.tracing_id, None);
        assert!(frame.warnings.is_empty());
    }
}
