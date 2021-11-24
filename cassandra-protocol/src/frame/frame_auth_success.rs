use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;
use crate::types::CBytes;

use super::Serialize;

/// `BodyReqAuthSuccess` is a frame that represents a successful authentication response.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct BodyReqAuthSuccess {
    pub data: CBytes,
}

impl Serialize for BodyReqAuthSuccess {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.data.serialize(cursor);
    }
}

impl FromCursor for BodyReqAuthSuccess {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyReqAuthSuccess> {
        CBytes::from_cursor(cursor).map(|data| BodyReqAuthSuccess { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_req_auth_success() {
        let bytes = &[0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expected = BodyReqAuthSuccess {
            data: CBytes::new(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let body = BodyReqAuthSuccess::from_cursor(&mut cursor).unwrap();
            assert_eq!(
                body.data.into_bytes().unwrap(),
                vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            );
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor);
            assert_eq!(buffer, bytes);
        }
    }
}
