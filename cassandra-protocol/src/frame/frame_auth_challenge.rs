use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;
use crate::types::CBytes;

use super::Serialize;

/// Server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthChallenge {
    pub data: CBytes,
}

impl Serialize for BodyResAuthChallenge {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.data.serialize(cursor);
    }
}

impl FromCursor for BodyResAuthChallenge {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResAuthChallenge> {
        CBytes::from_cursor(cursor).map(|data| BodyResAuthChallenge { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_res_auth_challenge_from_cursor() {
        let bytes = &[0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expected = BodyResAuthChallenge {
            data: CBytes::new(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let body = BodyResAuthChallenge::from_cursor(&mut cursor).unwrap();
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
