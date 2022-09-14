use super::Serialize;
use crate::error;
use crate::frame::{FromCursor, Version};
use crate::types::CBytes;
use std::io::Cursor;

/// Server authentication challenge.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct BodyResAuthChallenge {
    pub data: CBytes,
}

impl Serialize for BodyResAuthChallenge {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.data.serialize(cursor, version);
    }
}

impl FromCursor for BodyResAuthChallenge {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<BodyResAuthChallenge> {
        CBytes::from_cursor(cursor, version).map(|data| BodyResAuthChallenge { data })
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
            let body = BodyResAuthChallenge::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(
                body.data.into_bytes().unwrap(),
                vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            );
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}
