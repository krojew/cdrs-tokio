use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;
use crate::types::{from_cursor_str, serialize_str};

use super::Serialize;

/// A server authentication challenge.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Hash, Clone)]
pub struct BodyResAuthenticate {
    pub data: String,
}

impl Serialize for BodyResAuthenticate {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        serialize_str(cursor, &self.data);
    }
}

impl FromCursor for BodyResAuthenticate {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResAuthenticate> {
        Ok(BodyResAuthenticate {
            data: from_cursor_str(&mut cursor)?.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_res_authenticate() {
        // string "abcde"
        let bytes = [0, 5, 97, 98, 99, 100, 101];
        let expected = BodyResAuthenticate {
            data: "abcde".into(),
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(&bytes);
            let auth = BodyResAuthenticate::from_cursor(&mut cursor).unwrap();
            assert_eq!(auth, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor);
            assert_eq!(buffer, bytes);
        }
    }
}
