use super::Serialize;
use crate::error;
use crate::frame::{FromCursor, Version};
use crate::types::{from_cursor_str, from_cursor_string_list, serialize_str, CIntShort, SHORT_LEN};
use std::collections::HashMap;
use std::io::{Cursor, Read};

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct BodyResSupported {
    pub data: HashMap<String, Vec<String>>,
}

impl Serialize for BodyResSupported {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        (self.data.len() as CIntShort).serialize(cursor, version);
        self.data.iter().for_each(|(key, value)| {
            serialize_str(cursor, key.as_str(), version);
            (value.len() as CIntShort).serialize(cursor, version);
            value
                .iter()
                .for_each(|s| serialize_str(cursor, s.as_str(), version));
        })
    }
}

impl FromCursor for BodyResSupported {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        _version: Version,
    ) -> error::Result<BodyResSupported> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let l = i16::from_be_bytes(buff) as usize;
        let mut data: HashMap<String, Vec<String>> = HashMap::with_capacity(l);
        for _ in 0..l {
            let name = from_cursor_str(cursor)?.to_string();
            let val = from_cursor_string_list(cursor)?;
            data.insert(name, val);
        }

        Ok(BodyResSupported { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use crate::frame::Version;
    use std::io::Cursor;

    #[test]
    fn body_res_supported() {
        let bytes = [
            0, 1, // n options
            // 1-st option
            0, 2, 97, 98, // key [string] "ab"
            0, 2, 0, 1, 97, 0, 1, 98, /* value ["a", "b"] */
        ];
        let mut data: HashMap<String, Vec<String>> = HashMap::new();
        data.insert("ab".into(), vec!["a".into(), "b".into()]);
        let expected = BodyResSupported { data };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(&bytes);
            let auth = BodyResSupported::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(auth, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}
