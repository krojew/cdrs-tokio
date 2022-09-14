use crate::error;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::query::PrepareFlags;
use crate::types::{
    from_cursor_str, from_cursor_str_long, serialize_str, serialize_str_long, INT_LEN, SHORT_LEN,
};
use std::io::Cursor;

/// Struct that represents a body of a envelope of type `prepare`
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Default)]
pub struct BodyReqPrepare {
    pub query: String,
    pub keyspace: Option<String>,
}

impl BodyReqPrepare {
    /// Creates new body of a envelope of type `prepare` that prepares query `query`.
    #[inline]
    pub fn new(query: String, keyspace: Option<String>) -> BodyReqPrepare {
        BodyReqPrepare { query, keyspace }
    }
}

impl Serialize for BodyReqPrepare {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str_long(cursor, &self.query, version);

        if version >= Version::V5 {
            if let Some(keyspace) = &self.keyspace {
                PrepareFlags::WITH_KEYSPACE.serialize(cursor, version);
                serialize_str(cursor, keyspace.as_str(), version);
            } else {
                PrepareFlags::empty().serialize(cursor, version);
            }
        }
    }

    #[inline]
    fn serialize_to_vec(&self, version: Version) -> Vec<u8> {
        let mut buf = if version >= Version::V5 {
            Vec::with_capacity(
                INT_LEN * 2
                    + self.query.len()
                    + self
                        .keyspace
                        .as_ref()
                        .map(|keyspace| SHORT_LEN + keyspace.len())
                        .unwrap_or(0),
            )
        } else {
            Vec::with_capacity(INT_LEN + self.query.len())
        };

        self.serialize(&mut Cursor::new(&mut buf), version);
        buf
    }
}

impl FromCursor for BodyReqPrepare {
    #[inline]
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        if version >= Version::V5 {
            from_cursor_str_long(cursor)
                .and_then(|query| {
                    PrepareFlags::from_cursor(cursor, version).map(|flags| (query, flags))
                })
                .and_then(|(query, flags)| {
                    if flags.contains(PrepareFlags::WITH_KEYSPACE) {
                        from_cursor_str(cursor).map(|keyspace| {
                            BodyReqPrepare::new(query.into(), Some(keyspace.into()))
                        })
                    } else {
                        Ok(BodyReqPrepare::new(query.into(), None))
                    }
                })
        } else {
            from_cursor_str_long(cursor).map(|query| BodyReqPrepare::new(query.into(), None))
        }
    }
}

impl Envelope {
    pub fn new_req_prepare(
        query: String,
        keyspace: Option<String>,
        flags: Flags,
        version: Version,
    ) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Prepare;
        let body = BodyReqPrepare::new(query, keyspace);

        Envelope::new(
            version,
            direction,
            flags,
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
    use crate::frame::message_prepare::BodyReqPrepare;
    use crate::frame::{FromCursor, Serialize, Version};
    use std::io::Cursor;

    #[test]
    fn should_deserialize_body() {
        let data = [0, 0, 0, 3, 102, 111, 111, 0];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqPrepare::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(body.query, "foo");
    }

    #[test]
    fn should_support_keyspace() {
        let keyspace = "abc";
        let query = "test";

        let body = BodyReqPrepare::new(query.into(), Some(keyspace.into()));

        let data_v4 = body.serialize_to_vec(Version::V4);
        let body_v4 =
            BodyReqPrepare::from_cursor(&mut Cursor::new(data_v4.as_slice()), Version::V4).unwrap();
        assert_eq!(body_v4.query, query);
        assert_eq!(body_v4.keyspace, None);

        let data_v5 = body.serialize_to_vec(Version::V5);
        let body_v5 =
            BodyReqPrepare::from_cursor(&mut Cursor::new(data_v5.as_slice()), Version::V5).unwrap();
        assert_eq!(body_v5.query, query);
        assert_eq!(body_v5.keyspace, Some(keyspace.to_string()));
    }
}
