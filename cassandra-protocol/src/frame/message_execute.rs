use derive_more::Constructor;
use std::io::Cursor;

use crate::error;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::query::QueryParams;
use crate::types::CBytesShort;

/// The structure that represents a body of a envelope of type `execute`.
#[derive(Debug, Constructor, Eq, PartialEq)]
pub struct BodyReqExecute<'a> {
    pub id: &'a CBytesShort,
    pub result_metadata_id: Option<&'a CBytesShort>,
    pub query_parameters: &'a QueryParams,
}

impl<'a> Serialize for BodyReqExecute<'a> {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.id.serialize(cursor, version);

        if let Some(result_metadata_id) = self.result_metadata_id {
            result_metadata_id.serialize(cursor, version);
        }

        self.query_parameters.serialize(cursor, version);
    }

    #[inline]
    fn serialize_to_vec(&self, version: Version) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            self.id.serialized_len()
                + self
                    .result_metadata_id
                    .map(|id| id.serialized_len())
                    .unwrap_or(0),
        );

        self.serialize(&mut Cursor::new(&mut buf), version);
        buf
    }
}

/// The structure that represents an owned body of a envelope of type `execute`.
#[derive(Debug, Constructor, Clone, Eq, PartialEq, Default)]
pub struct BodyReqExecuteOwned {
    pub id: CBytesShort,
    pub result_metadata_id: Option<CBytesShort>,
    pub query_parameters: QueryParams,
}

impl FromCursor for BodyReqExecuteOwned {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        let id = CBytesShort::from_cursor(cursor, version)?;

        let result_metadata_id = if version >= Version::V5 {
            Some(CBytesShort::from_cursor(cursor, version)?)
        } else {
            None
        };

        let query_parameters = QueryParams::from_cursor(cursor, version)?;

        Ok(BodyReqExecuteOwned::new(
            id,
            result_metadata_id,
            query_parameters,
        ))
    }
}

impl Serialize for BodyReqExecuteOwned {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        BodyReqExecute::new(
            &self.id,
            self.result_metadata_id.as_ref(),
            &self.query_parameters,
        )
        .serialize(cursor, version);
    }
}

impl Envelope {
    pub fn new_req_execute(
        id: &CBytesShort,
        result_metadata_id: Option<&CBytesShort>, // only required for protocol >= V5
        query_parameters: &QueryParams,
        flags: Flags,
        version: Version,
    ) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Execute;

        let body = BodyReqExecute::new(id, result_metadata_id, query_parameters);

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
    use std::io::Cursor;

    use crate::consistency::Consistency;
    use crate::frame::message_execute::BodyReqExecuteOwned;
    use crate::frame::traits::Serialize;
    use crate::frame::{FromCursor, Version};
    use crate::query::QueryParams;
    use crate::types::CBytesShort;

    #[test]
    fn should_deserialize_body() {
        let data = [0, 1, 2, 0, 0, 0];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqExecuteOwned::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(body.id, CBytesShort::new(vec![2]));
        assert_eq!(body.query_parameters.consistency, Consistency::Any);
    }

    #[test]
    fn should_support_result_metadata_id() {
        let body = BodyReqExecuteOwned::new(
            CBytesShort::new(vec![1]),
            Some(CBytesShort::new(vec![2])),
            QueryParams::default(),
        );
        let data = body.serialize_to_vec(Version::V5);
        assert_eq!(
            BodyReqExecuteOwned::from_cursor(&mut Cursor::new(&data), Version::V5).unwrap(),
            body
        );
    }
}
