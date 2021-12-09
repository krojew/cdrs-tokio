use derive_more::Constructor;
use std::io::Cursor;

use crate::frame::*;
use crate::query::QueryParams;
use crate::types::*;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug, Constructor, Eq, PartialEq)]
pub struct BodyReqExecute<'a> {
    id: &'a CBytesShort,
    query_parameters: &'a QueryParams,
}

impl<'a> Serialize for BodyReqExecute<'a> {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.id.serialize(cursor);
        self.query_parameters.serialize(cursor);
    }

    #[inline]
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.id.serialized_len());

        // ignore error, since it can only happen when going over 2^64 bytes size
        let _ = self.serialize(&mut Cursor::new(&mut buf));
        buf
    }
}

/// The structure that represents an owned body of a frame of type `execute`.
#[derive(Debug, Constructor, Clone, Eq, PartialEq, Default)]
pub struct BodyReqExecuteOwned {
    id: CBytesShort,
    query_parameters: QueryParams,
}

impl FromCursor for BodyReqExecuteOwned {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<Self> {
        let id = CBytesShort::from_cursor(cursor)?;
        let query_parameters = QueryParams::from_cursor(cursor)?;

        Ok(BodyReqExecuteOwned::new(id, query_parameters))
    }
}

impl Serialize for BodyReqExecuteOwned {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        BodyReqExecute::new(&self.id, &self.query_parameters).serialize(cursor)
    }
}

impl Frame {
    pub fn new_req_execute(
        id: &CBytesShort,
        query_parameters: &QueryParams,
        flags: Flags,
        version: Version,
    ) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Execute;

        let body = BodyReqExecute::new(id, query_parameters);

        Frame::new(
            version,
            direction,
            flags,
            opcode,
            0,
            body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::consistency::Consistency;
    use crate::frame::frame_execute::BodyReqExecuteOwned;
    use crate::frame::FromCursor;
    use crate::types::CBytesShort;

    #[test]
    fn should_deserialize_body() {
        let data = [0, 1, 2, 0, 0, 0];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqExecuteOwned::from_cursor(&mut cursor).unwrap();
        assert_eq!(body.id, CBytesShort::new(vec![2]));
        assert_eq!(body.query_parameters.consistency, Consistency::Any);
    }
}
