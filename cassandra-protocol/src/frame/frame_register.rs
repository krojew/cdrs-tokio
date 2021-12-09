use derive_more::Constructor;
use itertools::Itertools;
use std::io::Cursor;

use crate::frame::events::SimpleServerEvent;
use crate::frame::*;
use crate::types::serialize_str_list;

/// The structure which represents a body of a frame of type `register`.
#[derive(Debug, Constructor, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Clone)]
pub struct BodyReqRegister {
    pub events: Vec<SimpleServerEvent>,
}

impl Serialize for BodyReqRegister {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let events = self.events.iter().map(|event| event.as_str());
        serialize_str_list(cursor, events);
    }
}

impl FromCursor for BodyReqRegister {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<Self> {
        let events = from_cursor_string_list(cursor)?;
        events
            .iter()
            .map(|event| SimpleServerEvent::try_from(event.as_str()))
            .try_collect()
            .map(BodyReqRegister::new)
    }
}

impl Frame {
    /// Creates new frame of type `REGISTER`.
    pub fn new_req_register(events: Vec<SimpleServerEvent>, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Register;
        let register_body = BodyReqRegister::new(events);

        Frame::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            0,
            register_body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::events::SimpleServerEvent;
    use crate::frame::frame_register::BodyReqRegister;
    use crate::frame::FromCursor;

    #[test]
    fn should_deserialize_body() {
        let data = [
            0, 1, 0, 15, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f, 0x43, 0x48, 0x41,
            0x4e, 0x47, 0x45,
        ];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqRegister::from_cursor(&mut cursor).unwrap();
        assert_eq!(body.events, vec![SimpleServerEvent::TopologyChange]);
    }
}
