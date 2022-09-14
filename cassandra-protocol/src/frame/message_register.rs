use crate::error;
use crate::frame::events::SimpleServerEvent;
use crate::frame::{Direction, Envelope, Flags, FromCursor, Opcode, Serialize, Version};
use crate::types::{from_cursor_string_list, serialize_str_list};
use derive_more::Constructor;
use itertools::Itertools;
use std::convert::TryFrom;
use std::io::Cursor;

/// The structure which represents a body of a envelope of type `register`.
#[derive(Debug, Constructor, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Clone)]
pub struct BodyReqRegister {
    pub events: Vec<SimpleServerEvent>,
}

impl Serialize for BodyReqRegister {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        let events = self.events.iter().map(|event| event.as_str());
        serialize_str_list(cursor, events, version);
    }
}

impl FromCursor for BodyReqRegister {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<Self> {
        let events = from_cursor_string_list(cursor)?;
        events
            .iter()
            .map(|event| SimpleServerEvent::try_from(event.as_str()))
            .try_collect()
            .map(BodyReqRegister::new)
    }
}

impl Envelope {
    /// Creates new envelope of type `REGISTER`.
    pub fn new_req_register(events: Vec<SimpleServerEvent>, version: Version) -> Envelope {
        let direction = Direction::Request;
        let opcode = Opcode::Register;
        let register_body = BodyReqRegister::new(events);

        Envelope::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            0,
            register_body.serialize_to_vec(version),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::events::SimpleServerEvent;
    use crate::frame::message_register::BodyReqRegister;
    use crate::frame::{FromCursor, Version};
    use std::io::Cursor;

    #[test]
    fn should_deserialize_body() {
        let data = [
            0, 1, 0, 15, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f, 0x43, 0x48, 0x41,
            0x4e, 0x47, 0x45,
        ];
        let mut cursor = Cursor::new(data.as_slice());

        let body = BodyReqRegister::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(body.events, vec![SimpleServerEvent::TopologyChange]);
    }
}
