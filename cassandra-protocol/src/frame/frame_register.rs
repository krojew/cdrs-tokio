use std::io::Cursor;

use crate::frame::events::SimpleServerEvent;
use crate::frame::*;
use crate::types::CStringList;

/// The structure which represents a body of a frame of type `options`.
pub struct BodyReqRegister {
    pub events: Vec<SimpleServerEvent>,
}

impl Serialize for BodyReqRegister {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let events_string_list = CStringList {
            list: self.events.iter().map(|event| event.as_string()).collect(),
        };

        events_string_list.serialize(cursor)
    }
}

// Frame implementation related to BodyReqRegister

impl Frame {
    /// Creates new frame of type `REGISTER`.
    pub fn new_req_register(events: Vec<SimpleServerEvent>, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Register;
        let register_body = BodyReqRegister { events };

        Frame::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            register_body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}
