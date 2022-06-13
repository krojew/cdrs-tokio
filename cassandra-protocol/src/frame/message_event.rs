use std::io::Cursor;

use crate::error;
use crate::frame::events::ServerEvent;
use crate::frame::Serialize;
use crate::frame::{FromCursor, Version};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BodyResEvent {
    pub event: ServerEvent,
}

impl Serialize for BodyResEvent {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.event.serialize(cursor, version);
    }
}

impl FromCursor for BodyResEvent {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<BodyResEvent> {
        let event = ServerEvent::from_cursor(cursor, version)?;
        Ok(BodyResEvent { event })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::events::*;
    use crate::frame::traits::FromCursor;
    use crate::types::CInet;
    use std::io::Cursor;

    #[test]
    fn body_res_event() {
        let bytes = &[
            // TOPOLOGY_CHANGE
            0, 15, 84, 79, 80, 79, 76, 79, 71, 89, 95, 67, 72, 65, 78, 71, 69, // NEW_NODE
            0, 8, 78, 69, 87, 95, 78, 79, 68, 69, //
            4, 127, 0, 0, 1, 0, 0, 0, 1, // inet - 127.0.0.1:1
        ];
        let expected = ServerEvent::TopologyChange(TopologyChange {
            change_type: TopologyChangeType::NewNode,
            addr: CInet::new("127.0.0.1:1".parse().unwrap()),
        });

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let event = BodyResEvent::from_cursor(&mut cursor, Version::V4)
                .unwrap()
                .event;
            assert_eq!(event, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}
