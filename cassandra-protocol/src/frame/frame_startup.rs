use std::collections::HashMap;
use std::io::Cursor;

use crate::frame::*;
use crate::types::*;

const CQL_VERSION: &str = "CQL_VERSION";
const CQL_VERSION_VAL: &str = "3.0.0";
const COMPRESSION: &str = "COMPRESSION";

#[derive(Debug)]
pub struct BodyReqStartup<'a> {
    pub map: HashMap<&'static str, &'a str>,
}

impl<'a> BodyReqStartup<'a> {
    pub fn new(compression: Option<&str>) -> BodyReqStartup {
        let mut map = HashMap::new();
        map.insert(CQL_VERSION, CQL_VERSION_VAL);
        if let Some(c) = compression {
            map.insert(COMPRESSION, c);
        }
        BodyReqStartup { map }
    }
}

impl<'a> Serialize for BodyReqStartup<'a> {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let num = self.map.len() as CIntShort;
        num.serialize(cursor);

        for (key, val) in &self.map {
            serialize_str(cursor, key);
            serialize_str(cursor, val);
        }
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `startup`.
    pub fn new_req_startup(compression: Option<&str>, version: Version) -> Frame {
        let direction = Direction::Request;
        let opcode = Opcode::Startup;
        let body = BodyReqStartup::new(compression);

        Frame::new(
            version,
            direction,
            Flags::empty(),
            opcode,
            body.serialize_to_vec(),
            None,
            vec![],
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::{Flags, Frame, Opcode, Version};

    #[test]
    fn new_body_req_startup_some_compression() {
        let compression = "test_compression";
        let body = BodyReqStartup::new(Some(compression));
        assert_eq!(body.map.get("CQL_VERSION"), Some(&"3.0.0"));
        assert_eq!(body.map.get("COMPRESSION"), Some(&compression));
        assert_eq!(body.map.len(), 2);
    }

    #[test]
    fn new_body_req_startup_none_compression() {
        let body = BodyReqStartup::new(None);
        assert_eq!(body.map.get("CQL_VERSION"), Some(&"3.0.0"));
        assert_eq!(body.map.len(), 1);
    }

    #[test]
    fn new_req_startup() {
        let compression = Some("test_compression");
        let frame = Frame::new_req_startup(compression, Version::V4);
        assert_eq!(frame.version, Version::V4);
        assert_eq!(frame.flags, Flags::empty());
        assert_eq!(frame.opcode, Opcode::Startup);
        assert_eq!(frame.tracing_id, None);
        assert_eq!(frame.warnings, vec![] as Vec<String>);
    }
}
