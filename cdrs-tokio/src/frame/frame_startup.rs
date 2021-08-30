use std::collections::HashMap;

use crate::frame::*;
use crate::types::SHORT_LEN;

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

impl<'a> AsBytes for BodyReqStartup<'a> {
    fn as_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(SHORT_LEN);
        // push number of key-value pairs
        let num = self.map.len() as i16;
        v.extend_from_slice(&num.to_be_bytes());
        for (key, val) in self.map.iter() {
            // push key len
            let key_len = key.len() as i16;
            v.extend_from_slice(&key_len.to_be_bytes());
            // push key itself
            v.extend_from_slice(key.as_bytes());
            // push val len
            let val_len = val.len() as i16;
            v.extend_from_slice(&val_len.to_be_bytes());
            // push val itself
            v.extend_from_slice(val.as_bytes());
        }
        v
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `startup`.
    pub fn new_req_startup(compression: Option<&str>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let opcode = Opcode::Startup;
        let body = BodyReqStartup::new(compression);

        Frame::new(version, vec![flag], opcode, body.as_bytes(), None, vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::{Flag, Frame, Opcode, Version};

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
        let frame = Frame::new_req_startup(compression);
        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.flags, vec![Flag::Ignore]);
        assert_eq!(frame.opcode, Opcode::Startup);
        assert_eq!(frame.tracing_id, None);
        assert_eq!(frame.warnings, vec![] as Vec<String>);
    }
}
