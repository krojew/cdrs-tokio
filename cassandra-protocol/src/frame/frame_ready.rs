use std::io::Cursor;

use crate::frame::Serialize;

#[derive(Debug, PartialEq, Default)]
pub struct BodyResReady;

impl Serialize for BodyResReady {
    #[inline]
    fn serialize(&self, _cursor: &mut Cursor<&mut Vec<u8>>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_res_ready_new() {
        let body: BodyResReady = Default::default();
        assert_eq!(body, BodyResReady);
    }

    #[test]
    fn body_res_ready_into_cbytes() {
        let body = BodyResReady;
        assert!(body.serialize_to_vec().is_empty());
    }
}
