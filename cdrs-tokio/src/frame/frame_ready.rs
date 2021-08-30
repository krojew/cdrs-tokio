use std::convert::From;

use crate::frame::AsBytes;

#[derive(Debug, PartialEq, Default)]
pub struct BodyResReady;

impl From<Vec<u8>> for BodyResReady {
    #[inline]
    fn from(_: Vec<u8>) -> BodyResReady {
        BodyResReady {}
    }
}

impl AsBytes for BodyResReady {
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::AsBytes;

    #[test]
    fn body_res_ready_new() {
        let body: BodyResReady = Default::default();
        assert_eq!(body, BodyResReady {});
    }

    #[test]
    fn body_res_ready_into_cbytes() {
        let body = BodyResReady {};
        assert_eq!(body.as_bytes(), vec![] as Vec<u8>);
    }

    #[test]
    fn body_res_ready_from() {
        let body = BodyResReady::from(vec![]);
        assert_eq!(body, BodyResReady {});
    }
}
