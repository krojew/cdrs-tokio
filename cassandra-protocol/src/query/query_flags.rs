use bitflags::bitflags;
use std::io::{Cursor, Read};

use crate::error;
use crate::frame::{FromCursor, Serialize, Version};
use crate::types::INT_LEN;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct QueryFlags: u32 {
        /// Indicates that Query Params contain value.
        const VALUE = 0x001;
        /// Indicates that Query Params does not contain metadata.
        const SKIP_METADATA = 0x002;
        /// Indicates that Query Params contain page size.
        const PAGE_SIZE = 0x004;
        /// Indicates that Query Params contain paging state.
        const WITH_PAGING_STATE = 0x008;
        /// Indicates that Query Params contain serial consistency.
        const WITH_SERIAL_CONSISTENCY = 0x010;
        /// Indicates that Query Params contain default timestamp.
        const WITH_DEFAULT_TIMESTAMP = 0x020;
        /// Indicates that Query Params values are named ones.
        const WITH_NAMES_FOR_VALUES = 0x040;
        /// Indicates that Query Params contain keyspace name.
        const WITH_KEYSPACE = 0x080;
        /// Indicates that Query Params contain "now" in seconds.
        const WITH_NOW_IN_SECONDS = 0x100;
    }
}

impl Default for QueryFlags {
    #[inline]
    fn default() -> Self {
        QueryFlags::empty()
    }
}

impl Serialize for QueryFlags {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        if version >= Version::V5 {
            self.bits().serialize(cursor, version);
        } else {
            (self.bits() as u8).serialize(cursor, version);
        }
    }
}

impl FromCursor for QueryFlags {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self> {
        if version >= Version::V5 {
            let mut buff = [0; INT_LEN];
            cursor
                .read_exact(&mut buff)
                .map(|()| QueryFlags::from_bits_truncate(u32::from_be_bytes(buff)))
                .map_err(|error| error.into())
        } else {
            let mut buff = [0];
            cursor.read_exact(&mut buff)?;
            Ok(QueryFlags::from_bits_truncate(buff[0] as u32))
        }
    }
}
