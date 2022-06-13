use bitflags::bitflags;
use std::io::{Cursor, Read};

use crate::error::Result;
use crate::frame::{FromCursor, Serialize, Version};
use crate::types::INT_LEN;

bitflags! {
    pub struct PrepareFlags: u32 {
        /// The prepare request contains explicit keyspace.
        const WITH_KEYSPACE = 0x01;
    }
}

impl Default for PrepareFlags {
    #[inline]
    fn default() -> Self {
        PrepareFlags::empty()
    }
}

impl Serialize for PrepareFlags {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.bits().serialize(cursor, version);
    }
}

impl FromCursor for PrepareFlags {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> Result<Self> {
        let mut buff = [0; INT_LEN];
        cursor
            .read_exact(&mut buff)
            .map(|()| PrepareFlags::from_bits_truncate(u32::from_be_bytes(buff)))
            .map_err(|error| error.into())
    }
}
