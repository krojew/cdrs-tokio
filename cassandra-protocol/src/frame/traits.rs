use num::BigInt;
use std::io::{Cursor, Write};

use crate::error;
use crate::frame::Version;
use crate::query;

/// Trait that should be implemented by all types that wish to be serialized to a buffer.
pub trait Serialize {
    /// Serializes given value using the cursor.
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version);

    /// Wrapper for easily starting hierarchical serialization.
    fn serialize_to_vec(&self, version: Version) -> Vec<u8> {
        let mut buf = vec![];

        // ignore error, since it can only happen when going over 2^64 bytes size
        let _ = self.serialize(&mut Cursor::new(&mut buf), version);
        buf
    }
}

/// `FromBytes` should be used to parse an array of bytes into a structure.
pub trait FromBytes {
    /// It gets and array of bytes and should return an implementor struct.
    fn from_bytes(bytes: &[u8]) -> error::Result<Self>
    where
        Self: Sized;
}

/// `FromCursor` should be used to get parsed structure from an `io:Cursor`
/// which bound to an array of bytes.
pub trait FromCursor {
    /// Tries to parse Self from a cursor of bytes.
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<Self>
    where
        Self: Sized;
}

/// The trait that allows transformation of `Self` to CDRS query values.
pub trait IntoQueryValues {
    fn into_query_values(self) -> query::QueryValues;
}

pub trait TryFromRow: Sized {
    fn try_from_row(row: crate::types::rows::Row) -> error::Result<Self>;
}

pub trait TryFromUdt: Sized {
    fn try_from_udt(udt: crate::types::udt::Udt) -> error::Result<Self>;
}

impl<const S: usize> Serialize for [u8; S] {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
        let _ = cursor.write(self);
    }
}

impl Serialize for &[u8] {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
        let _ = cursor.write(self);
    }
}

impl Serialize for Vec<u8> {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
        let _ = cursor.write(self);
    }
}

impl Serialize for BigInt {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
        let _ = cursor.write(&self.to_signed_bytes_be());
    }
}

macro_rules! impl_serialized {
    ($t:ty) => {
        impl Serialize for $t {
            #[inline]
            fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
                let _ = cursor.write(&self.to_be_bytes());
            }
        }
    };
}

impl_serialized!(i8);
impl_serialized!(i16);
impl_serialized!(i32);
impl_serialized!(i64);
impl_serialized!(u8);
impl_serialized!(u16);
impl_serialized!(u32);
impl_serialized!(u64);
