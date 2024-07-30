use std::cmp::Eq;
use std::collections::{BTreeMap, HashMap};
use std::convert::Into;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};

use chrono::prelude::*;
use num_bigint::BigInt;
use time::PrimitiveDateTime;
use uuid::Uuid;

use super::blob::Blob;
use super::decimal::Decimal;
use super::duration::Duration;
use super::*;
use crate::Error;

const NULL_INT_VALUE: i32 = -1;
const NOT_SET_INT_VALUE: i32 = -2;

/// Cassandra value which could be an array of bytes, null and non-set values.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub enum Value {
    Some(Vec<u8>),
    Null,
    NotSet,
}

impl Value {
    pub fn new<B>(v: B) -> Value
    where
        B: Into<Bytes>,
    {
        Value::Some(v.into().0)
    }
}

impl Serialize for Value {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            Value::Null => NULL_INT_VALUE.serialize(cursor, version),
            Value::NotSet => NOT_SET_INT_VALUE.serialize(cursor, version),
            Value::Some(value) => {
                let len = value.len() as CInt;
                len.serialize(cursor, version);
                value.serialize(cursor, version);
            }
        }
    }
}

impl FromCursor for Value {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> Result<Value, Error> {
        let value_size = {
            let mut buff = [0; INT_LEN];
            cursor.read_exact(&mut buff)?;
            CInt::from_be_bytes(buff)
        };

        if value_size > 0 {
            Ok(Value::Some(cursor_next_value(cursor, value_size as usize)?))
        } else if value_size == -1 {
            Ok(Value::Null)
        } else if value_size == -2 {
            Ok(Value::NotSet)
        } else {
            Err(Error::General("Could not decode query values".into()))
        }
    }
}

// We are assuming here primitive value serialization will not change across protocol versions,
// which gives us simpler user API.

impl<T: Into<Bytes>> From<T> for Value {
    fn from(b: T) -> Value {
        Value::new(b.into())
    }
}

impl<T: Into<Bytes>> From<Option<T>> for Value {
    fn from(b: Option<T>) -> Value {
        match b {
            Some(b) => Value::new(b.into()),
            None => Value::Null,
        }
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    /// Consumes `Bytes` and returns the inner `Vec<u8>`
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl From<String> for Bytes {
    #[inline]
    fn from(value: String) -> Self {
        Bytes(value.into_bytes())
    }
}

impl From<&str> for Bytes {
    #[inline]
    fn from(value: &str) -> Self {
        Bytes(value.as_bytes().to_vec())
    }
}

impl From<i8> for Bytes {
    #[inline]
    fn from(value: i8) -> Self {
        Bytes(vec![value as u8])
    }
}

impl From<i16> for Bytes {
    #[inline]
    fn from(value: i16) -> Self {
        Bytes(to_short(value))
    }
}

impl From<i32> for Bytes {
    #[inline]
    fn from(value: i32) -> Self {
        Bytes(to_int(value))
    }
}

impl From<i64> for Bytes {
    #[inline]
    fn from(value: i64) -> Self {
        Bytes(to_bigint(value))
    }
}

impl From<u8> for Bytes {
    #[inline]
    fn from(value: u8) -> Self {
        Bytes(vec![value])
    }
}

impl From<u16> for Bytes {
    #[inline]
    fn from(value: u16) -> Self {
        Bytes(to_u_short(value))
    }
}

impl From<u32> for Bytes {
    #[inline]
    fn from(value: u32) -> Self {
        Bytes(to_u_int(value))
    }
}

impl From<u64> for Bytes {
    #[inline]
    fn from(value: u64) -> Self {
        Bytes(to_u_big(value))
    }
}

impl From<NonZeroI8> for Bytes {
    #[inline]
    fn from(value: NonZeroI8) -> Self {
        value.get().into()
    }
}

impl From<NonZeroI16> for Bytes {
    #[inline]
    fn from(value: NonZeroI16) -> Self {
        value.get().into()
    }
}

impl From<NonZeroI32> for Bytes {
    #[inline]
    fn from(value: NonZeroI32) -> Self {
        value.get().into()
    }
}

impl From<NonZeroI64> for Bytes {
    #[inline]
    fn from(value: NonZeroI64) -> Self {
        value.get().into()
    }
}

impl From<bool> for Bytes {
    #[inline]
    fn from(value: bool) -> Self {
        if value {
            Bytes(vec![1])
        } else {
            Bytes(vec![0])
        }
    }
}

impl From<Uuid> for Bytes {
    #[inline]
    fn from(value: Uuid) -> Self {
        Bytes(value.as_bytes().to_vec())
    }
}

impl From<IpAddr> for Bytes {
    #[inline]
    fn from(value: IpAddr) -> Self {
        match value {
            IpAddr::V4(ip) => Bytes(ip.octets().to_vec()),
            IpAddr::V6(ip) => Bytes(ip.octets().to_vec()),
        }
    }
}

impl From<f32> for Bytes {
    #[inline]
    fn from(value: f32) -> Self {
        Bytes(to_float(value))
    }
}

impl From<f64> for Bytes {
    #[inline]
    fn from(value: f64) -> Self {
        Bytes(to_float_big(value))
    }
}

impl From<PrimitiveDateTime> for Bytes {
    #[inline]
    fn from(value: PrimitiveDateTime) -> Self {
        let ts: i64 =
            value.assume_utc().unix_timestamp() * 1_000 + value.nanosecond() as i64 / 1_000_000;
        Bytes(to_bigint(ts))
    }
}

impl From<Blob> for Bytes {
    #[inline]
    fn from(value: Blob) -> Self {
        Bytes(value.into_vec())
    }
}

impl From<Decimal> for Bytes {
    #[inline]
    fn from(value: Decimal) -> Self {
        Bytes(value.serialize_to_vec(Version::V4))
    }
}

impl From<NaiveDateTime> for Bytes {
    #[inline]
    fn from(value: NaiveDateTime) -> Self {
        value.and_utc().timestamp_millis().into()
    }
}

impl From<DateTime<Utc>> for Bytes {
    #[inline]
    fn from(value: DateTime<Utc>) -> Self {
        value.timestamp_millis().into()
    }
}

impl From<Duration> for Bytes {
    #[inline]
    fn from(value: Duration) -> Self {
        Bytes(value.serialize_to_vec(Version::V5))
    }
}

impl<T: Into<Bytes>> From<Vec<T>> for Bytes {
    fn from(vec: Vec<T>) -> Bytes {
        let mut bytes = Vec::with_capacity(INT_LEN);
        let len = vec.len() as CInt;

        bytes.extend_from_slice(&len.to_be_bytes());

        let mut cursor = Cursor::new(&mut bytes);
        cursor.set_position(INT_LEN as u64);

        for v in vec {
            let b: Bytes = v.into();
            Value::new(b).serialize(&mut cursor, Version::V4);
        }

        Bytes(bytes)
    }
}

impl From<BigInt> for Bytes {
    fn from(value: BigInt) -> Self {
        Self(value.serialize_to_vec(Version::V4))
    }
}

impl<K, V> From<HashMap<K, V>> for Bytes
where
    K: Into<Bytes> + Hash + Eq,
    V: Into<Bytes>,
{
    fn from(map: HashMap<K, V>) -> Bytes {
        let mut bytes = Vec::with_capacity(INT_LEN);
        let len = map.len() as CInt;

        bytes.extend_from_slice(&len.to_be_bytes());

        let mut cursor = Cursor::new(&mut bytes);
        cursor.set_position(INT_LEN as u64);

        for (k, v) in map {
            let key_bytes: Bytes = k.into();
            let val_bytes: Bytes = v.into();

            Value::new(key_bytes).serialize(&mut cursor, Version::V4);
            Value::new(val_bytes).serialize(&mut cursor, Version::V4);
        }

        Bytes(bytes)
    }
}

impl<K, V> From<BTreeMap<K, V>> for Bytes
where
    K: Into<Bytes> + Hash + Eq,
    V: Into<Bytes>,
{
    fn from(map: BTreeMap<K, V>) -> Bytes {
        let mut bytes = Vec::with_capacity(INT_LEN);
        let len = map.len() as CInt;

        bytes.extend_from_slice(&len.to_be_bytes());

        let mut cursor = Cursor::new(&mut bytes);
        cursor.set_position(INT_LEN as u64);

        for (k, v) in map {
            let key_bytes: Bytes = k.into();
            let val_bytes: Bytes = v.into();

            Value::new(key_bytes).serialize(&mut cursor, Version::V4);
            Value::new(val_bytes).serialize(&mut cursor, Version::V4);
        }

        Bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_serialization() {
        assert_eq!(
            Value::Some(vec![1]).serialize_to_vec(Version::V4),
            vec![0, 0, 0, 1, 1]
        );

        assert_eq!(
            Value::Some(vec![1, 2, 3]).serialize_to_vec(Version::V4),
            vec![0, 0, 0, 3, 1, 2, 3]
        );

        assert_eq!(
            Value::Null.serialize_to_vec(Version::V4),
            vec![255, 255, 255, 255]
        );
        assert_eq!(
            Value::NotSet.serialize_to_vec(Version::V4),
            vec![255, 255, 255, 254]
        )
    }

    #[test]
    fn test_new_value_all_types() {
        assert_eq!(
            Value::new("hello"),
            Value::Some(vec!(104, 101, 108, 108, 111))
        );
        assert_eq!(
            Value::new("hello".to_string()),
            Value::Some(vec!(104, 101, 108, 108, 111))
        );
        assert_eq!(Value::new(1_u8), Value::Some(vec!(1)));
        assert_eq!(Value::new(1_u16), Value::Some(vec!(0, 1)));
        assert_eq!(Value::new(1_u32), Value::Some(vec!(0, 0, 0, 1)));
        assert_eq!(Value::new(1_u64), Value::Some(vec!(0, 0, 0, 0, 0, 0, 0, 1)));
        assert_eq!(Value::new(1_i8), Value::Some(vec!(1)));
        assert_eq!(Value::new(1_i16), Value::Some(vec!(0, 1)));
        assert_eq!(Value::new(1_i32), Value::Some(vec!(0, 0, 0, 1)));
        assert_eq!(Value::new(1_i64), Value::Some(vec!(0, 0, 0, 0, 0, 0, 0, 1)));
        assert_eq!(Value::new(true), Value::Some(vec!(1)));
        assert_eq!(
            Value::new(Duration::new(100, 200, 300).unwrap()),
            Value::Some(vec!(200, 1, 144, 3, 216, 4))
        );
    }
}
