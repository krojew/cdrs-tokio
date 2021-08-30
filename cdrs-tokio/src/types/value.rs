use std::cmp::Eq;
use std::collections::HashMap;
use std::convert::Into;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};

use chrono::prelude::*;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::frame::AsBytes;

use super::blob::Blob;
use super::decimal::Decimal;
use super::*;

const NULL_INT_VALUE: i32 = -1;
const NOT_SET_INT_VALUE: i32 = -2;

/// Types of Cassandra value: normal value (bits), null value and not-set value
#[derive(Debug, Clone, PartialEq, Copy, Ord, PartialOrd, Eq, Hash)]
pub enum ValueType {
    Normal(i32),
    Null,
    NotSet,
}

impl AsBytes for ValueType {
    fn as_bytes(&self) -> Vec<u8> {
        match *self {
            ValueType::Normal(n) => to_int(n),
            ValueType::Null => to_int(NULL_INT_VALUE),
            ValueType::NotSet => to_int(NOT_SET_INT_VALUE),
        }
    }
}

impl From<ValueType> for i32 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Normal(value) => value,
            ValueType::Null => NULL_INT_VALUE,
            ValueType::NotSet => NOT_SET_INT_VALUE,
        }
    }
}

/// Cassandra value which could be an array of bytes, null and non-set values.
#[derive(Debug, Clone, PartialEq)]
pub struct Value {
    pub body: Vec<u8>,
    pub value_type: ValueType,
}

impl Value {
    /// The factory method which creates a normal type value basing on provided bytes.
    pub fn new_normal<B>(v: B) -> Value
    where
        B: Into<Bytes>,
    {
        let bytes = v.into().0;
        let l = bytes.len() as i32;
        Value {
            body: bytes,
            value_type: ValueType::Normal(l),
        }
    }

    /// The factory method which creates null Cassandra value.
    pub fn new_null() -> Value {
        Value {
            body: vec![],
            value_type: ValueType::Null,
        }
    }

    /// The factory method which creates non-set Cassandra value.
    pub fn new_not_set() -> Value {
        Value {
            body: vec![],
            value_type: ValueType::NotSet,
        }
    }
}

impl AsBytes for Value {
    fn as_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(INT_LEN + self.body.len());
        let value_int: i32 = self.value_type.into();
        v.extend_from_slice(&value_int.to_be_bytes());
        if let ValueType::Normal(_) = &self.value_type {
            v.extend_from_slice(self.body.as_slice());
        }

        v
    }
}

impl<T: Into<Bytes>> From<T> for Value {
    fn from(b: T) -> Value {
        Value::new_normal(b.into())
    }
}

impl<T: Into<Bytes>> From<Option<T>> for Value {
    fn from(b: Option<T>) -> Value {
        match b {
            Some(b) => Value::new_normal(b.into()),
            None => Value::new_null(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    pub fn new(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes)
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
        Bytes(value.as_bytes())
    }
}

impl From<NaiveDateTime> for Bytes {
    #[inline]
    fn from(value: NaiveDateTime) -> Self {
        value.timestamp_millis().into()
    }
}

impl From<DateTime<Utc>> for Bytes {
    #[inline]
    fn from(value: DateTime<Utc>) -> Self {
        value.timestamp_millis().into()
    }
}

impl<T: Into<Bytes> + Clone + Debug> From<Vec<T>> for Bytes {
    fn from(vec: Vec<T>) -> Bytes {
        let mut bytes: Vec<u8> = vec![];
        bytes.extend_from_slice(to_int(vec.len() as i32).as_slice());
        bytes = vec.iter().fold(bytes, |mut acc, v| {
            let b: Bytes = v.clone().into();
            acc.extend_from_slice(Value::new_normal(b).as_bytes().as_slice());
            acc
        });
        Bytes(bytes)
    }
}

impl<K, V> From<HashMap<K, V>> for Bytes
where
    K: Into<Bytes> + Clone + Debug + Hash + Eq,
    V: Into<Bytes> + Clone + Debug,
{
    fn from(map: HashMap<K, V>) -> Bytes {
        let mut bytes: Vec<u8> = vec![];
        bytes.extend_from_slice(to_int(map.len() as i32).as_slice());
        bytes = map.iter().fold(bytes, |mut acc, (k, v)| {
            let key_bytes: Bytes = k.clone().into();
            let val_bytes: Bytes = v.clone().into();
            acc.extend_from_slice(Value::new_normal(key_bytes).as_bytes().as_slice());
            acc.extend_from_slice(Value::new_normal(val_bytes).as_bytes().as_slice());
            acc
        });
        Bytes(bytes)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::frame::traits::AsBytes;

    #[test]
    fn test_value_type_into_cbytes() {
        // normal value types
        let normal_type = ValueType::Normal(1);
        assert_eq!(normal_type.as_bytes(), vec![0, 0, 0, 1]);
        // null value types
        let null_type = ValueType::Null;
        assert_eq!(null_type.as_bytes(), vec![255, 255, 255, 255]);
        // not set value types
        let not_set = ValueType::NotSet;
        assert_eq!(not_set.as_bytes(), vec![255, 255, 255, 254])
    }

    #[test]
    fn test_new_normal_value() {
        let plain_value = "hello";
        let len = plain_value.len() as i32;
        let normal_value = Value::new_normal(plain_value);
        assert_eq!(normal_value.body, b"hello");
        match normal_value.value_type {
            ValueType::Normal(l) => assert_eq!(l, len),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_new_normal_value_all_types() {
        let _ = Value::new_normal("hello");
        let _ = Value::new_normal("hello".to_string());
        let _ = Value::new_normal(1_u8);
        let _ = Value::new_normal(1_u16);
        let _ = Value::new_normal(1_u32);
        let _ = Value::new_normal(1_u64);
        let _ = Value::new_normal(1_i8);
        let _ = Value::new_normal(1_i16);
        let _ = Value::new_normal(1_i32);
        let _ = Value::new_normal(1_i64);
        let _ = Value::new_normal(true);
    }

    #[test]
    fn test_new_null_value() {
        let null_value = Value::new_null();
        assert_eq!(null_value.body, vec![]);
        assert_eq!(null_value.value_type, ValueType::Null);
    }

    #[test]
    fn test_new_not_set_value() {
        let not_set_value = Value::new_not_set();
        assert_eq!(not_set_value.body, vec![]);
        assert_eq!(not_set_value.value_type, ValueType::NotSet);
    }

    #[test]
    fn test_value_into_cbytes() {
        let value = Value::new_normal(1_u8);
        assert_eq!(value.as_bytes(), vec![0, 0, 0, 1, 1]);
    }
}
