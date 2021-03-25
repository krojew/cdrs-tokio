use std::cmp::Eq;
use std::collections::HashMap;
use std::convert::Into;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};

use chrono::prelude::*;
use uuid::Uuid;

use crate::frame::AsBytes;
use crate::time::PrimitiveDateTime;

use super::blob::Blob;
use super::decimal::Decimal;
use super::*;

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
            ValueType::Null => i_to_n_bytes(-1, INT_LEN),
            ValueType::NotSet => i_to_n_bytes(-2, INT_LEN),
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
        v.extend_from_slice(self.value_type.as_bytes().as_slice());
        v.extend_from_slice(self.body.as_slice());
        v
    }
}

impl<T: Into<Bytes>> From<T> for Value {
    fn from(b: T) -> Value {
        Value::new_normal(b.into())
    }
}

#[cfg(cdrs_orm)]
impl <T: serde::Serialize + Into<Bytes>> Into<Bytes> for Option<T> {
    fn into(self) -> Bytes {
        match self {
            // Create empty bytes
            None => "".to_string().into(),
            Some(t) => t.into(),
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

impl Into<Bytes> for String {
    fn into(self) -> Bytes {
        Bytes(self.into_bytes())
    }
}

impl<'a> Into<Bytes> for &'a str {
    fn into(self) -> Bytes {
        Bytes(self.as_bytes().to_vec())
    }
}

impl Into<Bytes> for i8 {
    fn into(self) -> Bytes {
        Bytes(vec![self as u8])
    }
}

impl Into<Bytes> for i16 {
    fn into(self) -> Bytes {
        Bytes(to_short(self))
    }
}

impl Into<Bytes> for i32 {
    fn into(self) -> Bytes {
        Bytes(to_int(self))
    }
}

impl Into<Bytes> for i64 {
    fn into(self) -> Bytes {
        Bytes(to_bigint(self))
    }
}

impl Into<Bytes> for u8 {
    fn into(self) -> Bytes {
        Bytes(vec![self])
    }
}

impl Into<Bytes> for u16 {
    fn into(self) -> Bytes {
        Bytes(to_u_short(self))
    }
}

impl Into<Bytes> for u32 {
    fn into(self) -> Bytes {
        Bytes(to_u(self))
    }
}

impl Into<Bytes> for u64 {
    fn into(self) -> Bytes {
        Bytes(to_u_big(self))
    }
}

impl Into<Bytes> for NonZeroI8 {
    #[inline]
    fn into(self) -> Bytes {
        self.get().into()
    }
}

impl Into<Bytes> for NonZeroI16 {
    #[inline]
    fn into(self) -> Bytes {
        self.get().into()
    }
}

impl Into<Bytes> for NonZeroI32 {
    #[inline]
    fn into(self) -> Bytes {
        self.get().into()
    }
}

impl Into<Bytes> for NonZeroI64 {
    #[inline]
    fn into(self) -> Bytes {
        self.get().into()
    }
}

impl Into<Bytes> for bool {
    fn into(self) -> Bytes {
        if self {
            Bytes(vec![1])
        } else {
            Bytes(vec![0])
        }
    }
}

impl Into<Bytes> for Uuid {
    fn into(self) -> Bytes {
        Bytes(self.as_bytes().to_vec())
    }
}

impl Into<Bytes> for IpAddr {
    fn into(self) -> Bytes {
        match self {
            IpAddr::V4(ip) => Bytes(ip.octets().to_vec()),
            IpAddr::V6(ip) => Bytes(ip.octets().to_vec()),
        }
    }
}

impl Into<Bytes> for f32 {
    fn into(self) -> Bytes {
        Bytes(to_float(self))
    }
}

impl Into<Bytes> for f64 {
    fn into(self) -> Bytes {
        Bytes(to_float_big(self))
    }
}

impl Into<Bytes> for PrimitiveDateTime {
    fn into(self) -> Bytes {
        let ts: i64 =
            self.assume_utc().unix_timestamp() * 1_000 + self.nanosecond() as i64 / 1_000_000;
        Bytes(to_bigint(ts))
    }
}

impl Into<Bytes> for Blob {
    fn into(self) -> Bytes {
        Bytes(self.into_vec())
    }
}

impl Into<Bytes> for Decimal {
    fn into(self) -> Bytes {
        Bytes(self.as_bytes())
    }
}

impl Into<Bytes> for NaiveDateTime {
    fn into(self) -> Bytes {
        self.timestamp_millis().into()
    }
}

impl Into<Bytes> for DateTime<Utc> {
    fn into(self) -> Bytes {
        self.timestamp_millis().into()
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
