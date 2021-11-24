use derive_more::Constructor;
use std::convert::TryInto;
use std::io::{self, Write};
use std::io::{Cursor, Read};
use std::net::{IpAddr, SocketAddr};

use crate::error::{column_is_empty_err, Error as ErrorBody, Result as CDRSResult};
use crate::frame::traits::FromCursor;
use crate::frame::Serialize;
use crate::types::data_serialization_types::decode_inet;

pub const SHORT_LEN: usize = 2;
pub const INT_LEN: usize = 4;
pub const UUID_LEN: usize = 16;

const NULL_INT_LEN: CInt = -1;
const NULL_SHORT_LEN: CIntShort = -1;

#[macro_use]
pub mod blob;
pub mod data_serialization_types;
pub mod decimal;
pub mod from_cdrs;
pub mod list;
pub mod map;
pub mod rows;
pub mod tuple;
pub mod udt;
pub mod value;

pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::frame::{TryFromRow, TryFromUdt};
    pub use crate::types::blob::Blob;
    pub use crate::types::decimal::Decimal;
    pub use crate::types::list::List;
    pub use crate::types::map::Map;
    pub use crate::types::rows::Row;
    pub use crate::types::tuple::Tuple;
    pub use crate::types::udt::Udt;
    pub use crate::types::value::{Bytes, Value};
    pub use crate::types::AsRustType;
}

/// Should be used to represent a single column as a Rust value.
pub trait AsRustType<T> {
    fn as_rust_type(&self) -> CDRSResult<Option<T>>;

    fn as_r_type(&self) -> CDRSResult<T> {
        self.as_rust_type()
            .and_then(|op| op.ok_or_else(|| ErrorBody::from("Value is null or non-set")))
    }
}

pub trait AsRust {
    fn as_rust<R>(&self) -> CDRSResult<Option<R>>
    where
        Self: AsRustType<R>,
    {
        self.as_rust_type()
    }

    fn as_r_rust<T>(&self) -> CDRSResult<T>
    where
        Self: AsRustType<T>,
    {
        self.as_rust()
            .and_then(|op| op.ok_or_else(|| "Value is null or non-set".into()))
    }
}

/// Should be used to return a single column as Rust value by its name.
pub trait IntoRustByName<R> {
    fn get_by_name(&self, name: &str) -> CDRSResult<Option<R>>;

    fn get_r_by_name(&self, name: &str) -> CDRSResult<R> {
        self.get_by_name(name)
            .and_then(|op| op.ok_or_else(|| column_is_empty_err(name)))
    }
}

pub trait ByName {
    fn by_name<R>(&self, name: &str) -> CDRSResult<Option<R>>
    where
        Self: IntoRustByName<R>,
    {
        self.get_by_name(name)
    }

    fn r_by_name<R>(&self, name: &str) -> CDRSResult<R>
    where
        Self: IntoRustByName<R>,
    {
        self.by_name(name)
            .and_then(|op| op.ok_or_else(|| column_is_empty_err(name)))
    }
}

/// Should be used to return a single column as Rust value by its name.
pub trait IntoRustByIndex<R> {
    fn get_by_index(&self, index: usize) -> CDRSResult<Option<R>>;

    fn get_r_by_index(&self, index: usize) -> CDRSResult<R> {
        self.get_by_index(index)
            .and_then(|op| op.ok_or_else(|| column_is_empty_err(index)))
    }
}

pub trait ByIndex {
    fn by_index<R>(&self, index: usize) -> CDRSResult<Option<R>>
    where
        Self: IntoRustByIndex<R>,
    {
        self.get_by_index(index)
    }

    fn r_by_index<R>(&self, index: usize) -> CDRSResult<R>
    where
        Self: IntoRustByIndex<R>,
    {
        self.by_index(index)
            .and_then(|op| op.ok_or_else(|| column_is_empty_err(index)))
    }
}

#[inline]
fn convert_to_array<const S: usize>(bytes: &[u8]) -> Result<[u8; S], io::Error> {
    bytes
        .try_into()
        .map_err(|error| io::Error::new(io::ErrorKind::UnexpectedEof, error))
}

#[inline]
pub fn try_u64_from_bytes(bytes: &[u8]) -> Result<u64, io::Error> {
    Ok(u64::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn try_i64_from_bytes(bytes: &[u8]) -> Result<i64, io::Error> {
    Ok(i64::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn try_i32_from_bytes(bytes: &[u8]) -> Result<i32, io::Error> {
    Ok(i32::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn try_i16_from_bytes(bytes: &[u8]) -> Result<i16, io::Error> {
    Ok(i16::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn try_f32_from_bytes(bytes: &[u8]) -> Result<f32, io::Error> {
    Ok(f32::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn try_f64_from_bytes(bytes: &[u8]) -> Result<f64, io::Error> {
    Ok(f64::from_be_bytes(convert_to_array(bytes)?))
}

#[inline]
pub fn u16_from_bytes(bytes: [u8; 2]) -> u16 {
    u16::from_be_bytes(bytes)
}

#[inline]
pub fn to_short(int: i16) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_int(int: i32) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_bigint(int: i64) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_u_short(int: u16) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_u_int(int: u32) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_u_big(int: u64) -> Vec<u8> {
    int.to_be_bytes().into()
}

#[inline]
pub fn to_float(f: f32) -> Vec<u8> {
    f.to_be_bytes().into()
}

#[inline]
pub fn to_float_big(f: f64) -> Vec<u8> {
    f.to_be_bytes().into()
}

pub fn serialize_str(cursor: &mut Cursor<&mut Vec<u8>>, value: &str) {
    let len = value.len() as CIntShort;
    len.serialize(cursor);
    let _ = cursor.write(value.as_bytes());
}

#[derive(Debug, Clone, Constructor, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct CString {
    string: String,
}

impl CString {
    /// Converts internal value into pointer of `str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.string.as_str()
    }

    /// Converts internal value into a plain `String`.
    #[inline]
    pub fn into_plain(self) -> String {
        self.string
    }

    /// Represents internal value as a `String`.
    #[inline]
    pub fn as_plain(&self) -> String {
        self.string.clone()
    }
}

impl Serialize for CString {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let len = self.string.len() as CIntShort;
        len.serialize(cursor);
        self.string.serialize(cursor);
    }
}

impl FromCursor for CString {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CString> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let len = i16::from_be_bytes(buff);
        let body_bytes = cursor_next_value(cursor, len as usize)?;

        String::from_utf8(body_bytes)
            .map_err(Into::into)
            .map(CString::new)
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct CStringLong {
    string: String,
}

impl CStringLong {
    /// Converts internal value into pointer of `str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.string.as_str()
    }

    /// Converts internal value into a plain `String`.
    #[inline]
    pub fn into_plain(self) -> String {
        self.string
    }
}

impl Serialize for CStringLong {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let len = self.string.len() as CInt;
        len.serialize(cursor);
        self.string.serialize(cursor);
    }
}

impl FromCursor for CStringLong {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CStringLong> {
        let mut buff = [0; INT_LEN];
        cursor.read_exact(&mut buff)?;

        let len = i32::from_be_bytes(buff);
        let body_bytes = cursor_next_value(cursor, len as usize)?;

        String::from_utf8(body_bytes)
            .map_err(Into::into)
            .map(CStringLong::new)
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct CStringList {
    pub list: Vec<CString>,
}

impl CStringList {
    pub fn into_plain(self) -> Vec<String> {
        self.list
            .into_iter()
            .map(|string| string.into_plain())
            .collect()
    }
}

impl Serialize for CStringList {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let len = self.list.len() as CIntShort;
        len.serialize(cursor);

        for string in &self.list {
            string.serialize(cursor);
        }
    }
}

impl FromCursor for CStringList {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CStringList> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let len = i16::from_be_bytes(buff);
        let mut list = Vec::with_capacity(len as usize * SHORT_LEN);
        for _ in 0..len {
            list.push(CString::from_cursor(cursor)?);
        }

        Ok(CStringList { list })
    }
}

//

#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
/// The structure that represents Cassandra byte type.
pub struct CBytes {
    bytes: Option<Vec<u8>>,
}

impl CBytes {
    #[inline]
    pub fn new(bytes: Vec<u8>) -> CBytes {
        CBytes { bytes: Some(bytes) }
    }

    /// Creates Cassandra bytes that represent empty or null value
    #[inline]
    pub fn new_empty() -> CBytes {
        CBytes { bytes: None }
    }

    /// Converts `CBytes` into a plain array of bytes
    #[inline]
    pub fn into_plain(self) -> Option<Vec<u8>> {
        self.bytes
    }

    #[inline]
    pub fn as_slice(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match &self.bytes {
            None => true,
            Some(bytes) => bytes.is_empty(),
        }
    }

    #[inline]
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        self.bytes
    }
}

impl FromCursor for CBytes {
    /// from_cursor gets Cursor who's position is set such that it should be a start of bytes.
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CBytes> {
        let len = CInt::from_cursor(cursor)?;
        // null or not set value
        if len < 0 {
            return Ok(CBytes { bytes: None });
        }

        cursor_next_value(cursor, len as usize).map(CBytes::new)
    }
}

impl Serialize for CBytes {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match &self.bytes {
            Some(bytes) => {
                let len = bytes.len() as CInt;
                len.serialize(cursor);
                bytes.serialize(cursor);
            }
            None => NULL_INT_LEN.serialize(cursor),
        }
    }
}

/// Cassandra short bytes
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CBytesShort {
    bytes: Option<Vec<u8>>,
}

impl CBytesShort {
    #[inline]
    pub fn new(bytes: Vec<u8>) -> CBytesShort {
        CBytesShort { bytes: Some(bytes) }
    }

    /// Converts `CBytesShort` into plain vector of bytes;
    #[inline]
    pub fn into_plain(self) -> Option<Vec<u8>> {
        self.bytes
    }

    #[inline]
    pub fn serialized_len(&self) -> usize {
        SHORT_LEN
            + if let Some(bytes) = &self.bytes {
                bytes.len()
            } else {
                0
            }
    }
}

impl FromCursor for CBytesShort {
    /// from_cursor gets Cursor who's position is set such that it should be a start of bytes.
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CBytesShort> {
        let len = CIntShort::from_cursor(cursor)?;

        if len < 0 {
            return Ok(CBytesShort { bytes: None });
        }

        cursor_next_value(cursor, len as usize)
            .map(CBytesShort::new)
            .map_err(Into::into)
    }
}

impl Serialize for CBytesShort {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match &self.bytes {
            Some(bytes) => {
                let len = bytes.len() as CIntShort;
                len.serialize(cursor);
                bytes.serialize(cursor);
            }
            None => NULL_SHORT_LEN.serialize(cursor),
        }
    }
}

/// Cassandra int type.
pub type CInt = i32;

impl FromCursor for CInt {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CInt> {
        let mut buff = [0; INT_LEN];
        cursor.read_exact(&mut buff)?;

        Ok(i32::from_be_bytes(buff))
    }
}

/// Cassandra int short type.
pub type CIntShort = i16;

impl FromCursor for CIntShort {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CIntShort> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        Ok(i16::from_be_bytes(buff))
    }
}

/// The structure which represents Cassandra inet
/// (<https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Constructor)]
pub struct CInet {
    pub addr: SocketAddr,
}

impl Serialize for CInet {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self.addr.ip() {
            IpAddr::V4(v4) => {
                [4].serialize(cursor);
                v4.octets().serialize(cursor);
            }
            IpAddr::V6(v6) => {
                [16].serialize(cursor);
                v6.octets().serialize(cursor);
            }
        }

        to_int(self.addr.port().into()).serialize(cursor);
    }
}

impl FromCursor for CInet {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> CDRSResult<CInet> {
        let mut buff = [0];
        cursor.read_exact(&mut buff)?;

        let n = buff[0];

        let ip = decode_inet(cursor_next_value(cursor, n as usize)?.as_slice())?;
        let port = CInt::from_cursor(cursor)?;
        let socket_addr = SocketAddr::new(ip, port as u16);

        Ok(CInet { addr: socket_addr })
    }
}

pub fn cursor_next_value(cursor: &mut Cursor<&[u8]>, len: usize) -> CDRSResult<Vec<u8>> {
    let mut buff = vec![0u8; len];
    cursor.read_exact(&mut buff)?;
    Ok(buff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use num::BigInt;
    use std::io::Cursor;

    fn from_i_bytes(bytes: &[u8]) -> i64 {
        try_i64_from_bytes(bytes).unwrap()
    }

    fn try_u16_from_bytes(bytes: &[u8]) -> Result<u16, io::Error> {
        Ok(u16::from_be_bytes(convert_to_array(bytes)?))
    }

    fn to_varint(int: BigInt) -> Vec<u8> {
        int.to_signed_bytes_be()
    }

    // CString
    #[test]
    fn test_cstring_new() {
        let value = "foo".to_string();
        let _ = CString::new(value);
    }

    #[test]
    fn test_cstring_as_str() {
        let value = "foo".to_string();
        let cstring = CString::new(value);

        assert_eq!(cstring.as_str(), "foo");
    }

    #[test]
    fn test_cstring_into_plain() {
        let value = "foo".to_string();
        let cstring = CString::new(value);

        assert_eq!(cstring.into_plain(), "foo".to_string());
    }

    #[test]
    fn test_cstring_serialize() {
        let value = "foo".to_string();
        let cstring = CString::new(value);

        assert_eq!(cstring.serialize_to_vec(), &[0, 3, 102, 111, 111]);
    }

    #[test]
    fn test_cstring_from_cursor() {
        let a = &[0, 3, 102, 111, 111, 0];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cstring = CString::from_cursor(&mut cursor).unwrap();
        assert_eq!(cstring.as_str(), "foo");
    }

    // CStringLong
    #[test]
    fn test_cstringlong_new() {
        let value = "foo".to_string();
        let _ = CStringLong::new(value);
    }

    #[test]
    fn test_cstringlong_as_str() {
        let value = "foo".to_string();
        let cstring = CStringLong::new(value);

        assert_eq!(cstring.as_str(), "foo");
    }

    #[test]
    fn test_cstringlong_into_plain() {
        let value = "foo".to_string();
        let cstring = CStringLong::new(value);

        assert_eq!(cstring.into_plain(), "foo".to_string());
    }

    #[test]
    fn test_cstringlong_serialize() {
        let value = "foo".to_string();
        let cstring = CStringLong::new(value);

        assert_eq!(cstring.serialize_to_vec(), &[0, 0, 0, 3, 102, 111, 111]);
    }

    #[test]
    fn test_cstringlong_from_cursor() {
        let a = &[0, 0, 0, 3, 102, 111, 111, 0];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cstring = CStringLong::from_cursor(&mut cursor).unwrap();
        assert_eq!(cstring.as_str(), "foo");
    }

    // CStringList
    #[test]
    fn test_cstringlist() {
        let a = &[0, 2, 0, 3, 102, 111, 111, 0, 3, 102, 111, 111];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let list = CStringList::from_cursor(&mut cursor).unwrap();
        let plain = list.into_plain();
        assert_eq!(plain.len(), 2);
        for s in plain.iter() {
            assert_eq!(s.as_str(), "foo");
        }
    }

    // CBytes
    #[test]
    fn test_cbytes_new() {
        let bytes_vec = vec![1, 2, 3];
        let _ = CBytes::new(bytes_vec);
    }

    #[test]
    fn test_cbytes_into_plain() {
        let cbytes = CBytes::new(vec![1, 2, 3]);
        assert_eq!(cbytes.into_bytes().unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn test_cbytes_from_cursor() {
        let a = &[0, 0, 0, 3, 1, 2, 3];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cbytes = CBytes::from_cursor(&mut cursor).unwrap();
        assert_eq!(cbytes.into_bytes().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytes_serialize() {
        let bytes_vec = vec![1, 2, 3];
        let cbytes = CBytes::new(bytes_vec);
        assert_eq!(cbytes.serialize_to_vec(), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    // CBytesShort
    #[test]
    fn test_cbytesshort_new() {
        let bytes_vec = vec![1, 2, 3];
        let _ = CBytesShort::new(bytes_vec);
    }

    #[test]
    fn test_cbytesshort_into_plain() {
        let cbytes = CBytesShort::new(vec![1, 2, 3]);
        assert_eq!(cbytes.into_plain().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytesshort_from_cursor() {
        let a = &[0, 3, 1, 2, 3];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cbytes = CBytesShort::from_cursor(&mut cursor).unwrap();
        assert_eq!(cbytes.into_plain().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytesshort_serialize() {
        let bytes_vec: Vec<u8> = vec![1, 2, 3];
        let cbytes = CBytesShort::new(bytes_vec);
        assert_eq!(cbytes.serialize_to_vec(), vec![0, 3, 1, 2, 3]);
    }

    // CInt
    #[test]
    fn test_cint_from_cursor() {
        let a = &[0, 0, 0, 5];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let i = CInt::from_cursor(&mut cursor).unwrap();
        assert_eq!(i, 5);
    }

    // CIntShort
    #[test]
    fn test_cintshort_from_cursor() {
        let a = &[0, 5];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let i = CIntShort::from_cursor(&mut cursor).unwrap();
        assert_eq!(i, 5);
    }

    // cursor_next_value
    #[test]
    fn test_cursor_next_value() {
        let a = &[0, 1, 2, 3, 4];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let l = 3;
        let val = cursor_next_value(&mut cursor, l).unwrap();
        assert_eq!(val, vec![0, 1, 2]);
    }

    #[test]
    fn test_try_u16_from_bytes() {
        let bytes: [u8; 2] = [0, 12]; // or .to_le()
        let val = try_u16_from_bytes(&bytes);
        assert_eq!(val.unwrap(), 12u16);
    }

    #[test]
    fn test_from_i_bytes() {
        let bytes: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 12]; // or .to_le()
        let val = from_i_bytes(&bytes);
        assert_eq!(val, 12i64);
    }

    #[test]
    fn test_to_varint() {
        assert_eq!(to_varint(0.into()), vec![0x00]);
        assert_eq!(to_varint(1.into()), vec![0x01]);
        assert_eq!(to_varint(127.into()), vec![0x7F]);
        assert_eq!(to_varint(128.into()), vec![0x00, 0x80]);
        assert_eq!(to_varint(129.into()), vec![0x00, 0x81]);
        assert_eq!(to_varint(BigInt::from(-1)), vec![0xFF]);
        assert_eq!(to_varint(BigInt::from(-128)), vec![0x80]);
        assert_eq!(to_varint(BigInt::from(-129)), vec![0xFF, 0x7F]);
    }
}
