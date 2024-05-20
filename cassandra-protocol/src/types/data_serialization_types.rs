use integer_encoding::VarInt;
use num_bigint::BigInt;
use std::convert::TryInto;
use std::io;
use std::net;
use std::string::FromUtf8Error;

use super::blob::Blob;
use super::decimal::Decimal;
use super::duration::Duration;
use crate::error;
use crate::frame::{FromCursor, Version};
use crate::types::{
    try_f32_from_bytes, try_f64_from_bytes, try_i16_from_bytes, try_i32_from_bytes,
    try_i64_from_bytes, CBytes, CInt, INT_LEN,
};

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L813

const FALSE_BYTE: u8 = 0;

// Decodes Cassandra `ascii` data (bytes)
#[inline]
pub fn decode_custom(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `ascii` data (bytes)
#[inline]
pub fn decode_ascii(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `varchar` data (bytes)
#[inline]
pub fn decode_varchar(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `bigint` data (bytes)
#[inline]
pub fn decode_bigint(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i64_from_bytes(bytes)
}

// Decodes Cassandra `blob` data (bytes)
#[inline]
pub fn decode_blob(bytes: &[u8]) -> Result<Blob, io::Error> {
    // in fact we just pass it through.
    Ok(bytes.into())
}

// Decodes Cassandra `boolean` data (bytes)
#[inline]
pub fn decode_boolean(bytes: &[u8]) -> Result<bool, io::Error> {
    if bytes.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "no bytes were found",
        ))
    } else {
        Ok(bytes[0] != FALSE_BYTE)
    }
}

// Decodes Cassandra `int` data (bytes)
#[inline]
pub fn decode_int(bytes: &[u8]) -> Result<i32, io::Error> {
    try_i32_from_bytes(bytes)
}

// Decodes Cassandra `date` data (bytes)
//    0: -5877641-06-23
// 2^31: 1970-1-1
// 2^32: 5881580-07-11
#[inline]
pub fn decode_date(bytes: &[u8]) -> Result<i32, io::Error> {
    try_i32_from_bytes(bytes)
}

// Decodes Cassandra `decimal` data (bytes)
pub fn decode_decimal(bytes: &[u8]) -> Result<Decimal, io::Error> {
    let lr = bytes.split_at(INT_LEN);

    let scale = try_i32_from_bytes(lr.0)?;
    let unscaled = decode_varint(lr.1)?;

    Ok(Decimal::new(unscaled, scale))
}

// Decodes Cassandra `double` data (bytes)
#[inline]
pub fn decode_double(bytes: &[u8]) -> Result<f64, io::Error> {
    try_f64_from_bytes(bytes)
}

// Decodes Cassandra `float` data (bytes)
#[inline]
pub fn decode_float(bytes: &[u8]) -> Result<f32, io::Error> {
    try_f32_from_bytes(bytes)
}

// Decodes Cassandra `inet` data (bytes)
#[allow(clippy::many_single_char_names)]
pub fn decode_inet(bytes: &[u8]) -> Result<net::IpAddr, io::Error> {
    match bytes.len() {
        // v4
        4 => {
            let array: [u8; 4] = bytes[0..4].try_into().unwrap();
            Ok(net::IpAddr::V4(net::Ipv4Addr::from(array)))
        }
        // v6
        16 => {
            let array: [u8; 16] = bytes[0..16].try_into().unwrap();
            Ok(net::IpAddr::V6(net::Ipv6Addr::from(array)))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid Ip address {bytes:?}"),
        )),
    }
}

// Decodes Cassandra `timestamp` data (bytes) into Rust's `Result<i64, io::Error>`
// `i32` represents a millisecond-precision
//  offset from the unix epoch (00:00:00, January 1st, 1970).  Negative values
//  represent a negative offset from the epoch.
#[inline]
pub fn decode_timestamp(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i64_from_bytes(bytes)
}

//noinspection DuplicatedCode
// Decodes Cassandra `list` data (bytes)
pub fn decode_list(bytes: &[u8], version: Version) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor, version)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut list = Vec::with_capacity(l as usize);
    for _ in 0..l {
        let b = CBytes::from_cursor(&mut cursor, version)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        list.push(b);
    }
    Ok(list)
}

pub fn decode_float_vector(
    bytes: &[u8],
    _version: Version,
    count: usize,
) -> Result<Vec<CBytes>, io::Error> {
    let type_size = 4;

    let mut vector = Vec::with_capacity(count);
    for i in (0..(count * type_size)).step_by(4) {
        vector.push(CBytes::new(bytes[i..i + type_size].to_vec()));
    }

    Ok(vector)
}

// Decodes Cassandra `set` data (bytes)
#[inline]
pub fn decode_set(bytes: &[u8], version: Version) -> Result<Vec<CBytes>, io::Error> {
    decode_list(bytes, version)
}

// Decodes Cassandra `map` data (bytes)
pub fn decode_map(bytes: &[u8], version: Version) -> Result<Vec<(CBytes, CBytes)>, io::Error> {
    let mut cursor = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor, version)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut map = Vec::with_capacity(l as usize);
    for _ in 0..l {
        let k = CBytes::from_cursor(&mut cursor, version)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let v = CBytes::from_cursor(&mut cursor, version)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        map.push((k, v));
    }
    Ok(map)
}

// Decodes Cassandra `smallint` data (bytes)
#[inline]
pub fn decode_smallint(bytes: &[u8]) -> Result<i16, io::Error> {
    try_i16_from_bytes(bytes)
}

// Decodes Cassandra `tinyint` data (bytes)
#[inline]
pub fn decode_tinyint(bytes: &[u8]) -> Result<i8, io::Error> {
    Ok(bytes[0] as i8)
}

// Decodes Cassandra `text` data (bytes)
#[inline]
pub fn decode_text(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `time` data (bytes)
#[inline]
pub fn decode_time(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i64_from_bytes(bytes)
}

// Decodes Cassandra `timeuuid` data (bytes)
#[inline]
pub fn decode_timeuuid(bytes: &[u8]) -> Result<uuid::Uuid, uuid::Error> {
    uuid::Uuid::from_slice(bytes)
}

// Decodes Cassandra `varint` data (bytes)
#[inline]
pub fn decode_varint(bytes: &[u8]) -> Result<BigInt, io::Error> {
    Ok(BigInt::from_signed_bytes_be(bytes))
}

// Decodes Cassandra `duration` data (bytes)
#[inline]
pub fn decode_duration(bytes: &[u8]) -> Result<Duration, io::Error> {
    let (months, month_bytes_read) =
        i32::decode_var(bytes).ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;

    let (days, day_bytes_read) = i32::decode_var(&bytes[month_bytes_read..])
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;

    let (nanoseconds, _) = i64::decode_var(&bytes[(month_bytes_read + day_bytes_read)..])
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;

    Duration::new(months, days, nanoseconds)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

// Decodes Cassandra `Udt` data (bytes)
pub fn decode_udt(bytes: &[u8], l: usize, version: Version) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor = io::Cursor::new(bytes);
    let mut udt = Vec::with_capacity(l);
    for _ in 0..l {
        let v = CBytes::from_cursor(&mut cursor, version)
            .or_else(|err| match err {
                error::Error::Io(io_err) => {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        Ok(CBytes::new_null())
                    } else {
                        Err(io_err.into())
                    }
                }
                _ => Err(err),
            })
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        udt.push(v);
    }
    Ok(udt)
}

//noinspection DuplicatedCode
// Decodes Cassandra `Tuple` data (bytes)
pub fn decode_tuple(bytes: &[u8], l: usize, version: Version) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor = io::Cursor::new(bytes);
    let mut tuple = Vec::with_capacity(l);
    for _ in 0..l {
        let v = CBytes::from_cursor(&mut cursor, version)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        tuple.push(v);
    }
    Ok(tuple)
}

//noinspection DuplicatedCode
#[cfg(test)]
mod tests {
    use super::super::super::frame::message_result::*;
    use super::*;
    use crate::types::{to_float, to_float_big};
    use float_eq::*;
    use std::net::IpAddr;

    #[test]
    fn decode_custom_test() {
        assert_eq!(decode_custom(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_ascii_test() {
        assert_eq!(decode_ascii(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_varchar_test() {
        assert_eq!(decode_varchar(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_bigint_test() {
        assert_eq!(decode_bigint(&[0, 0, 0, 0, 0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_blob_test() {
        assert_eq!(
            decode_blob(&[0, 0, 0, 3]).unwrap().into_vec(),
            vec![0, 0, 0, 3]
        );
    }

    #[test]
    fn decode_boolean_test() {
        assert!(!decode_boolean(&[0]).unwrap());
        assert!(decode_boolean(&[1]).unwrap());
        assert!(decode_boolean(&[]).is_err());
    }

    #[test]
    fn decode_int_test() {
        assert_eq!(decode_int(&[0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_date_test() {
        assert_eq!(decode_date(&[0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_double_test() {
        let bytes = to_float_big(0.3);
        assert_float_eq!(
            decode_double(bytes.as_slice()).unwrap(),
            0.3,
            abs <= f64::EPSILON
        );
    }

    #[test]
    fn decode_float_test() {
        let bytes = to_float(0.3);
        assert_float_eq!(
            decode_float(bytes.as_slice()).unwrap(),
            0.3,
            abs <= f32::EPSILON
        );
    }

    #[test]
    fn decode_inet_test() {
        let bytes_v4 = &[0, 0, 0, 0];
        match decode_inet(bytes_v4) {
            Ok(IpAddr::V4(ref ip)) => assert_eq!(ip.octets(), [0, 0, 0, 0]),
            _ => panic!("wrong ip v4 address"),
        }

        let bytes_v6 = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        match decode_inet(bytes_v6) {
            Ok(IpAddr::V6(ref ip)) => assert_eq!(ip.segments(), [0, 0, 0, 0, 0, 0, 0, 0]),
            _ => panic!("wrong ip v6 address"),
        };
    }

    #[test]
    fn decode_timestamp_test() {
        assert_eq!(decode_timestamp(&[0, 0, 0, 0, 0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_list_test() {
        let results = decode_list(&[0, 0, 0, 1, 0, 0, 0, 2, 1, 2], Version::V4).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_slice().unwrap(), &[1, 2]);
    }

    #[test]
    fn decode_duration_test() {
        let result = decode_duration(&[200, 1, 144, 3, 216, 4]).unwrap();
        assert_eq!(result, Duration::new(100, 200, 300).unwrap());
    }

    #[test]
    fn decode_set_test() {
        let results = decode_set(&[0, 0, 0, 1, 0, 0, 0, 2, 1, 2], Version::V4).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_slice().unwrap(), &[1, 2]);
    }

    #[test]
    fn decode_map_test() {
        let results = decode_map(
            &[0, 0, 0, 1, 0, 0, 0, 2, 1, 2, 0, 0, 0, 2, 2, 1],
            Version::V4,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_slice().unwrap(), &[1, 2]);
        assert_eq!(results[0].1.as_slice().unwrap(), &[2, 1]);
    }

    #[test]
    fn decode_smallint_test() {
        assert_eq!(decode_smallint(&[0, 10]).unwrap(), 10);
    }

    #[test]
    fn decode_tinyint_test() {
        assert_eq!(decode_tinyint(&[10]).unwrap(), 10);
    }

    #[test]
    fn decode_decimal_test() {
        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 10u8]).unwrap(),
            Decimal::new(10.into(), 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 0x00, 0x81]).unwrap(),
            Decimal::new(129.into(), 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 0xFF, 0x7F]).unwrap(),
            Decimal::new(BigInt::from(-129), 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 1, 0x00, 0x81]).unwrap(),
            Decimal::new(129.into(), 1)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 1, 0xFF, 0x7F]).unwrap(),
            Decimal::new(BigInt::from(-129), 1)
        );
    }

    #[test]
    fn decode_text_test() {
        assert_eq!(decode_text(b"abcba").unwrap(), "abcba");
    }

    #[test]
    fn decode_time_test() {
        assert_eq!(decode_time(&[0, 0, 0, 0, 0, 0, 0, 10]).unwrap(), 10);
    }

    #[test]
    fn decode_timeuuid_test() {
        assert_eq!(
            decode_timeuuid(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87])
                .unwrap()
                .as_bytes(),
            &[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]
        );
    }

    #[test]
    fn decode_varint_test() {
        assert_eq!(decode_varint(&[0x00]).unwrap(), 0.into());
        assert_eq!(decode_varint(&[0x01]).unwrap(), 1.into());
        assert_eq!(decode_varint(&[0x7F]).unwrap(), 127.into());
        assert_eq!(decode_varint(&[0x00, 0x80]).unwrap(), 128.into());
        assert_eq!(decode_varint(&[0x00, 0x81]).unwrap(), 129.into());
        assert_eq!(decode_varint(&[0xFF]).unwrap(), BigInt::from(-1));
        assert_eq!(decode_varint(&[0x80]).unwrap(), BigInt::from(-128));
        assert_eq!(decode_varint(&[0xFF, 0x7F]).unwrap(), BigInt::from(-129));
    }

    #[test]
    fn decode_udt_test() {
        let udt = decode_udt(&[0, 0, 0, 2, 1, 2], 1, Version::V4).unwrap();
        assert_eq!(udt.len(), 1);
        assert_eq!(udt[0].as_slice().unwrap(), &[1, 2]);
    }

    #[test]
    fn as_rust_blob_test() {
        let d_type = ColTypeOption {
            id: ColType::Blob,
            value: None,
        };
        let data = CBytes::new(vec![1, 2, 3]);
        assert_eq!(
            as_rust_type!(d_type, data, Blob)
                .unwrap()
                .unwrap()
                .into_vec(),
            vec![1, 2, 3]
        );
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, Blob).is_err());
    }

    #[test]
    fn as_rust_v4_blob_test() {
        let d_type = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.BytesType".into(),
            )),
        };
        let data = CBytes::new(vec![1, 2, 3]);
        assert_eq!(
            as_rust_type!(d_type, data, Blob)
                .unwrap()
                .unwrap()
                .into_vec(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn as_rust_string_test() {
        let type_custom = ColTypeOption {
            id: ColType::Custom,
            value: None,
        };
        let type_ascii = ColTypeOption {
            id: ColType::Ascii,
            value: None,
        };
        let type_varchar = ColTypeOption {
            id: ColType::Varchar,
            value: None,
        };
        let data = CBytes::new(b"abc".to_vec());
        assert_eq!(
            as_rust_type!(type_custom, data, String).unwrap().unwrap(),
            "abc"
        );
        assert_eq!(
            as_rust_type!(type_ascii, data, String).unwrap().unwrap(),
            "abc"
        );
        assert_eq!(
            as_rust_type!(type_varchar, data, String).unwrap().unwrap(),
            "abc"
        );
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, String).is_err());
    }

    #[test]
    fn as_rust_bool_test() {
        let type_boolean = ColTypeOption {
            id: ColType::Boolean,
            value: None,
        };
        let data_true = CBytes::new(vec![1]);
        let data_false = CBytes::new(vec![0]);
        assert!(as_rust_type!(type_boolean, data_true, bool)
            .unwrap()
            .unwrap());
        assert!(!as_rust_type!(type_boolean, data_false, bool)
            .unwrap()
            .unwrap());
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data_false, bool).is_err());
    }

    #[test]
    fn as_rust_v4_bool_test() {
        let type_boolean = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.BooleanType".into(),
            )),
        };
        let data_true = CBytes::new(vec![1]);
        let data_false = CBytes::new(vec![0]);
        assert!(as_rust_type!(type_boolean, data_true, bool)
            .unwrap()
            .unwrap());
        assert!(!as_rust_type!(type_boolean, data_false, bool)
            .unwrap()
            .unwrap());
    }

    #[test]
    fn as_rust_i64_test() {
        let type_bigint = ColTypeOption {
            id: ColType::Bigint,
            value: None,
        };
        let type_timestamp = ColTypeOption {
            id: ColType::Timestamp,
            value: None,
        };
        let type_time = ColTypeOption {
            id: ColType::Time,
            value: None,
        };
        let data = CBytes::new(vec![0, 0, 0, 0, 0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_bigint, data, i64).unwrap().unwrap(), 100);
        assert_eq!(
            as_rust_type!(type_timestamp, data, i64).unwrap().unwrap(),
            100
        );
        assert_eq!(as_rust_type!(type_time, data, i64).unwrap().unwrap(), 100);
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, i64).is_err());
    }

    #[test]
    fn as_rust_v4_i64_test() {
        let type_bigint = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.LongType".into(),
            )),
        };
        let type_timestamp = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.TimestampType".into(),
            )),
        };
        let type_time = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.TimeType".into(),
            )),
        };
        let data = CBytes::new(vec![0, 0, 0, 0, 0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_bigint, data, i64).unwrap().unwrap(), 100);
        assert_eq!(
            as_rust_type!(type_timestamp, data, i64).unwrap().unwrap(),
            100
        );
        assert_eq!(as_rust_type!(type_time, data, i64).unwrap().unwrap(), 100);
    }

    #[test]
    fn as_rust_i32_test() {
        let type_int = ColTypeOption {
            id: ColType::Int,
            value: None,
        };
        let type_date = ColTypeOption {
            id: ColType::Date,
            value: None,
        };
        let data = CBytes::new(vec![0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_int, data, i32).unwrap().unwrap(), 100);
        assert_eq!(as_rust_type!(type_date, data, i32).unwrap().unwrap(), 100);
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, i32).is_err());
    }

    #[test]
    fn as_rust_v4_i32_test() {
        let type_int = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.Int32Type".into(),
            )),
        };
        let type_date = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.SimpleDateType".into(),
            )),
        };
        let data = CBytes::new(vec![0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_int, data, i32).unwrap().unwrap(), 100);
        assert_eq!(as_rust_type!(type_date, data, i32).unwrap().unwrap(), 100);
    }

    #[test]
    fn as_rust_i16_test() {
        let type_smallint = ColTypeOption {
            id: ColType::Smallint,
            value: None,
        };
        let data = CBytes::new(vec![0, 100]);
        assert_eq!(
            as_rust_type!(type_smallint, data, i16).unwrap().unwrap(),
            100
        );
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, i16).is_err());
    }

    #[test]
    fn as_rust_v4_i16_test() {
        let type_smallint = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.ShortType".into(),
            )),
        };
        let data = CBytes::new(vec![0, 100]);
        assert_eq!(
            as_rust_type!(type_smallint, data, i16).unwrap().unwrap(),
            100
        );
    }

    #[test]
    fn as_rust_i8_test() {
        let type_tinyint = ColTypeOption {
            id: ColType::Tinyint,
            value: None,
        };
        let data = CBytes::new(vec![100]);
        assert_eq!(as_rust_type!(type_tinyint, data, i8).unwrap().unwrap(), 100);
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, i8).is_err());
    }

    #[test]
    fn as_rust_v4_i8_test() {
        let type_tinyint = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.ByteType".into(),
            )),
        };
        let data = CBytes::new(vec![100]);
        assert_eq!(as_rust_type!(type_tinyint, data, i8).unwrap().unwrap(), 100);
    }

    #[test]
    fn as_rust_f64_test() {
        let type_double = ColTypeOption {
            id: ColType::Double,
            value: None,
        };
        let data = CBytes::new(to_float_big(0.1_f64));
        assert_float_eq!(
            as_rust_type!(type_double, data, f64).unwrap().unwrap(),
            0.1,
            abs <= f64::EPSILON
        );
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, f64).is_err());
    }

    #[test]
    fn as_rust_v4_f64_test() {
        let type_double = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.DoubleType".into(),
            )),
        };
        let data = CBytes::new(to_float_big(0.1_f64));
        assert_float_eq!(
            as_rust_type!(type_double, data, f64).unwrap().unwrap(),
            0.1,
            abs <= f64::EPSILON
        );
    }

    #[test]
    fn as_rust_f32_test() {
        // let type_decimal = ColTypeOption { id: ColType::Decimal };
        let type_float = ColTypeOption {
            id: ColType::Float,
            value: None,
        };
        let data = CBytes::new(to_float(0.1_f32));
        // assert_eq!(as_rust_type!(type_decimal, data, f32).unwrap(), 100.0);
        assert_float_eq!(
            as_rust_type!(type_float, data, f32).unwrap().unwrap(),
            0.1,
            abs <= f32::EPSILON
        );
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, f32).is_err());
    }

    #[test]
    fn as_rust_v4_f32_test() {
        // let type_decimal = ColTypeOption { id: ColType::Decimal };
        let type_float = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.FloatType".into(),
            )),
        };
        let data = CBytes::new(to_float(0.1_f32));
        // assert_eq!(as_rust_type!(type_decimal, data, f32).unwrap(), 100.0);
        assert_float_eq!(
            as_rust_type!(type_float, data, f32).unwrap().unwrap(),
            0.1,
            abs <= f32::EPSILON
        );
    }

    #[test]
    fn as_rust_inet_test() {
        let type_inet = ColTypeOption {
            id: ColType::Inet,
            value: None,
        };
        let data = CBytes::new(vec![0, 0, 0, 0]);

        match as_rust_type!(type_inet, data, IpAddr) {
            Ok(Some(IpAddr::V4(ref ip))) => assert_eq!(ip.octets(), [0, 0, 0, 0]),
            _ => panic!("wrong ip v4 address"),
        }
        let wrong_type = ColTypeOption {
            id: ColType::Map,
            value: None,
        };
        assert!(as_rust_type!(wrong_type, data, f32).is_err());
    }

    #[test]
    fn as_rust_v4_inet_test() {
        let type_inet = ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(
                "org.apache.cassandra.db.marshal.InetAddressType".into(),
            )),
        };
        let data = CBytes::new(vec![0, 0, 0, 0]);

        match as_rust_type!(type_inet, data, IpAddr) {
            Ok(Some(IpAddr::V4(ref ip))) => assert_eq!(ip.octets(), [0, 0, 0, 0]),
            _ => panic!("wrong ip v4 address"),
        }
    }
}
