use num::BigInt;
use std::collections::HashMap;
use std::net::IpAddr;

use super::prelude::{Blob, Decimal};

#[derive(Debug, PartialEq, Clone)]
pub enum CassandraType {
    Ascii(String),
    Bigint(BigInt),
    Blob(Blob),
    Boolean(bool),
    Counter(i64),
    Decimal(Decimal),
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64),
    Uuid(uuid::Uuid),
    Varchar(String),
    Varint(BigInt),
    Timeuuid(uuid::Uuid),
    Inet(IpAddr),
    Date(i32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
    List(Vec<CassandraType>),
    Map(Vec<(CassandraType, CassandraType)>),
    Set(Vec<CassandraType>),
    Udt(HashMap<String, CassandraType>),
    Tuple(Vec<CassandraType>),
    Null,
}

pub mod wrappers {
    use super::CassandraType;
    use crate::frame::frame_result::{ColType, ColTypeOption, ColTypeOptionValue};
    use crate::types::data_serialization_types::*;
    use crate::types::list::List;
    use crate::types::AsCassandraType;
    use crate::types::CBytes;

    pub fn blob(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, Blob).unwrap();

        match t {
            Some(t) => CassandraType::Blob(t),
            None => CassandraType::Null,
        }
    }

    pub fn ascii(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, String).unwrap();

        match t {
            Some(t) => CassandraType::Ascii(t),
            None => CassandraType::Null,
        }
    }

    pub fn int(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i32).unwrap();

        match t {
            Some(t) => CassandraType::Int(t),
            None => CassandraType::Null,
        }
    }

    pub fn list(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let list = as_rust_type!(col_type, bytes, List).unwrap();
        match list {
            Some(t) => t.as_cassandra_type().unwrap().unwrap(),
            None => CassandraType::Null,
        }
    }

    pub fn bigint(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, BigInt).unwrap();

        match t {
            Some(t) => CassandraType::Bigint(t),
            None => CassandraType::Null,
        }
    }

    pub fn counter(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i64).unwrap();

        match t {
            Some(t) => CassandraType::Counter(t),
            None => CassandraType::Null,
        }
    }

    pub fn decimal(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, Decimal).unwrap();

        match t {
            Some(t) => CassandraType::Decimal(t),
            None => CassandraType::Null,
        }
    }

    pub fn double(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, f64).unwrap();

        match t {
            Some(t) => CassandraType::Double(t),
            None => CassandraType::Null,
        }
    }

    pub fn float(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, f32).unwrap();

        match t {
            Some(t) => CassandraType::Float(t),
            None => CassandraType::Null,
        }
    }

    pub fn timestamp(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i64).unwrap();

        match t {
            Some(t) => CassandraType::Timestamp(t),
            None => CassandraType::Null,
        }
    }

    pub fn uuid(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, Uuid).unwrap();

        match t {
            Some(t) => CassandraType::Uuid(t),
            None => CassandraType::Null,
        }
    }

    pub fn varchar(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, String).unwrap();

        match t {
            Some(t) => CassandraType::Varchar(t),
            None => CassandraType::Null,
        }
    }

    pub fn varint(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, BigInt).unwrap();

        match t {
            Some(t) => CassandraType::Varint(t),
            None => CassandraType::Null,
        }
    }

    pub fn timeuuid(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, Uuid).unwrap();

        match t {
            Some(t) => CassandraType::Timeuuid(t),
            None => CassandraType::Null,
        }
    }

    pub fn inet(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, IpAddr).unwrap();

        match t {
            Some(t) => CassandraType::Inet(t),
            None => CassandraType::Null,
        }
    }

    pub fn date(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i32).unwrap();

        match t {
            Some(t) => CassandraType::Date(t),
            None => CassandraType::Null,
        }
    }

    pub fn time(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i64).unwrap();

        match t {
            Some(t) => CassandraType::Time(t),
            None => CassandraType::Null,
        }
    }

    pub fn smallint(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i16).unwrap();

        match t {
            Some(t) => CassandraType::Smallint(t),
            None => CassandraType::Null,
        }
    }

    pub fn tinyint(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, i8).unwrap();

        match t {
            Some(t) => CassandraType::Tinyint(t),
            None => CassandraType::Null,
        }
    }

    pub fn bool(bytes: &CBytes, col_type: &ColTypeOption) -> CassandraType {
        let t = as_rust_type!(col_type, bytes, bool).unwrap();

        match t {
            Some(t) => CassandraType::Boolean(t),
            None => CassandraType::Null,
        }
    }
}
