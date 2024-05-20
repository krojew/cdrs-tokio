use num_bigint::BigInt;
use std::collections::HashMap;
use std::net::IpAddr;

use super::prelude::{Blob, Decimal, Duration};
use crate::error::Result as CDRSResult;
use crate::frame::message_result::{ColType, ColTypeOption};
use crate::frame::Version;
use crate::types::CBytes;

#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum CassandraType {
    Ascii(String),
    Bigint(i64),
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
    Duration(Duration),
    List(Vec<CassandraType>),
    Map(Vec<(CassandraType, CassandraType)>),
    Set(Vec<CassandraType>),
    Udt(HashMap<String, CassandraType>),
    Tuple(Vec<CassandraType>),
    Vector(Vec<CassandraType>),
    Null,
}

/// Get a function to convert `CBytes` and `ColTypeOption` into a `CassandraType`
pub fn wrapper_fn(
    col_type: &ColType,
) -> &'static dyn Fn(&CBytes, &ColTypeOption, Version) -> CDRSResult<CassandraType> {
    match col_type {
        ColType::Blob => &wrappers::blob,
        ColType::Ascii => &wrappers::ascii,
        ColType::Int => &wrappers::int,
        ColType::List => &wrappers::list,
        ColType::Custom => &wrappers::custom,
        ColType::Bigint => &wrappers::bigint,
        ColType::Boolean => &wrappers::bool,
        ColType::Counter => &wrappers::counter,
        ColType::Decimal => &wrappers::decimal,
        ColType::Double => &wrappers::double,
        ColType::Float => &wrappers::float,
        ColType::Timestamp => &wrappers::timestamp,
        ColType::Uuid => &wrappers::uuid,
        ColType::Varchar => &wrappers::varchar,
        ColType::Varint => &wrappers::varint,
        ColType::Timeuuid => &wrappers::timeuuid,
        ColType::Inet => &wrappers::inet,
        ColType::Date => &wrappers::date,
        ColType::Time => &wrappers::time,
        ColType::Smallint => &wrappers::smallint,
        ColType::Tinyint => &wrappers::tinyint,
        ColType::Duration => &wrappers::duration,
        ColType::Map => &wrappers::map,
        ColType::Set => &wrappers::set,
        ColType::Udt => &wrappers::udt,
        ColType::Tuple => &wrappers::tuple,
    }
}

pub mod wrappers {
    use super::CassandraType;
    use crate::error::Result as CDRSResult;
    use crate::frame::message_result::{ColType, ColTypeOption, ColTypeOptionValue};
    use crate::frame::Version;
    use crate::types::data_serialization_types::*;
    use crate::types::list::List;
    use crate::types::vector::{get_vector_type_info, Vector, VectorInfo};
    use crate::types::AsCassandraType;
    use crate::types::CBytes;
    use crate::types::{map::Map, tuple::Tuple, udt::Udt};

    pub fn custom(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        if let ColTypeOption {
            id: ColType::Custom,
            value: Some(value),
        } = col_type
        {
            let VectorInfo {
                internal_type: _,
                count,
            } = get_vector_type_info(value).unwrap();

            if let Some(actual_bytes) = bytes.as_slice() {
                let vector = decode_float_vector(actual_bytes, version, count)
                    .map(|data| Some(Vector::new(col_type.clone(), data, version)))
                    .expect("could not decode vector")
                    .unwrap()
                    .as_cassandra_type()?
                    .unwrap_or(CassandraType::Null);
                return Ok(vector);
            }
        }

        Ok(CassandraType::Null)
    }

    pub fn map(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        if let Some(actual_bytes) = bytes.as_slice() {
            let decoded_map = decode_map(actual_bytes, version)?;

            Ok(Map::new(decoded_map, col_type.clone(), version)
                .as_cassandra_type()?
                .unwrap_or(CassandraType::Null))
        } else {
            Ok(CassandraType::Null)
        }
    }

    pub fn set(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        if let Some(actual_bytes) = bytes.as_slice() {
            let decoded_set = decode_set(actual_bytes, version)?;

            Ok(List::new(col_type.clone(), decoded_set, version)
                .as_cassandra_type()?
                .unwrap_or(CassandraType::Null))
        } else {
            Ok(CassandraType::Null)
        }
    }

    pub fn udt(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        if let Some(ColTypeOptionValue::UdtType(ref list_type_option)) = col_type.value {
            if let Some(actual_bytes) = bytes.as_slice() {
                let len = list_type_option.descriptions.len();
                let decoded_udt = decode_udt(actual_bytes, len, version)?;

                return Ok(Udt::new(decoded_udt, list_type_option, version)
                    .as_cassandra_type()?
                    .unwrap_or(CassandraType::Null));
            }
        }

        Ok(CassandraType::Null)
    }

    pub fn tuple(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        if let Some(ColTypeOptionValue::TupleType(ref list_type_option)) = col_type.value {
            if let Some(actual_bytes) = bytes.as_slice() {
                let len = list_type_option.types.len();
                let decoded_tuple = decode_tuple(actual_bytes, len, version)?;

                return Ok(Tuple::new(decoded_tuple, list_type_option, version)
                    .as_cassandra_type()?
                    .unwrap_or(CassandraType::Null));
            }
        }

        Ok(CassandraType::Null)
    }

    pub fn null(
        _: &CBytes,
        _col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        Ok(CassandraType::Null)
    }

    pub fn blob(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, Blob)?;

        Ok(match t {
            Some(t) => CassandraType::Blob(t),
            None => CassandraType::Null,
        })
    }

    pub fn ascii(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, String)?;

        Ok(match t {
            Some(t) => CassandraType::Ascii(t),
            None => CassandraType::Null,
        })
    }

    pub fn int(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i32)?;

        Ok(match t {
            Some(t) => CassandraType::Int(t),
            None => CassandraType::Null,
        })
    }

    pub fn list(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        version: Version,
    ) -> CDRSResult<CassandraType> {
        let list = as_rust_type!(col_type, bytes, version, List)?;
        Ok(match list {
            Some(t) => t.as_cassandra_type()?.unwrap_or(CassandraType::Null),
            None => CassandraType::Null,
        })
    }

    pub fn bigint(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i64)?;

        Ok(match t {
            Some(t) => CassandraType::Bigint(t),
            None => CassandraType::Null,
        })
    }

    pub fn counter(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i64)?;

        Ok(match t {
            Some(t) => CassandraType::Counter(t),
            None => CassandraType::Null,
        })
    }

    pub fn decimal(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, Decimal)?;

        Ok(match t {
            Some(t) => CassandraType::Decimal(t),
            None => CassandraType::Null,
        })
    }

    pub fn double(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, f64)?;

        Ok(match t {
            Some(t) => CassandraType::Double(t),
            None => CassandraType::Null,
        })
    }

    pub fn float(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, f32)?;

        Ok(match t {
            Some(t) => CassandraType::Float(t),
            None => CassandraType::Null,
        })
    }

    pub fn timestamp(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i64)?;

        Ok(match t {
            Some(t) => CassandraType::Timestamp(t),
            None => CassandraType::Null,
        })
    }

    pub fn uuid(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, Uuid)?;

        Ok(match t {
            Some(t) => CassandraType::Uuid(t),
            None => CassandraType::Null,
        })
    }

    pub fn varchar(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, String)?;

        Ok(match t {
            Some(t) => CassandraType::Varchar(t),
            None => CassandraType::Null,
        })
    }

    pub fn varint(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, BigInt)?;

        Ok(match t {
            Some(t) => CassandraType::Varint(t),
            None => CassandraType::Null,
        })
    }

    pub fn timeuuid(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, Uuid)?;

        Ok(match t {
            Some(t) => CassandraType::Timeuuid(t),
            None => CassandraType::Null,
        })
    }

    pub fn inet(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, IpAddr)?;

        Ok(match t {
            Some(t) => CassandraType::Inet(t),
            None => CassandraType::Null,
        })
    }

    pub fn date(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i32)?;

        Ok(match t {
            Some(t) => CassandraType::Date(t),
            None => CassandraType::Null,
        })
    }

    pub fn time(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i64)?;

        Ok(match t {
            Some(t) => CassandraType::Time(t),
            None => CassandraType::Null,
        })
    }

    pub fn smallint(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i16)?;

        Ok(match t {
            Some(t) => CassandraType::Smallint(t),
            None => CassandraType::Null,
        })
    }

    pub fn tinyint(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, i8)?;

        Ok(match t {
            Some(t) => CassandraType::Tinyint(t),
            None => CassandraType::Null,
        })
    }

    pub fn bool(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, bool)?;

        Ok(match t {
            Some(t) => CassandraType::Boolean(t),
            None => CassandraType::Null,
        })
    }

    pub fn duration(
        bytes: &CBytes,
        col_type: &ColTypeOption,
        _version: Version,
    ) -> CDRSResult<CassandraType> {
        let t = as_rust_type!(col_type, bytes, Duration)?;

        Ok(match t {
            Some(t) => CassandraType::Duration(t),
            None => CassandraType::Null,
        })
    }
}
