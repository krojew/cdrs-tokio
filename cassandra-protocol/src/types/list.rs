use chrono::{DateTime, NaiveDateTime, Utc};
use derive_more::Constructor;
use itertools::Itertools;
use num_bigint::BigInt;
use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::frame::message_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::frame::Version;
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::duration::Duration;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::udt::Udt;
use crate::types::{AsRust, AsRustType, CBytes};

// TODO: consider using pointers to ColTypeOption and Vec<CBytes> instead of owning them.
#[derive(Debug, Constructor)]
pub struct List {
    /// column spec of the list, i.e. id should be List as it's a list and value should contain
    /// a type of list items.
    metadata: ColTypeOption,
    data: Vec<CBytes>,
    protocol_version: Version,
}

impl List {
    fn try_map<T, F>(&self, f: F) -> Result<Vec<T>>
    where
        F: FnMut(&CBytes) -> Result<T>,
    {
        self.data.iter().map(f).try_collect()
    }
}

impl AsRust for List {}

list_as_rust!(Blob);
list_as_rust!(String);
list_as_rust!(bool);
list_as_rust!(i64);
list_as_rust!(i32);
list_as_rust!(i16);
list_as_rust!(i8);
list_as_rust!(f64);
list_as_rust!(f32);
list_as_rust!(IpAddr);
list_as_rust!(Uuid);
list_as_rust!(List);
list_as_rust!(Map);
list_as_rust!(Udt);
list_as_rust!(Tuple);
list_as_rust!(Decimal);
list_as_rust!(BigInt);
list_as_rust!(PrimitiveDateTime);
list_as_rust!(NaiveDateTime);
list_as_rust!(DateTime<Utc>);
list_as_rust!(NonZeroI8);
list_as_rust!(NonZeroI16);
list_as_rust!(NonZeroI32);
list_as_rust!(NonZeroI64);
list_as_rust!(Duration);

list_as_cassandra_type!();

#[cfg(test)]
mod tests {
    use super::*;

    fn list_of(element_type: ColType, data: Vec<CBytes>) -> List {
        List::new(
            ColTypeOption {
                id: ColType::List,
                value: Some(ColTypeOptionValue::CList(Box::new(ColTypeOption {
                    id: element_type,
                    value: None,
                }))),
            },
            data,
            Version::V4,
        )
    }

    fn int_bytes(value: i32) -> CBytes {
        CBytes::new(value.to_be_bytes().to_vec())
    }

    #[test]
    fn converts_list_elements() {
        let list = list_of(ColType::Int, vec![int_bytes(1), int_bytes(2)]);
        let converted: Option<Vec<i32>> = list.as_rust_type().unwrap();
        assert_eq!(converted, Some(vec![1, 2]));
    }

    #[test]
    fn mismatched_element_type_is_error() {
        let list = list_of(ColType::Int, vec![int_bytes(1)]);
        let converted: Result<Option<Vec<String>>> = list.as_rust_type();
        assert!(converted.is_err());
    }

    #[test]
    fn malformed_element_is_error() {
        let list = list_of(ColType::Int, vec![int_bytes(1), CBytes::new(vec![0, 1])]);
        let converted: Result<Option<Vec<i32>>> = list.as_rust_type();
        assert!(converted.is_err());
    }

    #[test]
    fn null_element_is_skipped() {
        let list = list_of(
            ColType::Int,
            vec![int_bytes(1), CBytes::new_null(), int_bytes(3)],
        );
        let converted: Option<Vec<i32>> = list.as_rust_type().unwrap();
        assert_eq!(converted, Some(vec![1, 3]));
    }

    #[test]
    fn malformed_nested_list_is_error() {
        let list = List::new(
            ColTypeOption {
                id: ColType::List,
                value: Some(ColTypeOptionValue::CList(Box::new(ColTypeOption {
                    id: ColType::List,
                    value: Some(ColTypeOptionValue::CList(Box::new(ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    }))),
                }))),
            },
            // a nested list claiming one element but carrying no element bytes
            vec![CBytes::new(vec![0, 0, 0, 1]), CBytes::new_null()],
            Version::V4,
        );
        let converted: Result<Option<Vec<List>>> = list.as_rust_type();
        assert!(converted.is_err());
    }

    fn bigint_bytes(value: i64) -> CBytes {
        CBytes::new(value.to_be_bytes().to_vec())
    }

    #[test]
    fn converts_timestamp_elements_to_date_times() {
        let list = list_of(
            ColType::Timestamp,
            vec![bigint_bytes(1_500), bigint_bytes(-1_500)],
        );
        let expected = vec![
            chrono::DateTime::from_timestamp(1, 500_000_000).unwrap(),
            chrono::DateTime::from_timestamp(-2, 500_000_000).unwrap(),
        ];

        let utc: Option<Vec<chrono::DateTime<chrono::Utc>>> = list.as_rust_type().unwrap();
        assert_eq!(utc, Some(expected.clone()));

        let naive: Option<Vec<chrono::NaiveDateTime>> = list.as_rust_type().unwrap();
        assert_eq!(
            naive,
            Some(expected.iter().map(|value| value.naive_utc()).collect())
        );

        let primitive: Option<Vec<time::PrimitiveDateTime>> = list.as_rust_type().unwrap();
        assert_eq!(
            primitive,
            Some(vec![
                time::macros::datetime!(1970-01-01 00:00:01.5),
                time::macros::datetime!(1969-12-31 23:59:58.5),
            ])
        );
    }

    #[test]
    fn converts_elements_to_non_zero_integers() {
        let list = list_of(ColType::Int, vec![int_bytes(1), int_bytes(-2)]);
        let converted: Option<Vec<std::num::NonZeroI32>> = list.as_rust_type().unwrap();
        assert_eq!(
            converted,
            Some(vec![
                std::num::NonZeroI32::new(1).unwrap(),
                std::num::NonZeroI32::new(-2).unwrap(),
            ])
        );

        let list = list_of(ColType::Bigint, vec![bigint_bytes(7)]);
        let converted: Option<Vec<std::num::NonZeroI64>> = list.as_rust_type().unwrap();
        assert_eq!(converted, Some(vec![std::num::NonZeroI64::new(7).unwrap()]));
    }

    #[test]
    fn converts_duration_elements() {
        // (1, 2, 3) and (-1, -2, -3) as zig-zag encoded vints
        let data = vec![
            CBytes::new(vec![0x02, 0x04, 0x06]),
            CBytes::new(vec![0x01, 0x03, 0x05]),
        ];
        let expected = vec![
            Duration::new(1, 2, 3).unwrap(),
            Duration::new(-1, -2, -3).unwrap(),
        ];

        let native = list_of(ColType::Duration, data.clone());
        let converted: Option<Vec<Duration>> = native.as_rust_type().unwrap();
        assert_eq!(converted, Some(expected.clone()));

        // protocol v4 has no native duration type and sends the marshal class instead
        let custom = List::new(
            ColTypeOption {
                id: ColType::List,
                value: Some(ColTypeOptionValue::CList(Box::new(ColTypeOption {
                    id: ColType::Custom,
                    value: Some(ColTypeOptionValue::CString(
                        "org.apache.cassandra.db.marshal.DurationType".into(),
                    )),
                }))),
            },
            data,
            Version::V4,
        );
        let converted: Option<Vec<Duration>> = custom.as_rust_type().unwrap();
        assert_eq!(converted, Some(expected));
    }
}
