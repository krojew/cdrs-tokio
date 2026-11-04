use chrono::prelude::*;
use num_bigint::BigInt;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::{column_is_empty_err, Error, Result};
use crate::frame::message_result::{CTuple, ColType, ColTypeOption, ColTypeOptionValue};
use crate::frame::Version;
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::duration::Duration;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::udt::Udt;
use crate::types::{ByIndex, CBytes, IntoRustByIndex};

#[derive(Debug)]
pub struct Tuple {
    data: Vec<(ColTypeOption, CBytes)>,
    protocol_version: Version,
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Tuple) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        for (s, o) in self.data.iter().zip(other.data.iter()) {
            if s.1 != o.1 {
                return false;
            }
        }
        true
    }
}

impl Eq for Tuple {}

impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for data in &self.data {
            data.1.hash(state);
        }
    }
}

impl Tuple {
    pub fn new(elements: Vec<CBytes>, metadata: &CTuple, protocol_version: Version) -> Tuple {
        Tuple {
            data: metadata
                .types
                .iter()
                .zip(elements)
                .map(|(val_type, val_b)| (val_type.clone(), val_b))
                .collect(),
            protocol_version,
        }
    }
}

impl ByIndex for Tuple {}

into_rust_by_index!(Tuple, Blob);
into_rust_by_index!(Tuple, String);
into_rust_by_index!(Tuple, bool);
into_rust_by_index!(Tuple, i64);
into_rust_by_index!(Tuple, i32);
into_rust_by_index!(Tuple, i16);
into_rust_by_index!(Tuple, i8);
into_rust_by_index!(Tuple, f64);
into_rust_by_index!(Tuple, f32);
into_rust_by_index!(Tuple, IpAddr);
into_rust_by_index!(Tuple, Uuid);
into_rust_by_index!(Tuple, List);
into_rust_by_index!(Tuple, Map);
into_rust_by_index!(Tuple, Udt);
into_rust_by_index!(Tuple, Tuple);
into_rust_by_index!(Tuple, PrimitiveDateTime);
into_rust_by_index!(Tuple, Decimal);
into_rust_by_index!(Tuple, NaiveDateTime);
into_rust_by_index!(Tuple, DateTime<Utc>);
into_rust_by_index!(Tuple, BigInt);
into_rust_by_index!(Tuple, Duration);

tuple_as_cassandra_type!();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_duration_elements() {
        let metadata = CTuple {
            types: vec![
                ColTypeOption {
                    id: ColType::Duration,
                    value: None,
                },
                ColTypeOption {
                    id: ColType::Custom,
                    value: Some(ColTypeOptionValue::CString(
                        "org.apache.cassandra.db.marshal.DurationType".into(),
                    )),
                },
                ColTypeOption {
                    id: ColType::Duration,
                    value: None,
                },
            ],
        };
        // (1, 2, 3) and (-1, -2, -3) as zig-zag encoded vints
        let tuple = Tuple::new(
            vec![
                CBytes::new(vec![0x02, 0x04, 0x06]),
                CBytes::new(vec![0x01, 0x03, 0x05]),
                CBytes::new_null(),
            ],
            &metadata,
            Version::V4,
        );

        let native: Duration = tuple.r_by_index(0).unwrap();
        assert_eq!(native, Duration::new(1, 2, 3).unwrap());

        let custom: Duration = tuple.r_by_index(1).unwrap();
        assert_eq!(custom, Duration::new(-1, -2, -3).unwrap());

        let missing: Option<Duration> = tuple.by_index(2).unwrap();
        assert_eq!(missing, None);
    }
}
