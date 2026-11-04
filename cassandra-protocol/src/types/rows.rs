use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};
use std::sync::Arc;

use chrono::prelude::*;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::{column_is_empty_err, Error, Result};
use crate::frame::message_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata,
};
use crate::frame::Version;
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::duration::Duration;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::udt::Udt;
use crate::types::vector::Vector;
use crate::types::{ByIndex, ByName, CBytes, IntoRustByIndex, IntoRustByName};
use num_bigint::BigInt;

#[derive(Clone, Debug)]
pub struct Row {
    metadata: Arc<RowsMetadata>,
    row_content: Vec<CBytes>,
    protocol_version: Version,
}

impl Row {
    pub fn from_body(body: BodyResResultRows) -> Vec<Row> {
        let metadata = Arc::new(body.metadata);
        let protocol_version = body.protocol_version;
        body.rows_content
            .into_iter()
            .map(|row| Row {
                metadata: metadata.clone(),
                row_content: row,
                protocol_version,
            })
            .collect()
    }

    /// Checks if a column is present in the row.
    pub fn contains_column(&self, name: &str) -> bool {
        self.metadata
            .col_specs
            .iter()
            .any(|spec| spec.name.as_str() == name)
    }

    /// Checks for NULL for a given column. Returns false if given column does not exist.
    pub fn is_empty(&self, index: usize) -> bool {
        self.row_content
            .get(index)
            .map(|data| data.is_null_or_empty())
            .unwrap_or(false)
    }

    /// Checks for NULL for a given column. Returns false if given column does not exist.
    pub fn is_empty_by_name(&self, name: &str) -> bool {
        self.metadata
            .col_specs
            .iter()
            .position(|spec| spec.name.as_str() == name)
            .map(|index| self.is_empty(index))
            .unwrap_or(false)
    }

    fn col_spec_by_name(&self, name: &str) -> Option<(&ColSpec, &CBytes)> {
        self.metadata
            .col_specs
            .iter()
            .position(|spec| spec.name.as_str() == name)
            .map(|i| {
                let col_spec = &self.metadata.col_specs[i];
                let data = &self.row_content[i];
                (col_spec, data)
            })
    }

    fn col_spec_by_index(&self, index: usize) -> Option<(&ColSpec, &CBytes)> {
        let specs = self.metadata.col_specs.iter();
        let values = self.row_content.iter();
        specs.zip(values).nth(index)
    }
}

impl ByName for Row {}

into_rust_by_name!(Row, Blob);
into_rust_by_name!(Row, String);
into_rust_by_name!(Row, bool);
into_rust_by_name!(Row, i64);
into_rust_by_name!(Row, i32);
into_rust_by_name!(Row, i16);
into_rust_by_name!(Row, i8);
into_rust_by_name!(Row, f64);
into_rust_by_name!(Row, f32);
into_rust_by_name!(Row, IpAddr);
into_rust_by_name!(Row, Uuid);
into_rust_by_name!(Row, List);
into_rust_by_name!(Row, Vector);
into_rust_by_name!(Row, Map);
into_rust_by_name!(Row, Udt);
into_rust_by_name!(Row, Tuple);
into_rust_by_name!(Row, PrimitiveDateTime);
into_rust_by_name!(Row, Decimal);
into_rust_by_name!(Row, NonZeroI8);
into_rust_by_name!(Row, NonZeroI16);
into_rust_by_name!(Row, NonZeroI32);
into_rust_by_name!(Row, NonZeroI64);
into_rust_by_name!(Row, NaiveDateTime);
into_rust_by_name!(Row, DateTime<Utc>);
into_rust_by_name!(Row, BigInt);
into_rust_by_name!(Row, Duration);

impl ByIndex for Row {}

into_rust_by_index!(Row, Blob);
into_rust_by_index!(Row, String);
into_rust_by_index!(Row, bool);
into_rust_by_index!(Row, i64);
into_rust_by_index!(Row, i32);
into_rust_by_index!(Row, i16);
into_rust_by_index!(Row, i8);
into_rust_by_index!(Row, f64);
into_rust_by_index!(Row, f32);
into_rust_by_index!(Row, IpAddr);
into_rust_by_index!(Row, Uuid);
into_rust_by_index!(Row, List);
into_rust_by_index!(Row, Vector);
into_rust_by_index!(Row, Map);
into_rust_by_index!(Row, Udt);
into_rust_by_index!(Row, Tuple);
into_rust_by_index!(Row, PrimitiveDateTime);
into_rust_by_index!(Row, Decimal);
into_rust_by_index!(Row, NonZeroI8);
into_rust_by_index!(Row, NonZeroI16);
into_rust_by_index!(Row, NonZeroI32);
into_rust_by_index!(Row, NonZeroI64);
into_rust_by_index!(Row, NaiveDateTime);
into_rust_by_index!(Row, DateTime<Utc>);
into_rust_by_index!(Row, BigInt);
into_rust_by_index!(Row, Duration);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::message_result::RowsMetadataFlags;
    use crate::types::from_cdrs::FromCdrsByName;
    use crate::types::AsRustType;

    fn row_with_column(class_name: &str, value: CBytes) -> Row {
        row_of("embedding", custom(class_name), value)
    }

    fn custom(class_name: &str) -> ColTypeOption {
        ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(class_name.into())),
        }
    }

    fn row_of(name: &str, col_type: ColTypeOption, value: CBytes) -> Row {
        let metadata = RowsMetadata {
            flags: RowsMetadataFlags::empty(),
            columns_count: 1,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs: vec![ColSpec {
                table_spec: None,
                name: name.into(),
                col_type,
            }],
        };

        Row::from_body(BodyResResultRows {
            metadata,
            rows_count: 1,
            rows_content: vec![vec![value]],
            protocol_version: Version::V5,
        })
        .remove(0)
    }

    const FLOAT_VECTOR_3: &str =
        "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 3)";

    #[test]
    fn reads_float_vector_column() {
        // vector<float, 3> [1.0, 2.5, -2.0]: fixed-size elements are concatenated without a count
        // or per-element lengths
        let row = row_with_column(
            FLOAT_VECTOR_3,
            CBytes::new(vec![
                0x3F, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00,
            ]),
        );

        let by_name: Vector = row.r_by_name("embedding").unwrap();
        let values: Vec<f32> = by_name.as_r_type().unwrap();
        assert_eq!(values, vec![1.0, 2.5, -2.0]);

        let by_index: Vector = row.r_by_index(0).unwrap();
        let values: Vec<f32> = by_index.as_r_type().unwrap();
        assert_eq!(values, vec![1.0, 2.5, -2.0]);
    }

    #[test]
    fn reads_null_vector_column() {
        let row = row_with_column(FLOAT_VECTOR_3, CBytes::new_null());

        let by_name: Option<Vector> = row.by_name("embedding").unwrap();
        assert!(by_name.is_none());
    }

    #[test]
    fn truncated_vector_column_is_error() {
        let row = row_with_column(
            FLOAT_VECTOR_3,
            CBytes::new(vec![0x3F, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00]),
        );

        let by_name: Result<Option<Vector>> = row.by_name("embedding");
        assert!(by_name.is_err());
    }

    #[test]
    fn non_float_vector_column_is_error() {
        let row = row_with_column(
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type, 2)",
            CBytes::new(vec![0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02]),
        );

        let by_name: Result<Option<Vector>> = row.by_name("embedding");
        assert!(by_name.is_err());
    }

    #[test]
    fn non_vector_custom_column_is_not_a_vector() {
        let row = row_with_column(
            "com.example.Pair(com.example.FloatType, 1)",
            CBytes::new(vec![0x3F, 0x80, 0x00, 0x00]),
        );

        let by_name: Result<Option<Vector>> = row.by_name("embedding");
        assert!(by_name.is_err());
    }

    // duration of 100 months, 200 days and 300 nanoseconds: three zig-zag encoded vints
    const DURATION_BYTES: [u8; 6] = [0x80, 0xC8, 0x81, 0x90, 0x82, 0x58];

    #[test]
    fn reads_duration_column() {
        let expected = Duration::new(100, 200, 300).unwrap();
        let row = row_of(
            "span",
            ColTypeOption {
                id: ColType::Duration,
                value: None,
            },
            CBytes::new(DURATION_BYTES.to_vec()),
        );

        let by_name: Duration = row.r_by_name("span").unwrap();
        assert_eq!(by_name, expected);

        let by_index: Duration = row.r_by_index(0).unwrap();
        assert_eq!(by_index, expected);

        let from_cdrs = <Duration as FromCdrsByName>::from_cdrs_r(&row, "span").unwrap();
        assert_eq!(from_cdrs, expected);
    }

    #[test]
    fn reads_custom_duration_column() {
        // protocol v4 has no native duration type and sends the marshal class instead
        let expected = Duration::new(100, 200, 300).unwrap();
        let row = row_of(
            "span",
            custom("org.apache.cassandra.db.marshal.DurationType"),
            CBytes::new(DURATION_BYTES.to_vec()),
        );

        let by_name: Duration = row.r_by_name("span").unwrap();
        assert_eq!(by_name, expected);

        let by_index: Duration = row.r_by_index(0).unwrap();
        assert_eq!(by_index, expected);
    }

    #[test]
    fn reads_null_duration_column() {
        let row = row_of(
            "span",
            ColTypeOption {
                id: ColType::Duration,
                value: None,
            },
            CBytes::new_null(),
        );

        let by_name: Option<Duration> = row.by_name("span").unwrap();
        assert_eq!(by_name, None);

        let by_index: Option<Duration> = row.by_index(0).unwrap();
        assert_eq!(by_index, None);
    }

    #[test]
    fn non_duration_column_is_not_a_duration() {
        let row = row_of(
            "span",
            custom("org.apache.cassandra.db.marshal.Int32Type"),
            CBytes::new(vec![0x00, 0x00, 0x00, 0x01]),
        );

        let by_name: Result<Option<Duration>> = row.by_name("span");
        assert!(by_name.is_err());
    }
}
