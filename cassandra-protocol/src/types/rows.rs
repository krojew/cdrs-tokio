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
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::udt::Udt;
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
