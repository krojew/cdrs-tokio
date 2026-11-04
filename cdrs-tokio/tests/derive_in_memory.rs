#![cfg(feature = "derive")]
//! Derive macro tests that decode rows and UDT values built in memory, so they don't need a live
//! Cassandra node.

use cdrs_tokio::frame::message_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata,
    RowsMetadataFlags,
};
use cdrs_tokio::frame::{TryFromRow, Version};
use cdrs_tokio::types::from_cdrs::FromCdrsByName;
use cdrs_tokio::types::list::List;
use cdrs_tokio::types::map::Map;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Bytes;
use cdrs_tokio::types::CBytes;
use cdrs_tokio::TryFromRow;
use std::collections::HashMap;

fn native(id: ColType) -> ColTypeOption {
    ColTypeOption { id, value: None }
}

fn list_of(item: ColTypeOption) -> ColTypeOption {
    ColTypeOption {
        id: ColType::List,
        value: Some(ColTypeOptionValue::CList(Box::new(item))),
    }
}

fn map_of(key: ColTypeOption, value: ColTypeOption) -> ColTypeOption {
    ColTypeOption {
        id: ColType::Map,
        value: Some(ColTypeOptionValue::CMap(Box::new(key), Box::new(value))),
    }
}

fn value<T: Into<Bytes>>(value: T) -> CBytes {
    CBytes::new(value.into().into_inner())
}

fn null() -> CBytes {
    CBytes::new_null()
}

fn row(columns: Vec<(&str, ColTypeOption, CBytes)>) -> Row {
    let mut col_specs = Vec::with_capacity(columns.len());
    let mut content = Vec::with_capacity(columns.len());
    for (name, col_type, data) in columns {
        col_specs.push(ColSpec {
            table_spec: None,
            name: name.to_string(),
            col_type,
        });
        content.push(data);
    }

    let body = BodyResResultRows {
        metadata: RowsMetadata {
            flags: RowsMetadataFlags::empty(),
            columns_count: col_specs.len() as i32,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs,
        },
        rows_count: 1,
        rows_content: vec![content],
        protocol_version: Version::V4,
    };

    Row::from_body(body).pop().expect("one row")
}

#[derive(Debug, PartialEq, TryFromRow)]
struct CollectionRow {
    list: Vec<i32>,
    map: HashMap<String, i32>,
}

fn collection_row(list: CBytes, map: CBytes) -> Row {
    row(vec![
        ("list", list_of(native(ColType::Int)), list),
        (
            "map",
            map_of(native(ColType::Varchar), native(ColType::Int)),
            map,
        ),
    ])
}

#[test]
fn null_collections_decode_as_empty() {
    let decoded = CollectionRow::try_from_row(collection_row(null(), null())).unwrap();

    assert_eq!(
        decoded,
        CollectionRow {
            list: vec![],
            map: HashMap::new(),
        }
    );
}

#[test]
fn non_empty_collections_decode() {
    let mut map = HashMap::new();
    map.insert("a".to_string(), 1);

    let decoded =
        CollectionRow::try_from_row(collection_row(value(vec![1, 2]), value(map.clone()))).unwrap();

    assert_eq!(
        decoded,
        CollectionRow {
            list: vec![1, 2],
            map,
        }
    );
}

#[test]
fn collection_type_mismatch_error_is_propagated() {
    let row = row(vec![
        ("list", native(ColType::Int), value(1)),
        (
            "map",
            map_of(native(ColType::Varchar), native(ColType::Int)),
            null(),
        ),
    ]);
    let expected = <List as FromCdrsByName>::from_cdrs_by_name(&row, "list").unwrap_err();

    let error = CollectionRow::try_from_row(row).unwrap_err();

    assert_eq!(error.to_string(), expected.to_string());
}

#[test]
fn missing_collection_column_error_is_propagated() {
    let row = row(vec![("list", list_of(native(ColType::Int)), null())]);
    let expected = <Map as FromCdrsByName>::from_cdrs_by_name(&row, "map").unwrap_err();

    let error = CollectionRow::try_from_row(row).unwrap_err();

    assert_eq!(error.to_string(), expected.to_string());
}
