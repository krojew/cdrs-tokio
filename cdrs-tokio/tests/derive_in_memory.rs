#![cfg(feature = "derive")]
//! Derive macro tests that decode rows and UDT values built in memory, so they don't need a live
//! Cassandra node.

use cdrs_tokio::frame::message_result::{
    BodyResResultRows, CUdt, ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata,
    RowsMetadataFlags,
};
use cdrs_tokio::frame::{Serialize, TryFromRow, TryFromUdt, Version};
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::from_cdrs::FromCdrsByName;
use cdrs_tokio::types::list::List;
use cdrs_tokio::types::map::Map;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::{Bytes, Value};
use cdrs_tokio::types::CBytes;
use cdrs_tokio::{DbMirror, IntoCdrsValue, TryFromRow, TryFromUdt};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::Cursor;
use std::num::{NonZeroI32, NonZeroI64};

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

fn udt_of(fields: Vec<(&str, ColTypeOption)>) -> ColTypeOption {
    ColTypeOption {
        id: ColType::Udt,
        value: Some(ColTypeOptionValue::UdtType(CUdt {
            ks: "ks".to_string(),
            udt_name: "udt".to_string(),
            descriptions: fields
                .into_iter()
                .map(|(name, col_type)| (name.to_string(), col_type))
                .collect(),
        })),
    }
}

/// Serializes UDT field values in the given order.
fn udt(fields: Vec<Value>) -> Bytes {
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);
    for field in fields {
        field.serialize(&mut cursor, Version::V4);
    }

    Bytes::new(bytes)
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

type FixedState = BuildHasherDefault<DefaultHasher>;

#[derive(Debug, PartialEq, TryFromRow)]
struct CustomHasherRow {
    map: HashMap<String, i32, FixedState>,
}

#[test]
fn map_with_custom_hasher_decodes() {
    let mut map = HashMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    let row = row(vec![(
        "map",
        map_of(native(ColType::Varchar), native(ColType::Int)),
        value(map.clone()),
    )]);

    let decoded = CustomHasherRow::try_from_row(row).unwrap();

    assert_eq!(decoded.map.len(), 2);
    assert_eq!(decoded.map["a"], 1);
    assert_eq!(decoded.map["b"], 2);
}

#[derive(Debug, PartialEq, TryFromRow)]
struct TimeAndNonZeroRow {
    naive: NaiveDateTime,
    utc: DateTime<Utc>,
    optional_naive: Option<NaiveDateTime>,
    optional_utc: Option<DateTime<Utc>>,
    missing_naive: Option<NaiveDateTime>,
    missing_utc: Option<DateTime<Utc>>,
    non_zero: NonZeroI32,
    optional_non_zero: Option<NonZeroI64>,
}

#[test]
fn time_and_non_zero_types_decode() {
    let utc = DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
    let naive = utc.naive_utc();
    let row = row(vec![
        ("naive", native(ColType::Timestamp), value(naive)),
        ("utc", native(ColType::Timestamp), value(utc)),
        ("optional_naive", native(ColType::Timestamp), value(naive)),
        ("optional_utc", native(ColType::Timestamp), value(utc)),
        ("missing_naive", native(ColType::Timestamp), null()),
        ("missing_utc", native(ColType::Timestamp), null()),
        ("non_zero", native(ColType::Int), value(7)),
        ("optional_non_zero", native(ColType::Bigint), value(8i64)),
    ]);

    let decoded = TimeAndNonZeroRow::try_from_row(row).unwrap();

    assert_eq!(
        decoded,
        TimeAndNonZeroRow {
            naive,
            utc,
            optional_naive: Some(naive),
            optional_utc: Some(utc),
            missing_naive: None,
            missing_utc: None,
            non_zero: NonZeroI32::new(7).unwrap(),
            optional_non_zero: NonZeroI64::new(8),
        }
    );
}

#[derive(Debug, PartialEq, TryFromUdt)]
struct Leaf {
    text: String,
}

#[derive(Debug, PartialEq, TryFromUdt)]
struct Labeled<T: TryFromUdt> {
    label: String,
    inner: T,
}

#[derive(Debug, PartialEq, TryFromRow)]
struct GenericRow<T>
where
    T: TryFromUdt,
{
    single: Labeled<T>,
    list: Vec<Labeled<T>>,
    optional: Option<Labeled<T>>,
    map: HashMap<String, Labeled<T>>,
}

fn labeled_type() -> ColTypeOption {
    udt_of(vec![
        ("label", native(ColType::Varchar)),
        ("inner", udt_of(vec![("text", native(ColType::Varchar))])),
    ])
}

fn labeled_value(label: &str, text: &str) -> Bytes {
    udt(vec![
        Value::new(label),
        Value::new(udt(vec![Value::new(text)])),
    ])
}

fn labeled_leaf(label: &str, text: &str) -> Labeled<Leaf> {
    Labeled {
        label: label.to_string(),
        inner: Leaf {
            text: text.to_string(),
        },
    }
}

#[test]
fn generic_structs_decode() {
    let mut map = HashMap::new();
    map.insert("key".to_string(), labeled_value("d", "w"));
    let row = row(vec![
        ("single", labeled_type(), value(labeled_value("a", "z"))),
        (
            "list",
            list_of(labeled_type()),
            value(vec![labeled_value("b", "y")]),
        ),
        ("optional", labeled_type(), value(labeled_value("c", "x"))),
        (
            "map",
            map_of(native(ColType::Varchar), labeled_type()),
            value(map),
        ),
    ]);

    let decoded = GenericRow::<Leaf>::try_from_row(row).unwrap();

    let mut expected_map = HashMap::new();
    expected_map.insert("key".to_string(), labeled_leaf("d", "w"));
    assert_eq!(
        decoded,
        GenericRow {
            single: labeled_leaf("a", "z"),
            list: vec![labeled_leaf("b", "y")],
            optional: Some(labeled_leaf("c", "x")),
            map: expected_map,
        }
    );
}

#[derive(IntoCdrsValue)]
struct BorrowedUdt<'a, T: Into<Bytes>> {
    name: &'a str,
    inner: T,
}

#[test]
fn generic_struct_converts_into_bytes() {
    let bytes: Bytes = BorrowedUdt {
        name: "x",
        inner: 5,
    }
    .into();

    assert_eq!(
        bytes.into_inner(),
        udt(vec![Value::new("x"), Value::new(5)]).into_inner()
    );
}

#[derive(DbMirror)]
struct Mirrored<'a> {
    id: i32,
    name: &'a str,
}

#[test]
fn generic_struct_mirrors_db() {
    assert_eq!(
        Mirrored::insert_query(),
        "insert into Mirrored(id, name) values (?, ?)"
    );

    match (Mirrored { id: 1, name: "x" }).into_query_values() {
        QueryValues::NamedValues(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values["id"], Value::new(1));
            assert_eq!(values["name"], Value::new("x"));
        }
        other => panic!("unexpected query values: {:?}", other),
    }
}

#[derive(DbMirror)]
struct r#RawNames {
    id: i32,
    r#type: i32,
}

#[test]
fn raw_identifiers_are_unescaped_in_db_mirror() {
    assert_eq!(
        RawNames::insert_query(),
        "insert into RawNames(id, type) values (?, ?)"
    );

    match (RawNames { id: 1, r#type: 2 }).into_query_values() {
        QueryValues::NamedValues(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values["id"], Value::new(1));
            assert_eq!(values["type"], Value::new(2));
        }
        other => panic!("unexpected query values: {:?}", other),
    }
}

/// Shadows the crate names and prelude items that generated code could otherwise resolve to.
#[allow(dead_code)]
mod shadowed_names {
    mod cdrs_tokio {}
    mod core {}
    mod std {}

    struct Option;
    struct Some;
    struct None;
    struct Result;
    struct Ok;
    struct Err;
    struct Vec;
    struct String;
    struct HashMap;
    struct Default;
    struct ToString;
    trait From {}
    trait Into {}

    #[derive(Debug, PartialEq, ::cdrs_tokio::TryFromUdt, ::cdrs_tokio::IntoCdrsValue)]
    pub struct ShadowedUdt {
        pub text: ::std::string::String,
        pub number: ::std::option::Option<i32>,
    }

    #[derive(Debug, PartialEq, ::cdrs_tokio::TryFromRow, ::cdrs_tokio::DbMirror)]
    pub struct ShadowedRow {
        pub id: i32,
        pub text: ::std::option::Option<::std::string::String>,
        pub list: ::std::vec::Vec<i32>,
        pub map: ::std::collections::HashMap<::std::string::String, i32>,
        pub udt: ShadowedUdt,
        pub udts: ::std::vec::Vec<ShadowedUdt>,
        pub optional_udt: ::std::option::Option<ShadowedUdt>,
    }
}

#[test]
fn shadowed_names_do_not_affect_generated_code() {
    use shadowed_names::{ShadowedRow, ShadowedUdt};

    let udt_type = udt_of(vec![
        ("text", native(ColType::Varchar)),
        ("number", native(ColType::Int)),
    ]);
    let udt_value = || ShadowedUdt {
        text: "u".to_string(),
        number: Some(2),
    };
    let mut map = HashMap::new();
    map.insert("a".to_string(), 1);
    let row = row(vec![
        ("id", native(ColType::Int), value(1)),
        ("text", native(ColType::Varchar), value("t")),
        ("list", list_of(native(ColType::Int)), value(vec![1])),
        (
            "map",
            map_of(native(ColType::Varchar), native(ColType::Int)),
            value(map.clone()),
        ),
        ("udt", udt_type.clone(), value(udt_value())),
        ("udts", list_of(udt_type.clone()), value(vec![udt_value()])),
        ("optional_udt", udt_type, null()),
    ]);

    let decoded = ShadowedRow::try_from_row(row).unwrap();

    let expected = ShadowedRow {
        id: 1,
        text: Some("t".to_string()),
        list: vec![1],
        map,
        udt: udt_value(),
        udts: vec![udt_value()],
        optional_udt: None,
    };
    assert_eq!(decoded, expected);
    assert_eq!(
        ShadowedRow::insert_query(),
        "insert into ShadowedRow(id, text, list, map, udt, udts, optional_udt) \
         values (?, ?, ?, ?, ?, ?, ?)"
    );
    match expected.into_query_values() {
        QueryValues::NamedValues(values) => {
            assert_eq!(values.len(), 7);
            assert_eq!(values["udt"], Value::new(udt_value()));
        }
        other => panic!("unexpected query values: {:?}", other),
    }
}

/// Field types are only reachable through their paths here, since nothing is imported.
mod qualified_paths {
    pub mod udts {
        #[derive(Debug, PartialEq, cdrs_tokio::TryFromUdt)]
        pub struct Inner {
            pub text: String,
        }
    }

    #[derive(Debug, PartialEq, cdrs_tokio::TryFromRow)]
    pub struct QualifiedRow {
        pub id: uuid::Uuid,
        pub address: std::net::IpAddr,
        pub ids: Vec<uuid::Uuid>,
        pub optional_address: Option<std::net::IpAddr>,
        pub map: std::collections::HashMap<String, uuid::Uuid>,
        pub when: chrono::NaiveDateTime,
        pub optional_when: Option<chrono::DateTime<chrono::Utc>>,
        pub non_zero: std::num::NonZeroI16,
        pub inner: udts::Inner,
        pub inners: Vec<self::udts::Inner>,
    }
}

#[test]
fn qualified_field_types_decode() {
    use qualified_paths::{udts::Inner, QualifiedRow};
    use std::net::{IpAddr, Ipv4Addr};
    use std::num::NonZeroI16;
    use uuid::Uuid;

    let id = Uuid::from_u128(1);
    let address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let when = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let mut map = HashMap::new();
    map.insert("a".to_string(), id);
    let inner_type = udt_of(vec![("text", native(ColType::Varchar))]);
    let row = row(vec![
        ("id", native(ColType::Uuid), value(id)),
        ("address", native(ColType::Inet), value(address)),
        ("ids", list_of(native(ColType::Uuid)), value(vec![id])),
        ("optional_address", native(ColType::Inet), value(address)),
        (
            "map",
            map_of(native(ColType::Varchar), native(ColType::Uuid)),
            value(map.clone()),
        ),
        ("when", native(ColType::Timestamp), value(when.naive_utc())),
        ("optional_when", native(ColType::Timestamp), value(when)),
        ("non_zero", native(ColType::Smallint), value(3i16)),
        (
            "inner",
            inner_type.clone(),
            value(udt(vec![Value::new("in")])),
        ),
        (
            "inners",
            list_of(inner_type),
            value(vec![udt(vec![Value::new("list")])]),
        ),
    ]);

    let decoded = QualifiedRow::try_from_row(row).unwrap();

    assert_eq!(
        decoded,
        QualifiedRow {
            id,
            address,
            ids: vec![id],
            optional_address: Some(address),
            map,
            when: when.naive_utc(),
            optional_when: Some(when),
            non_zero: NonZeroI16::new(3).unwrap(),
            inner: Inner {
                text: "in".to_string(),
            },
            inners: vec![Inner {
                text: "list".to_string(),
            }],
        }
    );
}
