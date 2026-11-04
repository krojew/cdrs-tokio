#![cfg(feature = "derive")]
mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::consistency::Consistency;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::frame::TryFromRow;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query::QueryValues;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::statement::StatementParamsBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::blob::Blob;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::IntoCdrsValue;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::{TryFromRow, TryFromUdt};
#[cfg(feature = "e2e-tests")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use maplit::hashmap;
#[cfg(feature = "e2e-tests")]
use std::collections::HashMap;
#[cfg(feature = "e2e-tests")]
use std::str::FromStr;
#[cfg(feature = "e2e-tests")]
use time::PrimitiveDateTime;
#[cfg(feature = "e2e-tests")]
use uuid::Uuid;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn simple_udt_v4() {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.derive_udt (my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derived_udt \
         (my_key int PRIMARY KEY, my_udt derive_udt, my_uuid uuid, my_blob blob)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V4)
        .await
        .expect("setup");

    simple_udt_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn simple_udt_v5() {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.derive_udt (my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derived_udt \
         (my_key int PRIMARY KEY, my_udt derive_udt, my_uuid uuid, my_blob blob)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V5)
        .await
        .expect("setup");

    simple_udt_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn simple_udt_test(session: CurrentSession) {
    #[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
    struct RowStruct {
        my_key: i32,
        my_udt: MyUdt,
        my_uuid: Uuid,
        my_blob: Blob,
    }

    impl RowStruct {
        fn into_query_values(self) -> QueryValues {
            query_values!("my_key" => self.my_key, "my_udt" => self.my_udt, "my_uuid" => self.my_uuid, "my_blob" => self.my_blob)
        }
    }

    #[derive(Debug, Clone, PartialEq, IntoCdrsValue, TryFromUdt)]
    struct MyUdt {
        pub my_text: String,
    }

    let row_struct = RowStruct {
        my_key: 1i32,
        my_udt: MyUdt {
            my_text: "my_text".to_string(),
        },
        my_uuid: Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap(),
        my_blob: Blob::new(vec![]),
    };

    let cql = "INSERT INTO cdrs_test.test_derived_udt \
               (my_key, my_udt, my_uuid, my_blob) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(cql, row_struct.clone().into_query_values())
        .await
        .expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_derived_udt";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_udt_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        assert_eq!(my_udt_row, row_struct);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn nested_udt_v4() {
    let create_type1_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_inner_udt (my_text text)";
    let create_type2_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_outer_udt \
                            (my_inner_udt frozen<nested_inner_udt>)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_udt \
                            (my_key int PRIMARY KEY, my_outer_udt nested_outer_udt)";
    let session = setup_multiple(
        &[create_type1_cql, create_type2_cql, create_table_cql],
        Version::V4,
    )
    .await
    .expect("setup");

    nested_udt_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn nested_udt_v5() {
    let create_type1_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_inner_udt (my_text text)";
    let create_type2_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_outer_udt \
                            (my_inner_udt frozen<nested_inner_udt>)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_udt \
                            (my_key int PRIMARY KEY, my_outer_udt nested_outer_udt)";
    let session = setup_multiple(
        &[create_type1_cql, create_type2_cql, create_table_cql],
        Version::V5,
    )
    .await
    .expect("setup");

    nested_udt_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn nested_udt_test(session: CurrentSession) {
    #[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
    struct RowStruct {
        my_key: i32,
        my_outer_udt: MyOuterUdt,
    }

    impl RowStruct {
        fn into_query_values(self) -> QueryValues {
            query_values!("my_key" => self.my_key, "my_outer_udt" => self.my_outer_udt)
        }
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct MyInnerUdt {
        pub my_text: String,
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct MyOuterUdt {
        pub my_inner_udt: MyInnerUdt,
    }

    let row_struct = RowStruct {
        my_key: 0,
        my_outer_udt: MyOuterUdt {
            my_inner_udt: MyInnerUdt {
                my_text: "my_text".to_string(),
            },
        },
    };

    let cql = "INSERT INTO cdrs_test.test_nested_udt \
               (my_key, my_outer_udt) VALUES (?, ?)";
    session
        .query_with_values(cql, row_struct.clone().into_query_values())
        .await
        .expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_nested_udt";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_row_struct: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        assert_eq!(my_row_struct, row_struct);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn alter_udt_add_v4() {
    let drop_table_cql = "DROP TABLE IF EXISTS cdrs_test.test_alter_udt_add";
    let drop_type_cql = "DROP TYPE IF EXISTS cdrs_test.alter_udt_add_udt";
    let create_type_cql = "CREATE TYPE cdrs_test.alter_udt_add_udt (my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_alter_udt_add \
                            (my_key int PRIMARY KEY, my_map frozen<map<text, alter_udt_add_udt>>)";
    let session = setup_multiple(
        &[
            drop_table_cql,
            drop_type_cql,
            create_type_cql,
            create_table_cql,
        ],
        Version::V4,
    )
    .await
    .expect("setup");

    alter_udt_add_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn alter_udt_add_v5() {
    let drop_table_cql = "DROP TABLE IF EXISTS cdrs_test.test_alter_udt_add";
    let drop_type_cql = "DROP TYPE IF EXISTS cdrs_test.alter_udt_add_udt";
    let create_type_cql = "CREATE TYPE cdrs_test.alter_udt_add_udt (my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_alter_udt_add \
                            (my_key int PRIMARY KEY, my_map frozen<map<text, alter_udt_add_udt>>)";
    let session = setup_multiple(
        &[
            drop_table_cql,
            drop_type_cql,
            create_type_cql,
            create_table_cql,
        ],
        Version::V5,
    )
    .await
    .expect("setup");

    alter_udt_add_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn alter_udt_add_test(session: CurrentSession) {
    #[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
    struct RowStruct {
        my_key: i32,
        my_map: HashMap<String, MyUdtA>,
    }

    impl RowStruct {
        fn into_query_values(self) -> QueryValues {
            query_values!("my_key" => self.my_key, "my_map" => self.my_map)
        }
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct MyUdtA {
        pub my_text: String,
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
    struct RowStructB {
        my_key: i32,
        my_map: HashMap<String, MyUdtB>,
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct MyUdtB {
        pub my_text: String,
        pub my_timestamp: Option<PrimitiveDateTime>,
    }

    let row_struct = RowStruct {
        my_key: 0,
        my_map: hashmap! { "1".to_string() => MyUdtA {my_text: "my_text".to_string()} },
    };

    let cql = "INSERT INTO cdrs_test.test_alter_udt_add \
               (my_key, my_map) VALUES (?, ?)";
    session
        .query_with_values(cql, row_struct.clone().into_query_values())
        .await
        .expect("insert");

    let cql = "ALTER TYPE cdrs_test.alter_udt_add_udt ADD my_timestamp timestamp";
    session.query(cql).await.expect("alter type");

    let expected_nested_udt = MyUdtB {
        my_text: "my_text".to_string(),
        my_timestamp: None,
    };

    let cql = "SELECT * FROM cdrs_test.test_alter_udt_add";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let altered_row: RowStructB = RowStructB::try_from_row(row).expect("into RowStructB");
        assert_eq!(
            altered_row.my_map,
            hashmap! { "1".to_string() => expected_nested_udt.clone() }
        );
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn update_list_with_udt_v4() {
    let drop_table_cql = "DROP TABLE IF EXISTS cdrs_test.update_list_with_udt";
    let drop_type_cql = "DROP TYPE IF EXISTS cdrs_test.update_list_with_udt";
    let create_type_cql = "CREATE TYPE cdrs_test.update_list_with_udt (id uuid,
    text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.update_list_with_udt \
         (id uuid PRIMARY KEY, udts_set set<frozen<cdrs_test.update_list_with_udt>>)";
    let session = setup_multiple(
        &[
            drop_table_cql,
            drop_type_cql,
            create_type_cql,
            create_table_cql,
        ],
        Version::V4,
    )
    .await
    .expect("setup");

    update_list_with_udt_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn update_list_with_udt_v5() {
    let drop_table_cql = "DROP TABLE IF EXISTS cdrs_test.update_list_with_udt";
    let drop_type_cql = "DROP TYPE IF EXISTS cdrs_test.update_list_with_udt";
    let create_type_cql = "CREATE TYPE cdrs_test.update_list_with_udt (id uuid,
    text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.update_list_with_udt \
         (id uuid PRIMARY KEY, udts_set set<frozen<cdrs_test.update_list_with_udt>>)";
    let session = setup_multiple(
        &[
            drop_table_cql,
            drop_type_cql,
            create_type_cql,
            create_table_cql,
        ],
        Version::V5,
    )
    .await
    .expect("setup");

    update_list_with_udt_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn update_list_with_udt_test(session: CurrentSession) {
    #[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
    struct RowStruct {
        id: Uuid,
        udts_set: Vec<MyUdt>,
    }

    impl RowStruct {
        fn into_query_values(self) -> QueryValues {
            query_values!("id" => self.id, "udts_set" => self.udts_set)
        }
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct MyUdt {
        pub id: Uuid,
        pub text: String,
    }

    let row_struct = RowStruct {
        id: Uuid::parse_str("5bd8877a-e2b2-4d6f-aafd-c3f72a6964cf").expect("row id"),
        udts_set: vec![MyUdt {
            id: Uuid::parse_str("08f49fa5-934b-4aff-8a87-f3a3287296ba").expect("udt id"),
            text: "text".into(),
        }],
    };

    let cql = "INSERT INTO cdrs_test.update_list_with_udt \
               (id, udts_set) VALUES (?, ?)";
    session
        .query_with_values(cql, row_struct.clone().into_query_values())
        .await
        .expect("insert");

    let query = session
        .prepare("UPDATE cdrs_test.update_list_with_udt SET udts_set = udts_set + ? WHERE id = ?")
        .await
        .expect("prepare query");
    let params = StatementParamsBuilder::new()
        .with_consistency(Consistency::Quorum)
        .with_values(query_values!(
            vec![MyUdt {
                id: Uuid::parse_str("68f49fa5-934b-4aff-8a87-f3a32872a6ba").expect("udt id"),
                text: "abc".into(),
            }],
            Uuid::parse_str("5bd8877a-e2b2-4d6f-aafd-c3f72a6964cf").unwrap()
        ));
    session
        .exec_with_params(&query, &params.build())
        .await
        .expect("update set");

    let expected_row_struct = RowStruct {
        id: Uuid::parse_str("5bd8877a-e2b2-4d6f-aafd-c3f72a6964cf").expect("row id"),
        udts_set: vec![
            MyUdt {
                id: Uuid::parse_str("08f49fa5-934b-4aff-8a87-f3a3287296ba").expect("udt id"),
                text: "text".into(),
            },
            MyUdt {
                id: Uuid::parse_str("68f49fa5-934b-4aff-8a87-f3a32872a6ba").expect("udt id"),
                text: "abc".into(),
            },
        ],
    };

    let cql = "SELECT * FROM cdrs_test.update_list_with_udt";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let altered_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        assert_eq!(altered_row, expected_row_struct);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn collections_and_timestamps_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derive_collections \
               (my_key int PRIMARY KEY, my_list list<int>, my_set set<text>, \
               my_map map<text, int>, my_timestamps list<timestamp>, my_utc timestamp)";
    let session = setup(cql, Version::V4).await.expect("setup");

    collections_and_timestamps_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn collections_and_timestamps_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derive_collections \
               (my_key int PRIMARY KEY, my_list list<int>, my_set set<text>, \
               my_map map<text, int>, my_timestamps list<timestamp>, my_utc timestamp)";
    let session = setup(cql, Version::V5).await.expect("setup");

    collections_and_timestamps_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn collections_and_timestamps_test(session: CurrentSession) {
    #[derive(Clone, Debug, TryFromRow, PartialEq)]
    struct RowStruct {
        my_key: i32,
        my_list: Vec<i32>,
        my_set: Vec<String>,
        my_map: HashMap<String, i32>,
        my_timestamps: Vec<NaiveDateTime>,
        my_utc: Option<DateTime<Utc>>,
    }

    let timestamp = |year, month, day, milli| -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_milli_opt(12, 34, 56, milli))
            .expect("valid timestamp")
    };

    let filled = RowStruct {
        my_key: 2,
        my_list: vec![3, 1, 2],
        my_set: vec!["a".to_string(), "b".to_string()],
        my_map: hashmap! { "x".to_string() => 1, "y".to_string() => 2 },
        my_timestamps: vec![
            timestamp(1969, 7, 20, 123),
            timestamp(1970, 1, 1, 0),
            timestamp(2024, 2, 29, 999),
        ],
        my_utc: Some(timestamp(1955, 11, 5, 1).and_utc()),
    };

    // Cassandra stores empty non-frozen collections as null, so both rows read back empty
    session
        .query("INSERT INTO cdrs_test.test_derive_collections (my_key) VALUES (0)")
        .await
        .expect("insert nulls");
    session
        .query(
            "INSERT INTO cdrs_test.test_derive_collections \
             (my_key, my_list, my_set, my_map, my_timestamps) VALUES (1, [], {}, {}, [])",
        )
        .await
        .expect("insert empty collections");
    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_derive_collections \
             (my_key, my_list, my_set, my_map, my_timestamps, my_utc) VALUES (?, ?, ?, ?, ?, ?)",
            query_values!(
                filled.my_key,
                filled.my_list.clone(),
                filled.my_set.clone(),
                filled.my_map.clone(),
                filled.my_timestamps.clone(),
                filled.my_utc
            ),
        )
        .await
        .expect("insert filled row");

    let rows = session
        .query("SELECT * FROM cdrs_test.test_derive_collections")
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 3);

    let mut rows: Vec<RowStruct> = rows
        .into_iter()
        .map(|row| RowStruct::try_from_row(row).expect("into RowStruct"))
        .collect();
    rows.sort_by_key(|row| row.my_key);

    for (key, row) in rows.iter().take(2).enumerate() {
        assert_eq!(
            row,
            &RowStruct {
                my_key: key as i32,
                my_list: vec![],
                my_set: vec![],
                my_map: HashMap::new(),
                my_timestamps: vec![],
                my_utc: None,
            }
        );
    }
    assert_eq!(rows[2], filled);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn udt_with_null_fields_v4() {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.derive_optional_udt \
                           (my_text text, my_int int, my_utc timestamp)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derive_optional_udt \
                            (my_key int PRIMARY KEY, my_udt frozen<derive_optional_udt>)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V4)
        .await
        .expect("setup");

    udt_with_null_fields_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn udt_with_null_fields_v5() {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.derive_optional_udt \
                           (my_text text, my_int int, my_utc timestamp)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derive_optional_udt \
                            (my_key int PRIMARY KEY, my_udt frozen<derive_optional_udt>)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V5)
        .await
        .expect("setup");

    udt_with_null_fields_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn udt_with_null_fields_test(session: CurrentSession) {
    #[derive(Clone, Debug, TryFromRow, PartialEq)]
    struct RowStruct {
        my_key: i32,
        my_udt: OptionalUdt,
    }

    #[derive(Clone, Debug, IntoCdrsValue, TryFromUdt, PartialEq)]
    struct OptionalUdt {
        my_text: Option<String>,
        my_int: Option<i32>,
        my_utc: Option<DateTime<Utc>>,
    }

    let expected = vec![
        RowStruct {
            my_key: 0,
            my_udt: OptionalUdt {
                my_text: None,
                my_int: None,
                my_utc: None,
            },
        },
        RowStruct {
            my_key: 1,
            my_udt: OptionalUdt {
                my_text: Some("my_text".to_string()),
                my_int: None,
                my_utc: DateTime::from_timestamp_millis(-1_234_567),
            },
        },
        RowStruct {
            my_key: 2,
            my_udt: OptionalUdt {
                my_text: None,
                my_int: Some(-7),
                my_utc: None,
            },
        },
    ];

    for row in &expected {
        session
            .query_with_values(
                "INSERT INTO cdrs_test.test_derive_optional_udt (my_key, my_udt) VALUES (?, ?)",
                query_values!(row.my_key, row.my_udt.clone()),
            )
            .await
            .expect("insert");
    }

    let rows = session
        .query("SELECT * FROM cdrs_test.test_derive_optional_udt")
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    let mut rows: Vec<RowStruct> = rows
        .into_iter()
        .map(|row| RowStruct::try_from_row(row).expect("into RowStruct"))
        .collect();
    rows.sort_by_key(|row| row.my_key);

    assert_eq!(rows, expected);
}
