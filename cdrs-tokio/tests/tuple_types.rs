mod common;

#[cfg(feature = "e2e-tests")]
use common::*;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::error::Result;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::frame::Serialize;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::blob::Blob;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::tuple::Tuple;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::value::{Bytes, Value};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::{IntoRustByIndex, IntoRustByName};
#[cfg(feature = "e2e-tests")]
use std::io::Cursor;
#[cfg(feature = "e2e-tests")]
use std::str::FromStr;
#[cfg(feature = "e2e-tests")]
use time::{
    macros::{date, time},
    PrimitiveDateTime,
};
#[cfg(feature = "e2e-tests")]
use uuid::Uuid;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn simple_tuple() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.simple_tuple \
               (my_tuple tuple<text, int> PRIMARY KEY)";
    let session = setup(cql).await.expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyTuple {
        pub my_text: String,
        pub my_int: i32,
    }

    impl MyTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyTuple> {
            let my_text: String = tuple.get_r_by_index(0)?;
            let my_int: i32 = tuple.get_r_by_index(1)?;
            Ok(MyTuple { my_text, my_int })
        }
    }

    impl From<MyTuple> for Bytes {
        fn from(value: MyTuple) -> Bytes {
            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);
            let val_bytes: Bytes = value.my_text.into();
            Value::new(val_bytes).serialize(&mut cursor);
            let val_bytes: Bytes = value.my_int.into();
            Value::new(val_bytes).serialize(&mut cursor);
            Bytes::new(bytes)
        }
    }

    let my_tuple = MyTuple {
        my_text: "my_text".to_string(),
        my_int: 0,
    };
    let values = query_values!(my_tuple.clone());

    let cql = "INSERT INTO cdrs_test.simple_tuple \
               (my_tuple) VALUES (?)";
    session
        .query_with_values(cql, values)
        .await
        .expect("insert");

    let cql = "SELECT * FROM cdrs_test.simple_tuple";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_tuple_row: Tuple = row.get_r_by_name("my_tuple").expect("my_tuple");
        let my_tuple_row = MyTuple::try_from(my_tuple_row).expect("my_tuple as rust");
        assert_eq!(my_tuple_row, my_tuple);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn nested_tuples() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_tuples \
               (my_key int PRIMARY KEY, \
               my_outer_tuple tuple<uuid, blob, tuple<text, int, timestamp>>)";
    let session = setup(cql).await.expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyInnerTuple {
        pub my_text: String,
        pub my_int: i32,
        pub my_timestamp: PrimitiveDateTime,
    }

    impl MyInnerTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyInnerTuple> {
            let my_text: String = tuple.get_r_by_index(0)?;
            let my_int: i32 = tuple.get_r_by_index(1)?;
            let my_timestamp: PrimitiveDateTime = tuple.get_r_by_index(2)?;
            Ok(MyInnerTuple {
                my_text,
                my_int,
                my_timestamp,
            })
        }
    }

    impl From<MyInnerTuple> for Bytes {
        fn from(value: MyInnerTuple) -> Bytes {
            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);
            let val_bytes: Bytes = value.my_text.into();
            Value::new(val_bytes).serialize(&mut cursor);
            let val_bytes: Bytes = value.my_int.into();
            Value::new(val_bytes).serialize(&mut cursor);
            let val_bytes: Bytes = value.my_timestamp.into();
            Value::new(val_bytes).serialize(&mut cursor);
            Bytes::new(bytes)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MyOuterTuple {
        pub my_uuid: Uuid,
        pub my_blob: Vec<u8>,
        pub my_inner_tuple: MyInnerTuple,
    }

    impl MyOuterTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyOuterTuple> {
            let my_uuid: Uuid = tuple.get_r_by_index(0)?;
            let my_blob: Blob = tuple.get_r_by_index(1)?;
            let my_inner_tuple: Tuple = tuple.get_r_by_index(2)?;
            let my_inner_tuple = MyInnerTuple::try_from(my_inner_tuple).expect("from tuple");
            Ok(MyOuterTuple {
                my_uuid,
                my_blob: my_blob.into_vec(),
                my_inner_tuple,
            })
        }
    }

    impl From<MyOuterTuple> for Bytes {
        fn from(value: MyOuterTuple) -> Bytes {
            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);
            let val_bytes: Bytes = value.my_uuid.into();
            Value::new(val_bytes).serialize(&mut cursor);
            let val_bytes: Bytes = Bytes::new(value.my_blob);
            Value::new(val_bytes).serialize(&mut cursor);
            let val_bytes: Bytes = value.my_inner_tuple.into();
            Value::new(val_bytes).serialize(&mut cursor);
            Bytes::new(bytes)
        }
    }

    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let my_blob: Vec<u8> = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255];
    let timestamp = PrimitiveDateTime::new(date!(2019 - 01 - 01), time!(3:01));
    let my_inner_tuple = MyInnerTuple {
        my_text: "my_text".to_string(),
        my_int: 1_000,
        my_timestamp: timestamp,
    };
    let my_outer_tuple = MyOuterTuple {
        my_uuid,
        my_blob,
        my_inner_tuple,
    };
    let values = query_values!(0i32, my_outer_tuple.clone());

    let cql = "INSERT INTO cdrs_test.test_nested_tuples \
               (my_key, my_outer_tuple) VALUES (?, ?)";
    session
        .query_with_values(cql, values)
        .await
        .expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_nested_tuples";
    let rows = session
        .query(cql)
        .await
        .expect("query")
        .body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_outer_tuple_row: Tuple =
            row.get_r_by_name("my_outer_tuple").expect("my_outer_tuple");
        let my_outer_tuple_row = MyOuterTuple::try_from(my_outer_tuple_row).expect("from tuple");
        assert_eq!(my_outer_tuple_row, my_outer_tuple);
    }
}
