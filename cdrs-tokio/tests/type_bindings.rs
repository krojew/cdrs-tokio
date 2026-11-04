mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::blob::Blob;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::cassandra_type::CassandraType;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::decimal::Decimal;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::list::List;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::vector::{FloatVector, Vector};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::{AsRust, ByName, IntoRustByName};
#[cfg(feature = "e2e-tests")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::collections::HashMap;
#[cfg(feature = "e2e-tests")]
use std::convert::TryFrom;
#[cfg(feature = "e2e-tests")]
use time::PrimitiveDateTime;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn blob_bindings_v4() {
    blob_bindings_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn blob_bindings_v5() {
    blob_bindings_test(Version::V5).await;
}

#[cfg(feature = "e2e-tests")]
async fn blob_bindings_test(version: Version) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_bindings_blob \
               (id int PRIMARY KEY, my_blob blob, my_blobs list<blob>)";
    let session = setup(cql, version).await.expect("setup");

    let owned: Vec<u8> = vec![0, 1, 2, 127, 128, 255];
    let borrowed: &[u8] = &[255, 254, 0, 42];
    let insert = "INSERT INTO cdrs_test.test_bindings_blob (id, my_blob) VALUES (?, ?)";

    session
        .query_with_values(insert, query_values!(1, owned.clone()))
        .await
        .expect("insert Vec<u8>");
    session
        .query_with_values(insert, query_values!(2, borrowed))
        .await
        .expect("insert &[u8]");
    session
        .query_with_values(insert, query_values!(3, Vec::<u8>::new()))
        .await
        .expect("insert empty Vec<u8>");

    let blobs: Vec<Vec<u8>> = vec![vec![1, 2, 3], vec![], vec![255; 300]];
    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_bindings_blob (id, my_blobs) VALUES (?, ?)",
            query_values!(4, blobs.clone()),
        )
        .await
        .expect("insert Vec<Vec<u8>>");

    let rows = session
        .query("SELECT * FROM cdrs_test.test_bindings_blob")
        .await
        .expect("select blobs")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(rows.len(), 4);

    let mut blob_by_id = HashMap::new();
    let mut blobs_by_id = HashMap::new();
    for row in rows {
        let id: i32 = row.get_r_by_name("id").expect("id");
        let blob: Option<Blob> = row.get_by_name("my_blob").expect("my_blob");
        blob_by_id.insert(id, blob.map(Blob::into_vec));

        if let Some(list) = row.by_name::<List>("my_blobs").expect("my_blobs") {
            let list: Vec<Blob> = list.as_r_rust().expect("list of blobs");
            blobs_by_id.insert(id, list.into_iter().map(Blob::into_vec).collect::<Vec<_>>());
        }
    }

    assert_eq!(blob_by_id[&1], Some(owned));
    assert_eq!(blob_by_id[&2], Some(borrowed.to_vec()));
    assert_eq!(blob_by_id[&3], Some(vec![]));
    assert_eq!(blob_by_id[&4], None);
    assert_eq!(blobs_by_id.len(), 1);
    assert_eq!(blobs_by_id[&4], blobs);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn vector_bindings_v4() {
    vector_bindings_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn vector_bindings_v5() {
    vector_bindings_test(Version::V5).await;
}

#[cfg(feature = "e2e-tests")]
async fn vector_bindings_test(version: Version) {
    let session = setup_multiple(&[], version).await.expect("setup");

    let server_version = server_version(&session).await;
    if server_version < (5, 0) {
        println!(
            "Skipping vector bindings: Cassandra {}.{} has no vector type.",
            server_version.0, server_version.1
        );
        return;
    }

    session
        .query(
            "CREATE TABLE IF NOT EXISTS cdrs_test.test_bindings_vector \
             (id int PRIMARY KEY, my_float_vector vector<float, 3>, my_int_vector vector<int, 2>)",
        )
        .await
        .expect("create table");
    session
        .query("TRUNCATE TABLE cdrs_test.test_bindings_vector")
        .await
        .expect("truncate");

    let floats = vec![1.0f32, -2.5, 3.25];
    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_bindings_vector (id, my_float_vector, my_int_vector) \
             VALUES (1, ?, [7, -1])",
            query_values!(FloatVector(floats.clone())),
        )
        .await
        .expect("insert vectors");

    let body = session
        .query("SELECT my_float_vector, my_int_vector FROM cdrs_test.test_bindings_vector")
        .await
        .expect("select vectors")
        .response_body()
        .expect("body");

    // only float vectors have a known element layout; others are exposed as their raw bytes
    assert_eq!(
        column_values(&body, "my_float_vector"),
        vec![CassandraType::Vector(
            floats.iter().copied().map(CassandraType::Float).collect()
        )]
    );
    assert_eq!(
        column_values(&body, "my_int_vector"),
        vec![CassandraType::Blob(Blob::new(vec![
            0, 0, 0, 7, 0xFF, 0xFF, 0xFF, 0xFF
        ]))]
    );

    let rows = body.into_rows().expect("rows");
    assert_eq!(rows.len(), 1);
    let read: Vec<f32> = rows[0]
        .r_by_name::<Vector>("my_float_vector")
        .expect("my_float_vector")
        .as_r_rust()
        .expect("vector of floats");
    assert_eq!(read, floats);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn decimal_bindings_v4() {
    decimal_bindings_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn decimal_bindings_v5() {
    decimal_bindings_test(Version::V5).await;
}

#[cfg(feature = "e2e-tests")]
async fn decimal_bindings_test(version: Version) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_bindings_decimal \
               (id int PRIMARY KEY, my_decimal decimal)";
    let session = setup(cql, version).await.expect("setup");

    let decimals = vec![
        Decimal::new(12345.into(), 19),
        Decimal::new((-987_654_321_987_654_321i64).into(), 25),
        Decimal::new(i64::MAX.into(), 40),
        Decimal::new(123.into(), -5),
        Decimal::new((-1).into(), -20),
        Decimal::try_from(1e20f64).expect("1e20"),
        Decimal::try_from(0.1f64).expect("0.1"),
        Decimal::try_from(-123.456f64).expect("-123.456"),
        Decimal::try_from(1.5e-10f64).expect("1.5e-10"),
        Decimal::try_from(f64::MAX).expect("f64::MAX"),
        Decimal::try_from(f64::MIN_POSITIVE).expect("f64::MIN_POSITIVE"),
        Decimal::try_from(-0.0f64).expect("-0.0"),
        Decimal::try_from(3.25f32).expect("3.25"),
    ];

    for (id, decimal) in decimals.iter().enumerate() {
        session
            .query_with_values(
                "INSERT INTO cdrs_test.test_bindings_decimal (id, my_decimal) VALUES (?, ?)",
                query_values!(id as i32, decimal.clone()),
            )
            .await
            .expect("insert decimal");
    }

    let rows = session
        .query("SELECT id, my_decimal FROM cdrs_test.test_bindings_decimal")
        .await
        .expect("select decimals")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(rows.len(), decimals.len());

    for row in rows {
        let id: i32 = row.get_r_by_name("id").expect("id");
        let decimal: Decimal = row.get_r_by_name("my_decimal").expect("my_decimal");
        assert_eq!(decimal, decimals[id as usize]);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn pre_epoch_timestamps_v4() {
    pre_epoch_timestamps_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn pre_epoch_timestamps_v5() {
    pre_epoch_timestamps_test(Version::V5).await;
}

#[cfg(feature = "e2e-tests")]
async fn pre_epoch_timestamps_test(version: Version) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_bindings_timestamp \
               (id int PRIMARY KEY, my_timestamp timestamp)";
    let session = setup(cql, version).await.expect("setup");

    let timestamp = |year, month, day, hour, minute, second, milli| -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_milli_opt(hour, minute, second, milli))
            .expect("valid timestamp")
    };
    let timestamps = [
        timestamp(1969, 7, 20, 20, 17, 40, 123),
        timestamp(1969, 12, 31, 23, 59, 59, 999),
        timestamp(1969, 12, 31, 23, 59, 59, 1),
        timestamp(1900, 1, 1, 0, 0, 0, 1),
        timestamp(1582, 10, 15, 12, 30, 45, 500),
        timestamp(1970, 1, 1, 0, 0, 0, 0),
        timestamp(2024, 2, 29, 13, 14, 15, 16),
    ];

    let insert = "INSERT INTO cdrs_test.test_bindings_timestamp (id, my_timestamp) VALUES (?, ?)";
    for (id, naive) in timestamps.iter().enumerate() {
        let id = id as i32;
        // alternate between both chrono types for binding
        let values = if id % 2 == 0 {
            query_values!(id, *naive)
        } else {
            query_values!(id, naive.and_utc())
        };

        session
            .query_with_values(insert, values)
            .await
            .expect("insert timestamp");
    }

    let rows = session
        .query("SELECT id, my_timestamp FROM cdrs_test.test_bindings_timestamp")
        .await
        .expect("select timestamps")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(rows.len(), timestamps.len());

    for row in rows {
        let id: i32 = row.get_r_by_name("id").expect("id");
        let expected = timestamps[id as usize];

        let naive: NaiveDateTime = row.get_r_by_name("my_timestamp").expect("NaiveDateTime");
        let utc: DateTime<Utc> = row.get_r_by_name("my_timestamp").expect("DateTime<Utc>");
        let primitive: PrimitiveDateTime = row
            .get_r_by_name("my_timestamp")
            .expect("PrimitiveDateTime");
        let millis: i64 = row.get_r_by_name("my_timestamp").expect("i64");

        assert_eq!(naive, expected);
        assert_eq!(utc, expected.and_utc());
        assert_eq!(
            primitive.assume_utc().unix_timestamp(),
            expected.and_utc().timestamp()
        );
        assert_eq!(
            u32::from(primitive.millisecond()),
            expected.and_utc().timestamp_subsec_millis()
        );
        assert_eq!(millis, expected.and_utc().timestamp_millis());
    }
}
