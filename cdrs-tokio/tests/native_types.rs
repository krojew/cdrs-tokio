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
use cdrs_tokio::types::duration::Duration;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::list::List;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::map::Map;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::tuple::Tuple;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::udt::Udt;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::value::Bytes;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::{AsRust, ByName, IntoRustByIndex, IntoRustByName};
#[cfg(feature = "e2e-tests")]
use chrono::NaiveDate;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use float_eq::*;
#[cfg(feature = "e2e-tests")]
use num_bigint::BigInt;
#[cfg(feature = "e2e-tests")]
use std::collections::HashMap;
#[cfg(feature = "e2e-tests")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(feature = "e2e-tests")]
use std::str::FromStr;
#[cfg(feature = "e2e-tests")]
use time::PrimitiveDateTime;
#[cfg(feature = "e2e-tests")]
use uuid::Uuid;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn string_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_string \
               (my_ascii ascii PRIMARY KEY, my_text text, my_varchar varchar)";
    let session = setup(cql, Version::V4).await.expect("setup");

    string_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn string_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_string \
               (my_ascii ascii PRIMARY KEY, my_text text, my_varchar varchar)";
    let session = setup(cql, Version::V5).await.expect("setup");

    string_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn string_test(session: CurrentSession) {
    let my_ascii = "my_ascii";
    let my_text = "my_text";
    let my_varchar = "my_varchar";
    let values = query_values!(my_ascii, my_text, my_varchar);

    let query = "INSERT INTO cdrs_test.test_string \
                 (my_ascii, my_text, my_varchar) VALUES (?, ?, ?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert stings error");

    let cql = "SELECT * FROM cdrs_test.test_string";
    let rows = session
        .query(cql)
        .await
        .expect("select strings query error")
        .response_body()
        .expect("get body error")
        .into_rows()
        .expect("converting into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_ascii_row: String = row.get_r_by_name("my_ascii").expect("my_ascii");
        let my_text_row: String = row.get_r_by_name("my_text").expect("my_text");
        let my_varchar_row: String = row.get_r_by_name("my_varchar").expect("my_varchar");
        assert_eq!(my_ascii_row, my_ascii);
        assert_eq!(my_text_row, my_text);
        assert_eq!(my_varchar_row, my_varchar);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn counter_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_counter \
               (my_bigint bigint PRIMARY KEY, my_counter counter)";
    let session = setup(cql, Version::V4).await.expect("setup");

    counter_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn counter_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_counter \
               (my_bigint bigint PRIMARY KEY, my_counter counter)";
    let session = setup(cql, Version::V5).await.expect("setup");

    counter_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn counter_test(session: CurrentSession) {
    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_counter: i64 = 100_000_000;
    let values = query_values!(my_counter, my_bigint);

    let query = "UPDATE cdrs_test.test_counter SET my_counter = my_counter + ? \
                 WHERE my_bigint = ?";
    session
        .query_with_values(query, values)
        .await
        .expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_counter";
    let rows = session
        .query(cql)
        .await
        .expect("select counter query error")
        .response_body()
        .expect("get counter body error")
        .into_rows()
        .expect("converting coutner body into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_counter_row: i64 = row.get_r_by_name("my_counter").expect("my_counter");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_counter_row, my_counter);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn integer_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_boolean boolean)";
    let session = setup(cql, Version::V4).await.expect("setup");

    integer_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn integer_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_boolean boolean)";
    let session = setup(cql, Version::V5).await.expect("setup");

    integer_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn integer_test(session: CurrentSession) {
    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_boolean: bool = true;
    let values = query_values!(my_bigint, my_int, my_boolean);

    let query = "INSERT INTO cdrs_test.test_integer \
                 (my_bigint, my_int, my_boolean) VALUES (?, ?, ?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert integers error");

    let cql = "SELECT * FROM cdrs_test.test_integer";
    let rows = session
        .query(cql)
        .await
        .expect("select integers query error")
        .response_body()
        .expect("get body with integers error")
        .into_rows()
        .expect("converting body with integers into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_int_row: i32 = row.get_r_by_name("my_int").expect("my_int");
        let my_boolean_row: bool = row.get_r_by_name("my_boolean").expect("my_boolean");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_int_row, my_int);
        assert_eq!(my_boolean_row, my_boolean);
    }
}

// TODO counter
#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn integer_advanced_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer_v4 \
               (my_bigint bigint PRIMARY KEY, my_int int, my_smallint smallint, \
               my_tinyint tinyint, my_boolean boolean)";
    let session = setup(cql, Version::V4).await.expect("setup");

    integer_advanced_test(session).await;
}

// TODO counter
#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn integer_advanced_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer_v4 \
               (my_bigint bigint PRIMARY KEY, my_int int, my_smallint smallint, \
               my_tinyint tinyint, my_boolean boolean)";
    let session = setup(cql, Version::V5).await.expect("setup");

    integer_advanced_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn integer_advanced_test(session: CurrentSession) {
    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_smallint: i16 = 10_000;
    let my_tinyint: i8 = 100;
    let my_boolean: bool = true;
    let values = query_values!(my_bigint, my_int, my_smallint, my_tinyint, my_boolean);

    let query = "INSERT INTO cdrs_test.test_integer_v4 \
                 (my_bigint, my_int, my_smallint, my_tinyint, my_boolean) VALUES (?, ?, ?, ?, ?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert integers error");

    let cql = "SELECT * FROM cdrs_test.test_integer_v4";
    let rows = session
        .query(cql)
        .await
        .expect("query integers error")
        .response_body()
        .expect("get body with integers error")
        .into_rows()
        .expect("converting body with integers into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_int_row: i32 = row.get_r_by_name("my_int").expect("my_int");
        let my_smallint_row: i16 = row.get_r_by_name("my_smallint").expect("my_smallint");
        let my_tinyint_row: i8 = row.get_r_by_name("my_tinyint").expect("my_tinyint");
        let my_boolean_row: bool = row.get_r_by_name("my_boolean").expect("my_boolean");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_int_row, my_int);
        assert_eq!(my_smallint_row, my_smallint);
        assert_eq!(my_tinyint_row, my_tinyint);
        assert_eq!(my_boolean_row, my_boolean);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn varint_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_varint \
               (id int PRIMARY KEY, my_varint varint)";
    let session = setup(cql, Version::V4).await.expect("setup");

    varint_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn varint_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_varint \
               (id int PRIMARY KEY, my_varint varint)";
    let session = setup(cql, Version::V5).await.expect("setup");

    varint_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn varint_test(session: CurrentSession) {
    let large = BigInt::from(2).pow(200);
    // values around the boundaries of the minimal two's complement byte length
    let bound = vec![
        BigInt::from(0),
        BigInt::from(1),
        BigInt::from(-1),
        BigInt::from(127),
        BigInt::from(128),
        BigInt::from(-128),
        BigInt::from(-129),
        BigInt::from(i64::MAX),
        BigInt::from(i64::MIN),
        BigInt::from(u64::MAX),
        large.clone(),
        -large - 1,
    ];
    let literals = [
        "123456789012345678901234567890123456789",
        "-98765432109876543210987654321098765432109876543210",
    ];

    let mut expected = HashMap::new();

    for (id, varint) in bound.into_iter().enumerate() {
        let id = id as i32;
        session
            .query_with_values(
                "INSERT INTO cdrs_test.test_varint (id, my_varint) VALUES (?, ?)",
                query_values!(id, varint.clone()),
            )
            .await
            .expect("insert bound varint");
        expected.insert(id, varint);
    }

    for (index, literal) in literals.iter().enumerate() {
        let id = 100 + index as i32;
        session
            .query(format!(
                "INSERT INTO cdrs_test.test_varint (id, my_varint) VALUES ({id}, {literal})"
            ))
            .await
            .expect("insert varint literal");
        expected.insert(id, BigInt::from_str(literal).expect("varint literal"));
    }

    let rows = session
        .query("SELECT id, my_varint FROM cdrs_test.test_varint")
        .await
        .expect("select varints")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");

    let actual: HashMap<i32, BigInt> = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get_r_by_name("id").expect("id");
            let varint: BigInt = row.get_r_by_name("my_varint").expect("my_varint");
            (id, varint)
        })
        .collect();

    assert_eq!(actual, expected);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn float_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_float \
     (my_float float PRIMARY KEY, my_double double, my_decimal_a decimal, my_decimal_b decimal)";
    let session = setup(cql, Version::V4).await.expect("setup");

    float_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn float_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_float \
     (my_float float PRIMARY KEY, my_double double, my_decimal_a decimal, my_decimal_b decimal)";
    let session = setup(cql, Version::V5).await.expect("setup");

    float_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn float_test(session: CurrentSession) {
    let my_float: f32 = 123.456;
    let my_double: f64 = 987.654;
    let my_decimal_b = i64::MAX;
    let values = query_values!(
        my_float,
        my_double,
        Decimal::new(12001.into(), 2),
        Decimal::from(my_decimal_b)
    );

    let query =
        "INSERT INTO cdrs_test.test_float (my_float, my_double, my_decimal_a, my_decimal_b) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert floats error");

    let cql = "SELECT * FROM cdrs_test.test_float";
    let rows = session
        .query(cql)
        .await
        .expect("query floats error")
        .response_body()
        .expect("get body with floats error")
        .into_rows()
        .expect("converting body with floats into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_float_row: f32 = row.get_r_by_name("my_float").expect("my_float");
        let my_double_row: f64 = row.get_r_by_name("my_double").expect("my_double");
        let my_decimal_row_a: Decimal = row.get_r_by_name("my_decimal_a").expect("my_decimal_a");
        let my_decimal_row_b: Decimal = row.get_r_by_name("my_decimal_b").expect("my_decimal_b");
        assert_float_eq!(my_float_row, my_float, abs <= f32::EPSILON);
        assert_float_eq!(my_double_row, my_double, abs <= f64::EPSILON);
        assert_eq!(my_decimal_row_a, Decimal::new(12001.into(), 2));
        assert_eq!(my_decimal_row_b, Decimal::from(my_decimal_b));
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn blob_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_blob \
               (my_blob blob PRIMARY KEY, my_mapblob map<text, blob>)";
    let session = setup(cql, Version::V4).await.expect("setup");

    blob_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn blob_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_blob \
               (my_blob blob PRIMARY KEY, my_mapblob map<text, blob>)";
    let session = setup(cql, Version::V5).await.expect("setup");

    blob_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn blob_test(session: CurrentSession) {
    let my_blob: Blob = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255].into();
    let my_map: HashMap<String, Blob> = [
        ("a".to_owned(), b"aaaaa".to_vec().into()),
        ("b".to_owned(), b"bbbbb".to_vec().into()),
        ("c".to_owned(), b"ccccc".to_vec().into()),
        ("d".to_owned(), b"ddddd".to_vec().into()),
    ]
    .iter()
    .cloned()
    .collect();

    let val_map: HashMap<String, Bytes> = my_map
        .clone()
        .into_iter()
        .map(|(k, v)| (k, Bytes::new(v.into_vec())))
        .collect();

    let values = query_values!(my_blob.clone(), val_map);

    let query = "INSERT INTO cdrs_test.test_blob (my_blob, my_mapblob) VALUES (?,?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert blob error");

    let cql = "SELECT * FROM cdrs_test.test_blob";
    let rows = session
        .query(cql)
        .await
        .expect("query blobs error")
        .response_body()
        .expect("get body with blobs error")
        .into_rows()
        .expect("converting body with blobs into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_blob_row: Blob = row.get_r_by_name("my_blob").expect("my_blob");
        assert_eq!(my_blob_row, my_blob);
        let my_map_row: HashMap<String, Blob> = row
            .r_by_name::<Map>("my_mapblob")
            .expect("my_mapblob by name")
            .as_r_rust()
            .expect("my_mapblob as r rust");
        assert_eq!(my_map_row, my_map);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn uuid_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_uuid \
               (my_uuid uuid PRIMARY KEY)";
    let session = setup(cql, Version::V4).await.expect("setup");

    uuid_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn uuid_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_uuid \
               (my_uuid uuid PRIMARY KEY)";
    let session = setup(cql, Version::V5).await.expect("setup");

    uuid_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn uuid_test(session: CurrentSession) {
    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let values = query_values!(my_uuid);

    let query = "INSERT INTO cdrs_test.test_uuid (my_uuid) VALUES (?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert UUID error");

    let cql = "SELECT * FROM cdrs_test.test_uuid";
    let rows = session
        .query(cql)
        .await
        .expect("query UUID error")
        .response_body()
        .expect("get body with UUID error")
        .into_rows()
        .expect("conversion body with UUID into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_uuid_row: Uuid = row.get_r_by_name("my_uuid").expect("my_uuid");
        assert_eq!(my_uuid_row, my_uuid);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn timeuuid_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_timeuuid \
               (id int PRIMARY KEY, my_timeuuid timeuuid)";
    let session = setup(cql, Version::V4).await.expect("setup");

    timeuuid_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn timeuuid_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_timeuuid \
               (id int PRIMARY KEY, my_timeuuid timeuuid)";
    let session = setup(cql, Version::V5).await.expect("setup");

    timeuuid_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn timeuuid_test(session: CurrentSession) {
    let my_timeuuid = Uuid::from_str("e0e1a5a0-6e2a-11ee-b962-0242ac120002").unwrap();

    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_timeuuid (id, my_timeuuid) VALUES (?, ?)",
            query_values!(1, my_timeuuid),
        )
        .await
        .expect("insert timeuuid");
    session
        .query("INSERT INTO cdrs_test.test_timeuuid (id, my_timeuuid) VALUES (2, now())")
        .await
        .expect("insert generated timeuuid");

    let rows = session
        .query("SELECT id, my_timeuuid FROM cdrs_test.test_timeuuid")
        .await
        .expect("select timeuuids")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");

    let timeuuids: HashMap<i32, Uuid> = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get_r_by_name("id").expect("id");
            let timeuuid: Uuid = row.get_r_by_name("my_timeuuid").expect("my_timeuuid");
            (id, timeuuid)
        })
        .collect();

    assert_eq!(timeuuids.len(), 2);
    assert_eq!(timeuuids[&1], my_timeuuid);
    assert_eq!(timeuuids[&2].get_version_num(), 1);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn time_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_time \
               (my_timestamp timestamp PRIMARY KEY)";
    let session = setup(cql, Version::V4).await.expect("setup");

    time_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn time_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_time \
               (my_timestamp timestamp PRIMARY KEY)";
    let session = setup(cql, Version::V5).await.expect("setup");

    time_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn time_test(session: CurrentSession) {
    let my_timestamp: PrimitiveDateTime = time::macros::datetime!(2019-01-01 0:00);
    let values = query_values!(my_timestamp);

    let query = "INSERT INTO cdrs_test.test_time (my_timestamp) VALUES (?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert timestamp error");

    let cql = "SELECT * FROM cdrs_test.test_time";
    let rows = session
        .query(cql)
        .await
        .expect("query with time error")
        .response_body()
        .expect("get body with time error")
        .into_rows()
        .expect("converting body with time into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_timestamp_row: time::PrimitiveDateTime =
            row.get_r_by_name("my_timestamp").expect("my_timestamp");
        assert_eq!(my_timestamp_row.second(), my_timestamp.second());
        assert_eq!(
            my_timestamp_row.nanosecond() / 1_000_000,
            my_timestamp.nanosecond() / 1_000_000
        ); // C* `timestamp` has millisecond precision
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn duration_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_duration \
               (id int PRIMARY KEY, my_duration duration)";
    let session = setup(cql, Version::V4).await.expect("setup");

    duration_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn duration_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_duration \
               (id int PRIMARY KEY, my_duration duration)";
    let session = setup(cql, Version::V5).await.expect("setup");

    duration_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn duration_test(session: CurrentSession) {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    let bound = [
        Duration::new(0, 0, 0),
        Duration::new(1, 2, 3),
        // vint components of 64 and above take more than one byte
        Duration::new(64, 64, 64),
        Duration::new(100, 1000, 0),
        Duration::new(-100, -1000, -5 * NANOS_PER_SECOND),
        Duration::new(0, 0, (25 * 3600 + 30 * 60 + 15) * NANOS_PER_SECOND + 123),
        Duration::new(i32::MAX, i32::MAX, i64::MAX),
        Duration::new(i32::MIN, i32::MIN, i64::MIN),
    ];
    let literals = [
        (
            "1y2mo3w4d5h6m7s8ms9us10ns",
            Duration::new(
                14,
                25,
                (5 * 3600 + 6 * 60 + 7) * NANOS_PER_SECOND + 8_009_010,
            ),
        ),
        ("-2mo10d", Duration::new(-2, -10, 0)),
        ("100mo1000d", Duration::new(100, 1000, 0)),
        (
            "89h4m48s",
            Duration::new(0, 0, (89 * 3600 + 4 * 60 + 48) * NANOS_PER_SECOND),
        ),
        (
            "P1Y2M3DT4H5M6S",
            Duration::new(14, 3, (4 * 3600 + 5 * 60 + 6) * NANOS_PER_SECOND),
        ),
        ("P2W", Duration::new(0, 14, 0)),
        (
            "P0001-02-03T04:05:06",
            Duration::new(14, 3, (4 * 3600 + 5 * 60 + 6) * NANOS_PER_SECOND),
        ),
        ("-PT1S", Duration::new(0, 0, -NANOS_PER_SECOND)),
    ];

    let mut expected = HashMap::new();

    for (id, duration) in bound.iter().enumerate() {
        let id = id as i32;
        let duration = duration.expect("valid duration");
        session
            .query_with_values(
                "INSERT INTO cdrs_test.test_duration (id, my_duration) VALUES (?, ?)",
                query_values!(id, duration),
            )
            .await
            .expect("insert bound duration");
        expected.insert(id, duration);
    }

    for (index, (literal, duration)) in literals.iter().enumerate() {
        let id = 100 + index as i32;
        session
            .query(format!(
                "INSERT INTO cdrs_test.test_duration (id, my_duration) VALUES ({id}, {literal})"
            ))
            .await
            .expect("insert duration literal");
        expected.insert(id, duration.expect("valid duration"));
    }

    let body = session
        .query("SELECT id, my_duration FROM cdrs_test.test_duration")
        .await
        .expect("select durations")
        .response_body()
        .expect("body");

    let actual: HashMap<i32, Duration> = column_values(&body, "id")
        .into_iter()
        .zip(column_values(&body, "my_duration"))
        .map(|values| match values {
            (CassandraType::Int(id), CassandraType::Duration(duration)) => (id, duration),
            other => panic!("unexpected values {:?}", other),
        })
        .collect();

    assert_eq!(actual, expected);

    let rows = body.into_rows().expect("into rows");

    let by_name: HashMap<i32, Duration> = rows
        .iter()
        .map(|row| {
            (
                row.get_r_by_name("id").expect("id by name"),
                row.get_r_by_name("my_duration")
                    .expect("my_duration by name"),
            )
        })
        .collect();
    assert_eq!(by_name, expected);

    let by_index: HashMap<i32, Duration> = rows
        .iter()
        .map(|row| {
            (
                row.get_r_by_index(0).expect("id by index"),
                row.get_r_by_index(1).expect("my_duration by index"),
            )
        })
        .collect();
    assert_eq!(by_index, expected);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn duration_collections_v4() {
    let create_type_cql =
        "CREATE TYPE IF NOT EXISTS cdrs_test.duration_udt (my_duration duration, my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_duration_collections \
         (id int PRIMARY KEY, my_list list<duration>, my_map map<text, duration>, \
         my_udt frozen<duration_udt>, my_tuple tuple<duration, int>)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V4)
        .await
        .expect("setup");

    duration_collections_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn duration_collections_v5() {
    let create_type_cql =
        "CREATE TYPE IF NOT EXISTS cdrs_test.duration_udt (my_duration duration, my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_duration_collections \
         (id int PRIMARY KEY, my_list list<duration>, my_map map<text, duration>, \
         my_udt frozen<duration_udt>, my_tuple tuple<duration, int>)";
    let session = setup_multiple(&[create_type_cql, create_table_cql], Version::V5)
        .await
        .expect("setup");

    duration_collections_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn duration_collections_test(session: CurrentSession) {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    let id: i32 = 1;
    let my_list = vec![
        Duration::new(1, 2, 3).expect("valid duration"),
        Duration::new(-1, -2, -3).expect("valid duration"),
        Duration::new(i32::MAX, i32::MAX, i64::MAX).expect("valid duration"),
    ];
    let my_map = HashMap::from([
        (
            "a".to_string(),
            Duration::new(0, 0, 0).expect("valid duration"),
        ),
        (
            "b".to_string(),
            Duration::new(64, 64, 64).expect("valid duration"),
        ),
        (
            "c".to_string(),
            Duration::new(i32::MIN, i32::MIN, i64::MIN).expect("valid duration"),
        ),
    ]);

    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_duration_collections \
             (id, my_list, my_map, my_udt, my_tuple) \
             VALUES (?, ?, ?, {my_duration: 1y2mo3d, my_text: 'udt'}, (4h5m6s, 7))",
            query_values!(id, my_list.clone(), my_map.clone()),
        )
        .await
        .expect("insert duration collections");

    let rows = session
        .query("SELECT * FROM cdrs_test.test_duration_collections")
        .await
        .expect("select duration collections")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let list: Vec<Duration> = row
        .r_by_name::<List>("my_list")
        .expect("my_list by name")
        .as_r_rust()
        .expect("my_list as r rust");
    assert_eq!(list, my_list);

    let map: HashMap<String, Duration> = row
        .r_by_name::<Map>("my_map")
        .expect("my_map by name")
        .as_r_rust()
        .expect("my_map as r rust");
    assert_eq!(map, my_map);

    let udt: Udt = row.get_r_by_name("my_udt").expect("my_udt");
    let udt_duration: Duration = udt.get_r_by_name("my_duration").expect("udt my_duration");
    assert_eq!(
        udt_duration,
        Duration::new(14, 3, 0).expect("valid duration")
    );

    let tuple: Tuple = row.get_r_by_name("my_tuple").expect("my_tuple");
    let tuple_duration: Duration = tuple.get_r_by_index(0).expect("tuple duration");
    assert_eq!(
        tuple_duration,
        Duration::new(0, 0, (4 * 3600 + 5 * 60 + 6) * NANOS_PER_SECOND).expect("valid duration")
    );
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn date_time_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_date_time \
               (id int PRIMARY KEY, my_date date, my_time time)";
    let session = setup(cql, Version::V4).await.expect("setup");

    date_time_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn date_time_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_date_time \
               (id int PRIMARY KEY, my_date date, my_time time)";
    let session = setup(cql, Version::V5).await.expect("setup");

    date_time_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn date_time_test(session: CurrentSession) {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    // `date` is an unsigned day count with the Unix epoch at 2^31; the driver exposes its raw bits
    // as an i32, which is the day offset from the epoch with the sign bit flipped
    fn raw_date(year: i32, month: u32, day: u32) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32;
        days ^ i32::MIN
    }

    let bound = [
        (raw_date(1970, 1, 1), 0),
        (
            raw_date(1969, 7, 20),
            (20 * 3600 + 17 * 60 + 40) * NANOS_PER_SECOND,
        ),
        (raw_date(2024, 2, 29), 86_400 * NANOS_PER_SECOND - 1),
        (0, 1),
        (-1, 123_456_789),
    ];
    let literals = [
        (
            "'1969-07-20'",
            "'20:17:40.123456789'",
            raw_date(1969, 7, 20),
            (20 * 3600 + 17 * 60 + 40) * NANOS_PER_SECOND + 123_456_789,
        ),
        ("'2024-02-29'", "'00:00:00'", raw_date(2024, 2, 29), 0),
        (
            "'0001-01-01'",
            "'23:59:59.999999999'",
            raw_date(1, 1, 1),
            86_400 * NANOS_PER_SECOND - 1,
        ),
    ];

    let mut expected = HashMap::new();

    for (id, (date, time)) in bound.iter().enumerate() {
        let id = id as i32;
        session
            .query_with_values(
                "INSERT INTO cdrs_test.test_date_time (id, my_date, my_time) VALUES (?, ?, ?)",
                query_values!(id, *date, *time),
            )
            .await
            .expect("insert bound date and time");
        expected.insert(id, (*date, *time));
    }

    for (index, (date_literal, time_literal, date, time)) in literals.iter().enumerate() {
        let id = 100 + index as i32;
        session
            .query(format!(
                "INSERT INTO cdrs_test.test_date_time (id, my_date, my_time) \
                 VALUES ({id}, {date_literal}, {time_literal})"
            ))
            .await
            .expect("insert date and time literals");
        expected.insert(id, (*date, *time));
    }

    let rows = session
        .query("SELECT id, my_date, my_time FROM cdrs_test.test_date_time")
        .await
        .expect("select dates and times")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");

    let actual: HashMap<i32, (i32, i64)> = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get_r_by_name("id").expect("id");
            let date: i32 = row.get_r_by_name("my_date").expect("my_date");
            let time: i64 = row.get_r_by_name("my_time").expect("my_time");
            (id, (date, time))
        })
        .collect();

    assert_eq!(actual, expected);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn inet_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_inet \
               (my_inet_v4 inet PRIMARY KEY, my_inet_v6 inet)";
    let session = setup(cql, Version::V4).await.expect("setup");

    inet_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn inet_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_inet \
               (my_inet_v4 inet PRIMARY KEY, my_inet_v6 inet)";
    let session = setup(cql, Version::V5).await.expect("setup");

    inet_test(session).await;
}

#[cfg(feature = "e2e-tests")]
async fn inet_test(session: CurrentSession) {
    let my_inet_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let my_inet_v6 = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
    let values = query_values!(my_inet_v4, my_inet_v6);

    let query = "INSERT INTO cdrs_test.test_inet (my_inet_v4, my_inet_v6) VALUES (?, ?)";
    session
        .query_with_values(query, values)
        .await
        .expect("insert inet error");

    let query = "SELECT * FROM cdrs_test.test_inet";
    let rows = session
        .query(query)
        .await
        .expect("query inet error")
        .response_body()
        .expect("get body with inet error")
        .into_rows()
        .expect("converting body with inet into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_inet_v4_row: IpAddr = row.get_r_by_name("my_inet_v4").expect("my_inet_v4");
        let my_inet_v6_row: IpAddr = row.get_r_by_name("my_inet_v6").expect("my_inet_v6");
        assert_eq!(my_inet_v4_row, my_inet_v4);
        assert_eq!(my_inet_v6_row, my_inet_v6);
    }
}
