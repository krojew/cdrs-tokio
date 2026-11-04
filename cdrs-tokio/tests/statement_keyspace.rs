mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::statement::StatementParamsBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::prelude::Row;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "e2e-tests")]
const CREATE_OTHER_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS cdrs_test_other WITH \
     replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
     AND durable_writes = false";
#[cfg(feature = "e2e-tests")]
const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS cdrs_test.test_statement_keyspace \
     (pk int PRIMARY KEY, value text)";
#[cfg(feature = "e2e-tests")]
const CREATE_OTHER_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS cdrs_test_other.test_statement_keyspace \
     (pk int PRIMARY KEY, value text)";

#[cfg(feature = "e2e-tests")]
async fn setup_tables(version: Version) -> CurrentSession {
    setup_multiple(
        &[CREATE_OTHER_KEYSPACE, CREATE_TABLE, CREATE_OTHER_TABLE],
        version,
    )
    .await
    .expect("setup")
}

#[cfg(feature = "e2e-tests")]
async fn query_in_keyspace(session: &CurrentSession, cql: &str, keyspace: &str) -> Vec<Row> {
    session
        .query_with_params(
            cql,
            StatementParamsBuilder::new()
                .with_keyspace(keyspace.into())
                .build(),
        )
        .await
        .expect("query with keyspace")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
}

#[cfg(feature = "e2e-tests")]
fn values(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| row.get_r_by_name("value").expect("value"))
        .collect()
}

#[cfg(feature = "e2e-tests")]
async fn statement_keyspace_test(session: CurrentSession) {
    assert_eq!(session.current_keyspace(), None);

    // both keyspaces hold a table with the same name, so only the statement keyspace tells them
    // apart
    for keyspace in ["cdrs_test", "cdrs_test_other"] {
        query_in_keyspace(
            &session,
            &format!("INSERT INTO test_statement_keyspace (pk, value) VALUES (1, '{keyspace}')"),
            keyspace,
        )
        .await;
    }

    for keyspace in ["cdrs_test", "cdrs_test_other"] {
        let rows = query_in_keyspace(
            &session,
            "SELECT value FROM test_statement_keyspace WHERE pk = 1",
            keyspace,
        )
        .await;
        assert_eq!(values(&rows), vec![keyspace.to_string()]);
    }

    assert_eq!(session.current_keyspace(), None);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn statement_keyspace_v4() {
    let session = setup_tables(Version::V4).await;
    statement_keyspace_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn statement_keyspace_v5() {
    let session = setup_tables(Version::V5).await;
    statement_keyspace_test(session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn now_in_seconds_v5() {
    let session = setup_tables(Version::V5).await;

    session
        .query(
            "INSERT INTO cdrs_test.test_statement_keyspace (pk, value) \
             VALUES (2, 'expiring') USING TTL 1000",
        )
        .await
        .expect("insert with ttl");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs() as i32;

    let select_at = |now_in_seconds: i32| {
        session.query_with_params(
            "SELECT value FROM cdrs_test.test_statement_keyspace WHERE pk = 2",
            StatementParamsBuilder::new()
                .with_now_in_seconds(now_in_seconds)
                .build(),
        )
    };

    let rows = select_at(now)
        .await
        .expect("select now")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(values(&rows), vec!["expiring".to_string()]);

    let rows = select_at(now + 2000)
        .await
        .expect("select in the future")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert!(rows.is_empty());
}
