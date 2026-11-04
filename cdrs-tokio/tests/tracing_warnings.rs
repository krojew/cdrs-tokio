mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::statement::StatementParamsBuilder;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::time::{Duration, Instant};
#[cfg(feature = "e2e-tests")]
use uuid::Uuid;

#[cfg(feature = "e2e-tests")]
const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS cdrs_test.test_tracing_warnings \
     (pk int, ck int, PRIMARY KEY (pk, ck))";

// trace events are written asynchronously after the response is sent
#[cfg(feature = "e2e-tests")]
const TRACE_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(feature = "e2e-tests")]
async fn wait_for_trace_session(session: &CurrentSession, tracing_id: Uuid) {
    let deadline = Instant::now() + TRACE_TIMEOUT;

    loop {
        let rows = session
            .query_with_values(
                "SELECT session_id FROM system_traces.sessions WHERE session_id = ?",
                query_values!(tracing_id),
            )
            .await
            .expect("select trace session")
            .response_body()
            .expect("body")
            .into_rows()
            .expect("rows");
        if !rows.is_empty() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "trace session {} did not appear within {:?}",
            tracing_id,
            TRACE_TIMEOUT
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "e2e-tests")]
async fn tracing_test(version: Version) {
    let session = setup(CREATE_TABLE, version).await.expect("setup");
    session
        .query("INSERT INTO cdrs_test.test_tracing_warnings (pk, ck) VALUES (1, 1)")
        .await
        .expect("insert");

    let select = "SELECT * FROM cdrs_test.test_tracing_warnings WHERE pk = 1";

    let envelope = session.query(select).await.expect("untraced query");
    assert_eq!(envelope.tracing_id, None);

    let envelope = session
        .query_with_params(
            select,
            StatementParamsBuilder::new().with_tracing(true).build(),
        )
        .await
        .expect("traced query");
    let query_tracing_id = envelope.tracing_id.expect("query tracing id");
    let rows = envelope
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(rows.len(), 1);

    let prepared = session.prepare(select).await.expect("prepare");
    let envelope = session
        .exec_with_params(
            &prepared,
            &StatementParamsBuilder::new().with_tracing(true).build(),
        )
        .await
        .expect("traced execute");
    let exec_tracing_id = envelope.tracing_id.expect("execute tracing id");
    assert_ne!(exec_tracing_id, query_tracing_id);

    wait_for_trace_session(&session, query_tracing_id).await;
    wait_for_trace_session(&session, exec_tracing_id).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn tracing_v4() {
    tracing_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn tracing_v5() {
    tracing_test(Version::V5).await;
}

#[cfg(feature = "e2e-tests")]
async fn warnings_test(version: Version) {
    let session = setup(CREATE_TABLE, version).await.expect("setup");

    let envelope = session
        .query_with_params(
            "SELECT * FROM cdrs_test.test_tracing_warnings WHERE pk = 1",
            StatementParamsBuilder::new().with_warnings(true).build(),
        )
        .await
        .expect("query without warnings");
    assert!(envelope.warnings.is_empty());

    // aggregating without restricting the partition key makes the server warn
    let envelope = session
        .query_with_params(
            "SELECT count(*) FROM cdrs_test.test_tracing_warnings",
            StatementParamsBuilder::new().with_warnings(true).build(),
        )
        .await
        .expect("query with warnings");
    assert!(
        !envelope.warnings.is_empty(),
        "expected a warning for an aggregation without a partition key"
    );

    let rows = envelope
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn warnings_v4() {
    warnings_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn warnings_v5() {
    warnings_test(Version::V5).await;
}
