mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::frame::Envelope;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query::{PreparedQuery, QueryValues};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use futures::stream::{self, StreamExt};
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;
#[cfg(feature = "e2e-tests")]
use std::time::Instant;

#[cfg(feature = "e2e-tests")]
const KEY_COUNT: i32 = 40_000;

#[cfg(feature = "e2e-tests")]
const WORKERS: i32 = 4;

// requests in flight on the single connection at any time, split evenly between the workers
#[cfg(feature = "e2e-tests")]
const IN_FLIGHT: usize = 4096;

#[cfg(feature = "e2e-tests")]
async fn connect(version: Version) -> CurrentSession {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .with_version(version)
        .build()
        .await
        .unwrap();

    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
        .with_connection_pool_config(
            ConnectionPoolConfigBuilder::new()
                .with_local_size(1)
                .with_remote_size(1)
                .build(),
        )
        .build()
        .await
        .unwrap()
}

#[cfg(feature = "e2e-tests")]
fn value_of(key: i32) -> String {
    format!("value-{key}")
}

/// Executes `statement` with the values of every key on several tasks sharing the session, checks
/// each response and returns how many statements completed.
#[cfg(feature = "e2e-tests")]
async fn run_concurrently(
    session: &Arc<CurrentSession>,
    statement: &Arc<PreparedQuery>,
    values: fn(i32) -> QueryValues,
    check: fn(i32, Envelope),
) -> usize {
    let workers: Vec<_> = (0..WORKERS)
        .map(|worker| {
            let session = Arc::clone(session);
            let statement = Arc::clone(statement);

            tokio::spawn(async move {
                let session = &session;
                let statement = &statement;

                stream::iter((worker..KEY_COUNT).step_by(WORKERS as usize))
                    .map(|key| async move {
                        let envelope = session
                            .exec_with_values(statement, values(key))
                            .await
                            .expect("execute");
                        check(key, envelope);
                    })
                    .buffer_unordered(IN_FLIGHT / WORKERS as usize)
                    .count()
                    .await
            })
        })
        .collect();

    let mut completed = 0;
    for worker in workers {
        completed += worker.await.expect("worker");
    }

    completed
}

#[cfg(feature = "e2e-tests")]
async fn stream_id_stress_test(version: Version) {
    let session = Arc::new(connect(version).await);

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
             replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
             AND durable_writes = false",
        )
        .await
        .expect("create keyspace");
    session
        .query(
            "CREATE TABLE IF NOT EXISTS cdrs_test.test_stream_ids \
             (id int PRIMARY KEY, value text)",
        )
        .await
        .expect("create table");
    session
        .query("TRUNCATE TABLE cdrs_test.test_stream_ids")
        .await
        .expect("truncate");

    let start = Instant::now();

    let insert = Arc::new(
        session
            .prepare("INSERT INTO cdrs_test.test_stream_ids (id, value) VALUES (?, ?)")
            .await
            .expect("prepare insert"),
    );
    let inserted = run_concurrently(
        &session,
        &insert,
        |key| query_values!(key, value_of(key)),
        |_, _| {},
    )
    .await;
    assert_eq!(inserted, KEY_COUNT as usize);

    let insert_time = start.elapsed();

    let select = Arc::new(
        session
            .prepare("SELECT id, value FROM cdrs_test.test_stream_ids WHERE id = ?")
            .await
            .expect("prepare select"),
    );
    let selected = run_concurrently(
        &session,
        &select,
        |key| query_values!(key),
        |key, envelope| {
            let rows = envelope
                .response_body()
                .expect("body")
                .into_rows()
                .expect("rows");
            assert_eq!(rows.len(), 1, "rows for key {}", key);

            let id: i32 = rows[0].get_r_by_name("id").expect("id");
            let value: String = rows[0].get_r_by_name("value").expect("value");
            assert_eq!(id, key);
            assert_eq!(value, value_of(key));
        },
    )
    .await;
    assert_eq!(selected, KEY_COUNT as usize);

    println!(
        "Protocol {:?}: inserted {} keys in {:?} and selected them in {:?}.",
        version,
        KEY_COUNT,
        insert_time,
        start.elapsed() - insert_time
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "e2e-tests")]
async fn stream_id_stress_v4() {
    stream_id_stress_test(Version::V4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "e2e-tests")]
async fn stream_id_stress_v5() {
    stream_id_stress_test(Version::V5).await;
}
