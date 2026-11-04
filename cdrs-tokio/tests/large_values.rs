mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::compression::Compression;
#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::blob::Blob;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::prelude::Row;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::{ByName, IntoRustByName};
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

// Protocol v5 frames carry at most 128 KiB of payload, so values of this size force both the
// requests and the responses to span several frames.
#[cfg(feature = "e2e-tests")]
const VALUE_SIZE: usize = 300 * 1024;

#[cfg(feature = "e2e-tests")]
const ROW_COUNT: i32 = 3;

#[cfg(feature = "e2e-tests")]
async fn connect(version: Version, compression: Compression) -> CurrentSession {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .with_version(version)
        .build()
        .await
        .unwrap();

    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
        .with_compression(compression)
        .build()
        .await
        .unwrap()
}

/// Deterministic xorshift noise. LZ4 cannot shrink it, which exercises frames whose compressed
/// form is not smaller than the original.
#[cfg(feature = "e2e-tests")]
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 32) as u8
        })
        .collect()
}

/// Compressible text with multi-byte characters, at least `len` bytes long.
#[cfg(feature = "e2e-tests")]
fn text(seed: i32, len: usize) -> String {
    let mut text = String::with_capacity(len + 64);
    let mut line = 0;
    while text.len() < len {
        text.push_str(&format!(
            "row {seed} line {line}: zażółć gęślą jaźń, the quick brown fox jumps over the lazy dog\n"
        ));
        line += 1;
    }

    text
}

#[cfg(feature = "e2e-tests")]
fn assert_row(row: &Row) {
    let pk: i32 = row.get_r_by_name("pk").expect("pk");
    let data = row.r_by_name::<Blob>("data").expect("data").into_vec();
    let text_data: String = row.get_r_by_name("text_data").expect("text_data");

    // plain asserts keep a mismatch from dumping hundreds of KiB into the test output
    assert_eq!(data.len(), VALUE_SIZE);
    assert!(
        data == noise(pk as u64, VALUE_SIZE),
        "blob of row {} differs",
        pk
    );
    assert!(
        text_data == text(pk, VALUE_SIZE),
        "text of row {} differs",
        pk
    );
}

#[cfg(feature = "e2e-tests")]
async fn large_values_test(version: Version, compression: Compression) {
    let session = connect(version, compression).await;

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
            "CREATE TABLE IF NOT EXISTS cdrs_test.test_large_values \
             (pk int PRIMARY KEY, data blob, text_data text)",
        )
        .await
        .expect("create table");
    session
        .query("TRUNCATE TABLE cdrs_test.test_large_values")
        .await
        .expect("truncate");

    let insert = session
        .prepare("INSERT INTO cdrs_test.test_large_values (pk, data, text_data) VALUES (?, ?, ?)")
        .await
        .expect("prepare insert");

    for pk in 0..ROW_COUNT {
        let data = Blob::new(noise(pk as u64, VALUE_SIZE));
        session
            .exec_with_values(&insert, query_values!(pk, data, text(pk, VALUE_SIZE)))
            .await
            .expect("insert");
    }

    let rows = session
        .query("SELECT pk, data, text_data FROM cdrs_test.test_large_values")
        .await
        .expect("select all")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");

    assert_eq!(rows.len(), ROW_COUNT as usize);
    for row in &rows {
        assert_row(row);
    }

    let select = session
        .prepare("SELECT pk, data, text_data FROM cdrs_test.test_large_values WHERE pk = ?")
        .await
        .expect("prepare select");

    for pk in 0..ROW_COUNT {
        let rows = session
            .exec_with_values(&select, query_values!(pk))
            .await
            .expect("select one")
            .response_body()
            .expect("body")
            .into_rows()
            .expect("rows");

        assert_eq!(rows.len(), 1);
        assert_row(&rows[0]);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn large_values_v5_uncompressed() {
    large_values_test(Version::V5, Compression::None).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn large_values_v5_lz4() {
    large_values_test(Version::V5, Compression::Lz4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn large_values_v4_lz4() {
    large_values_test(Version::V4, Compression::Lz4).await;
}
