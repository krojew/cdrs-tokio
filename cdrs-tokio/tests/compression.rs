mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::compression::Compression;
#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cassandra_protocol::types::blob::Blob;
#[cfg(feature = "e2e-tests")]
use cassandra_protocol::types::ByIndex;
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
use common::*;
#[cfg(feature = "e2e-tests")]
use rand::prelude::*;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[cfg(feature = "e2e-tests")]
async fn encode_decode_test(version: Version) {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .with_version(version)
        .build()
        .await
        .unwrap();

    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .with_compression(Compression::Lz4)
        .build()
        .await
        .unwrap();

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
         replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
         AND durable_writes = false",
        )
        .await
        .unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS cdrs_test.test_compression (pk int PRIMARY KEY, data blob)",
        )
        .await
        .unwrap();

    let mut rng = thread_rng();

    let mut data = vec![0u8; 5 * 1024 * 1024];
    for elem in &mut data {
        *elem = rng.gen();
    }

    let blob = Blob::new(data);

    session
        .query_with_values(
            "INSERT INTO cdrs_test.test_compression (pk, data) VALUES (1, ?)",
            query_values!(blob.clone()),
        )
        .await
        .unwrap();

    let stored_data: Blob = session
        .query("SELECT data FROM cdrs_test.test_compression WHERE pk = 1")
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap()
        .get(0)
        .unwrap()
        .r_by_index::<Blob>(0)
        .unwrap();

    assert_eq!(stored_data, blob);
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn encode_decode_test_v4() {
    encode_decode_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn encode_decode_test_v5() {
    encode_decode_test(Version::V5).await;
}
