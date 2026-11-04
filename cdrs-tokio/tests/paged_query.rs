mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
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
use cdrs_tokio::statement::StatementParamsBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn paged_query() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let lb = RoundRobinLoadBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
        .build()
        .await
        .unwrap();

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                       'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
        )
        .await
        .expect("Keyspace creation error");

    session
        .query("use test_ks")
        .await
        .expect("Using keyspace went wrong");

    session.query("create table if not exists user (user_id int primary key) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };").await.expect("Could not create table");

    for i in 0..=9 {
        session
            .query(format!("insert into user(user_id) values ({i})"))
            .await
            .expect("Could not create table");
    }

    let mut pager = session.paged(3);
    let mut query_pager = pager.query("SELECT * FROM user");

    // This returns always false the first time
    assert!(!query_pager.has_more());

    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    assert!(query_pager.has_more());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    assert!(query_pager.has_more());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    assert!(query_pager.has_more());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(1, rows.len());

    assert!(!query_pager.has_more());
}

#[cfg(feature = "e2e-tests")]
const PAGE_SIZE: i32 = 4;

#[cfg(feature = "e2e-tests")]
async fn paged_prepared_with_values_test(version: Version) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_paged_prepared \
               (pk int, ck int, PRIMARY KEY (pk, ck))";
    let session = setup(cql, version).await.expect("setup");

    let insert = session
        .prepare("INSERT INTO cdrs_test.test_paged_prepared (pk, ck) VALUES (?, ?)")
        .await
        .expect("prepare insert");
    for (pk, count) in [(1, 12), (2, 10), (3, 5)] {
        for ck in 0..count {
            session
                .exec_with_values(&insert, query_values!(pk, ck))
                .await
                .expect("insert");
        }
    }

    let select = session
        .prepare("SELECT pk, ck FROM cdrs_test.test_paged_prepared WHERE pk = ? AND ck >= ?")
        .await
        .expect("prepare select");

    // an exact multiple of the page size, a remainder, and a restriction on the clustering key
    for (pk, min_ck, expected_count) in [(1, 0, 12), (2, 0, 10), (1, 5, 7)] {
        let mut pager = session.paged(PAGE_SIZE);
        let mut exec_pager = pager.exec_with_params(
            &select,
            StatementParamsBuilder::new()
                .with_values(query_values!(pk, min_ck))
                .build(),
        );

        // a full last page may be followed by an empty one
        let max_pages = expected_count / PAGE_SIZE + 2;
        let mut pages = 0;
        let mut cks = Vec::new();

        loop {
            let rows = exec_pager.next().await.expect("next page");
            pages += 1;
            assert!(pages <= max_pages, "paging did not terminate");
            assert!(rows.len() <= PAGE_SIZE as usize);

            for row in rows {
                let row_pk: i32 = row.get_r_by_name("pk").expect("pk");
                let ck: i32 = row.get_r_by_name("ck").expect("ck");
                assert_eq!(row_pk, pk);
                cks.push(ck);
            }

            if !exec_pager.has_more() {
                break;
            }
        }

        assert_eq!(cks, (min_ck..min_ck + expected_count).collect::<Vec<_>>());
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn paged_prepared_with_values_v4() {
    paged_prepared_with_values_test(Version::V4).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn paged_prepared_with_values_v5() {
    paged_prepared_with_values_test(Version::V5).await;
}
