#[cfg(feature = "e2e-tests")]
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
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
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
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
