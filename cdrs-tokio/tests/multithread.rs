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
async fn multithread() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let no_compression =
        TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
            .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
            .build()
            .unwrap();

    no_compression.query("CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await.expect("Could not create ks");
    no_compression
        .query("use test_ks;")
        .await
        .expect("Keyspace create error");
    no_compression.query("create table if not exists user (user_id int primary key) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };").await.expect("Could not create table");

    let arc = Arc::new(no_compression);
    let mut handles = vec![];

    for _ in 0..100 {
        let c = Arc::clone(&arc);

        handles.push(tokio::spawn(
            async move { c.query("select * from user").await },
        ));
    }

    for task in handles {
        let result = task.await.unwrap();

        match result {
            Ok(_) => {
                println!("Query went OK");
            }
            Err(e) => {
                panic!("Query error: {:#?}", e);
            }
        }
    }
}
