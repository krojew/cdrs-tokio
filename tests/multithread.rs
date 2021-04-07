#[cfg(feature = "e2e-tests")]
use cdrs_tokio::authenticators::NoneAuthenticator;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobin;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query::QueryExecutor;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn multithread() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", Arc::new(NoneAuthenticator {})).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression = cdrs_tokio::cluster::session::new(&cluster_config, RoundRobin::new())
        .await
        .expect("session should be created");

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

        handles.push(tokio::spawn(async move {
            let result = c.query("select * from user").await;

            result
        }));
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
