mod common;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use common::*;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;
#[cfg(feature = "e2e-tests")]
use std::time::Duration;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::speculative_execution::ConstantSpeculativeExecutionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn single_node_speculative_execution() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .build()
        .await
        .unwrap();

    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .with_speculative_execution_policy(Box::new(ConstantSpeculativeExecutionPolicy::new(
            5,
            Duration::from_secs(1),
        )))
        .build()
        .await
        .unwrap();

    let create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
         replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
         AND durable_writes = false";
    session
        .query(create_keyspace_query)
        .await
        .expect("create keyspace error");

    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.single_node_speculative_execution \
             (id text PRIMARY KEY)";

    session.query(cql).await.expect("create table error");

    let query_insert = "INSERT INTO cdrs_test.single_node_speculative_execution \
                      (id) VALUES (?)";

    let items = vec!["1".to_string(), "2".to_string(), "3".to_string()];

    for item in items {
        let values = query_values!(item);
        session
            .query_with_values(query_insert, values)
            .await
            .expect("insert item error");
    }

    let cql = "SELECT * FROM cdrs_test.single_node_speculative_execution WHERE id IN ?";
    let criteria = vec!["1".to_string(), "3".to_string()];

    let rows = session
        .query_with_values(cql, query_values!(criteria.clone()))
        .await
        .expect("select values query error")
        .response_body()
        .expect("get body error")
        .into_rows()
        .expect("converting into rows error");

    assert_eq!(rows.len(), criteria.len());

    let found_all_matching_criteria = criteria.iter().all(|criteria_item: &String| {
        rows.iter().any(|row| {
            let id: String = row.get_r_by_name("id").expect("id");

            criteria_item.clone() == id
        })
    });

    assert!(
        found_all_matching_criteria,
        "should find at least one element for each criteria"
    );
}
