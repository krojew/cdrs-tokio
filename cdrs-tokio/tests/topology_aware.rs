mod common;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::SessionBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::TcpSessionBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::node_distance_evaluator::TopologyAwareNodeDistanceEvaluator;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::TopologyAwareLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn query_topology_aware() {
    // this test is essentially the same as query values, but checks if topology aware load
    // balancing works

    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let session = TcpSessionBuilder::new(
        TopologyAwareLoadBalancingStrategy::new(None, false),
        cluster_config,
    )
    .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
    .with_node_distance_evaluator(Box::new(TopologyAwareNodeDistanceEvaluator::new(
        "datacenter1".into(),
    )))
    .build()
    .await
    .unwrap();

    let create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
         replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} \
         AND durable_writes = false";
    session.query(create_keyspace_query).await.unwrap();

    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_query_values_in \
             (id text PRIMARY KEY)";

    session.query(cql).await.unwrap();

    let query_insert = "INSERT INTO cdrs_test.test_query_values_in \
                      (id) VALUES (?)";

    let items = vec!["1".to_string(), "2".to_string(), "3".to_string()];

    for item in items {
        let values = query_values!(item);
        session
            .query_with_values(query_insert, values)
            .await
            .expect("insert item error");
    }

    let cql = "SELECT * FROM cdrs_test.test_query_values_in WHERE id IN ?";
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
