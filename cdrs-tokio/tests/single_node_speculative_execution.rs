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
use cdrs_tokio::speculative_execution::{
    ConstantSpeculativeExecutionPolicy, Context, SpeculativeExecutionPolicy,
};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::statement::StatementParamsBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use std::sync::atomic::{AtomicUsize, Ordering};

// Remembers the highest number of running executions the session reported.
#[cfg(feature = "e2e-tests")]
struct RecordingSpeculativeExecutionPolicy {
    inner: ConstantSpeculativeExecutionPolicy,
    max_running_executions: Arc<AtomicUsize>,
}

#[cfg(feature = "e2e-tests")]
impl SpeculativeExecutionPolicy for RecordingSpeculativeExecutionPolicy {
    fn execution_interval(&self, context: &Context) -> Option<Duration> {
        self.max_running_executions
            .fetch_max(context.running_executions, Ordering::Relaxed);
        self.inner.execution_interval(context)
    }
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn single_node_speculative_execution() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .build()
        .await
        .unwrap();

    let max_running_executions = Arc::new(AtomicUsize::new(0));

    // Speculative executions start right away. With a single node, they find no node to send the
    // request to, which must not fail the request still running on that node.
    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
        .with_speculative_execution_policy(Box::new(RecordingSpeculativeExecutionPolicy {
            inner: ConstantSpeculativeExecutionPolicy::new(5, Duration::ZERO),
            max_running_executions: max_running_executions.clone(),
        }))
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

    // speculative executions are only started for idempotent statements
    for item in items {
        let values = query_values!(item);
        session
            .query_with_params(
                query_insert,
                StatementParamsBuilder::new()
                    .with_values(values)
                    .idempotent(true)
                    .build(),
            )
            .await
            .expect("insert item error");
    }

    let cql = "SELECT * FROM cdrs_test.single_node_speculative_execution WHERE id IN ?";
    let criteria = vec!["1".to_string(), "3".to_string()];

    let rows = session
        .query_with_params(
            cql,
            StatementParamsBuilder::new()
                .with_values(query_values!(criteria.clone()))
                .idempotent(true)
                .build(),
        )
        .await
        .expect("select values query error")
        .response_body()
        .expect("get body error")
        .into_rows()
        .expect("converting into rows error");

    assert_eq!(rows.len(), criteria.len());
    assert!(
        max_running_executions.load(Ordering::Relaxed) > 1,
        "no speculative execution was started"
    );

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
