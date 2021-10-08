#[cfg(feature = "e2e-tests")]
use std::collections::HashMap;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[cfg(feature = "e2e-tests")]
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::SessionBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::TcpSessionBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::map::Map;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::{AsRust, ByName, IntoRustByName};

#[cfg(feature = "e2e-tests")]
#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn create_keyspace() {
    let nodes = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(nodes);
    let lb = RoundRobinBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .build();

    let drop_query = "DROP KEYSPACE IF EXISTS create_ks_test";
    let keyspace_dropped = session.query(drop_query).await.is_ok();
    assert!(keyspace_dropped, "Should drop new keyspace without errors");

    let create_query = "CREATE KEYSPACE IF NOT EXISTS create_ks_test WITH \
                        replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                        AND durable_writes = false";
    let keyspace_created = session.query(create_query).await.is_ok();
    assert!(
        keyspace_created,
        "Should create new keyspace without errors"
    );

    let select_query =
        "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'create_ks_test'";
    let keyspace_selected = session
        .query(select_query)
        .await
        .expect("select keyspace query")
        .body()
        .expect("get select keyspace query body")
        .into_rows()
        .expect("convert keyspaces results into rows");

    assert_eq!(keyspace_selected.len(), 1);
    let keyspace = &keyspace_selected[0];

    let keyspace_name: String = keyspace
        .get_r_by_name("keyspace_name")
        .expect("keyspace name into rust error");
    assert_eq!(
        keyspace_name,
        "create_ks_test".to_string(),
        "wrong keyspace name"
    );

    let durable_writes: bool = keyspace
        .get_r_by_name("durable_writes")
        .expect("durable writes into rust error");
    assert!(!durable_writes, "wrong durable writes");

    let mut expected_strategy_options: HashMap<String, String> = HashMap::new();
    expected_strategy_options.insert("replication_factor".to_string(), "1".to_string());
    expected_strategy_options.insert(
        "class".to_string(),
        "org.apache.cassandra.locator.SimpleStrategy".to_string(),
    );
    let strategy_options: HashMap<String, String> = keyspace
        .r_by_name::<Map>("replication")
        .expect("strategy options into rust error")
        .as_r_rust()
        .expect("uuid_key_map");
    assert_eq!(
        expected_strategy_options, strategy_options,
        "wrong strategy options"
    );
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn alter_keyspace() {
    let nodes = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(nodes);
    let lb = RoundRobinBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .build();

    let drop_query = "DROP KEYSPACE IF EXISTS alter_ks_test";
    let keyspace_dropped = session.query(drop_query).await.is_ok();
    assert!(keyspace_dropped, "Should drop new keyspace without errors");

    let create_query = "CREATE KEYSPACE IF NOT EXISTS alter_ks_test WITH \
                        replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                        AND durable_writes = false";
    let keyspace_created = session.query(create_query).await.is_ok();
    assert!(
        keyspace_created,
        "Should create new keyspace without errors"
    );

    let alter_query = "ALTER KEYSPACE alter_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 3} \
                       AND durable_writes = false";
    assert!(
        session.query(alter_query).await.is_ok(),
        "alter should be without errors"
    );

    let select_query =
        "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'alter_ks_test'";
    let keyspace_selected = session
        .query(select_query)
        .await
        .expect("select keyspace query")
        .body()
        .expect("get select keyspace query body")
        .into_rows()
        .expect("convert keyspaces results into rows");

    assert_eq!(keyspace_selected.len(), 1);
    let keyspace = &keyspace_selected[0];

    let strategy_options: HashMap<String, String> = keyspace
        .r_by_name::<Map>("replication")
        .expect("strategy options into rust error")
        .as_r_rust()
        .expect("uuid_key_map");

    assert_eq!(
        strategy_options
            .get("replication_factor")
            .expect("replication_factor unwrap"),
        &"3".to_string()
    );
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn use_keyspace() {
    let node = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(node);
    let lb = RoundRobinBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .build();

    let create_query = "CREATE KEYSPACE IF NOT EXISTS use_ks_test WITH \
                        replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                        AND durable_writes = false";
    let keyspace_created = session.query(create_query).await.is_ok();
    assert!(
        keyspace_created,
        "Should create new keyspace without errors"
    );

    let use_query = "USE use_ks_test";
    let keyspace_used = session
        .query(use_query)
        .await
        .expect("should use selected")
        .body()
        .expect("should get body")
        .into_set_keyspace()
        .expect("set keyspace")
        .body;
    assert_eq!(keyspace_used.as_str(), "use_ks_test", "wrong keyspace used");
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn drop_keyspace() {
    let nodes = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(nodes);
    let lb = RoundRobinBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .build();

    let create_query = "CREATE KEYSPACE IF NOT EXISTS drop_ks_test WITH \
                        replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                        AND durable_writes = false";
    let keyspace_created = session.query(create_query).await.is_ok();
    assert!(
        keyspace_created,
        "Should create new keyspace without errors"
    );

    let drop_query = "DROP KEYSPACE drop_ks_test";
    let keyspace_dropped = session.query(drop_query).await.is_ok();
    assert!(keyspace_dropped, "Should drop new keyspace without errors");
}
