#[cfg(feature = "e2e-tests")]
use std::sync::Arc;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::frame::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::Session;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::TcpConnectionManager;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::error::Result;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::transport::TransportTcp;
#[cfg(feature = "e2e-tests")]
use regex::Regex;

#[cfg(feature = "e2e-tests")]
pub const ADDR: &str = "127.0.0.1:9042";

#[cfg(feature = "e2e-tests")]
pub type CurrentSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

#[cfg(feature = "e2e-tests")]
#[allow(dead_code)]
pub async fn setup(create_table_cql: &'static str, version: Version) -> Result<CurrentSession> {
    setup_multiple(&[create_table_cql], version).await
}

#[cfg(feature = "e2e-tests")]
pub async fn setup_multiple(
    create_cqls: &[&'static str],
    version: Version,
) -> Result<CurrentSession> {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(ADDR.into())
        .with_version(version)
        .build()
        .await
        .unwrap();
    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .with_reconnection_policy(Arc::new(NeverReconnectionPolicy::default()))
        .build()
        .unwrap();
    let re_table_name = Regex::new(r"CREATE TABLE IF NOT EXISTS (\w+\.\w+)").unwrap();

    let create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
         replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
         AND durable_writes = false";
    session.query(create_keyspace_query).await?;

    for create_cql in create_cqls.iter() {
        let table_name = re_table_name
            .captures(create_cql)
            .map(|cap| cap.get(1).unwrap().as_str());

        // Re-using tables is a lot faster than creating/dropping them for every test.
        // But if table definitions change while editing tests
        // the old tables need to be dropped. For example by uncommenting the following lines.
        // if let Some(table_name) = table_name {
        //     let cql = format!("DROP TABLE IF EXISTS {}", table_name);
        //     let query = QueryBuilder::new(cql).finalize();
        //     session.query(query, true, true)?;
        // }

        session.query(create_cql.to_owned()).await?;

        if let Some(table_name) = table_name {
            let cql = format!("TRUNCATE TABLE {table_name}");
            session.query(cql).await?;
        }
    }

    Ok(session)
}
