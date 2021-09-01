use std::sync::Arc;

use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::query::*;
use cdrs_tokio::query_values;

use cdrs_tokio::frame::Serialize;
use cdrs_tokio::load_balancing::RoundRobin;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::from_cdrs::FromCdrsByName;
use cdrs_tokio::types::prelude::*;
use cdrs_tokio_helpers_derive::*;

type CurrentSession = Session<TransportTcp, TcpConnectionManager, RoundRobin<TcpConnectionManager>>;

#[tokio::main]
async fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042".parse().unwrap())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let lb = RoundRobin::new();
    let no_compression: Arc<CurrentSession> =
        Arc::new(TcpSessionBuilder::new(lb, cluster_config).build());

    create_keyspace(no_compression.clone()).await;
    create_table(no_compression.clone()).await;

    for i in 0..20 {
        let thread_session = no_compression.clone();
        tokio::spawn(insert_struct(thread_session, i))
            .await
            .expect("thread error");
    }

    select_struct(no_compression).await;
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key)
    }
}

#[derive(Debug, Clone, PartialEq, IntoCdrsValue, TryFromUdt)]
struct User {
    username: String,
}

async fn create_keyspace(session: Arc<CurrentSession>) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session
        .query(create_ks)
        .await
        .expect("Keyspace creation error");
}

async fn create_table(session: Arc<CurrentSession>) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.multi_thread_table (key int PRIMARY KEY);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn insert_struct(session: Arc<CurrentSession>, key: i32) {
    let row = RowStruct { key };

    let insert_struct_cql = "INSERT INTO test_ks.multi_thread_table (key) VALUES (?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .await
        .expect("insert");
}

async fn select_struct(session: Arc<CurrentSession>) {
    let select_struct_cql = "SELECT * FROM test_ks.multi_thread_table";
    let rows = session
        .query(select_struct_cql)
        .await
        .expect("query")
        .body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        println!("struct got: {:?}", my_row);
    }
}
