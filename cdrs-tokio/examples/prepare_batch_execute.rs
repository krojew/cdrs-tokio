use std::sync::Arc;

use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{new as new_session, Session};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs_tokio::load_balancing::RoundRobin;
use cdrs_tokio::query::*;
use cdrs_tokio::query_values;
use cdrs_tokio::retry::DefaultRetryPolicy;

use cdrs_tokio::frame::AsBytes;
use cdrs_tokio::types::from_cdrs::FromCdrsByName;
use cdrs_tokio::types::prelude::*;

use cdrs_tokio_helpers_derive::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool>>;

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        // **IMPORTANT NOTE:** query values should be WITHOUT NAMES
        // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L413
        query_values!(self.key)
    }
}

#[tokio::main]
async fn main() {
    let node =
        NodeTcpConfigBuilder::new("127.0.0.1:9042", Arc::new(NoneAuthenticatorProvider)).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let lb = RoundRobin::new();
    let mut no_compression =
        new_session(&cluster_config, lb, Box::new(DefaultRetryPolicy::default()))
            .await
            .expect("session should be created");

    create_keyspace(&mut no_compression).await;
    create_table(&mut no_compression).await;

    let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";
    let prepared_query = no_compression
        .prepare(insert_struct_cql)
        .await
        .expect("Prepare query error");

    for k in 100..110 {
        let row = RowStruct { key: k as i32 };

        insert_row(&mut no_compression, row, &prepared_query).await;
    }

    batch_few_queries(&mut no_compression, &insert_struct_cql).await;
}

async fn create_keyspace(session: &mut CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session
        .query(create_ks)
        .await
        .expect("Keyspace creation error");
}

async fn create_table(session: &mut CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn insert_row(session: &mut CurrentSession, row: RowStruct, prepared_query: &PreparedQuery) {
    session
        .exec_with_values(prepared_query, row.into_query_values())
        .await
        .expect("exec_with_values error");
}

async fn batch_few_queries(session: &mut CurrentSession, query: &str) {
    let prepared_query = session.prepare(query).await.expect("Prepare query error");
    let row_1 = RowStruct { key: 1001 };
    let row_2 = RowStruct { key: 2001 };

    let batch = BatchQueryBuilder::new()
        .add_query_prepared(prepared_query, row_1.into_query_values())
        .add_query(query, row_2.into_query_values())
        .finalize()
        .expect("batch builder");

    session
        .batch_with_params(batch)
        .await
        .expect("batch query error");
}
