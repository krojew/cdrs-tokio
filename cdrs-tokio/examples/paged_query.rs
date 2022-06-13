use std::sync::Arc;

use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, PagerState, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::*;
use cdrs_tokio::query_values;

use cdrs_tokio::frame::TryFromRow;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::{IntoCdrsValue, TryFromRow};

type CurrentSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key)
    }
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct AnotherTestTable {
    a: i32,
    b: i32,
    c: i32,
    d: i32,
    e: i32,
}

impl AnotherTestTable {
    fn into_query_values(self) -> QueryValues {
        query_values!("a" => self.a, "b" => self.b, "c" => self.c, "d" => self.d, "e" => self.e)
    }
}

#[tokio::main]
async fn main() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let lb = RoundRobinLoadBalancingStrategy::new();
    let session = TcpSessionBuilder::new(lb, cluster_config).build().unwrap();

    create_keyspace(&session).await;
    create_udt(&session).await;
    create_table(&session).await;
    fill_table(&session).await;
    println!("Internal pager state\n");
    paged_selection_query(&session).await;
    println!("\n\nExternal pager state for stateless executions\n");
    paged_selection_query_with_state(&session, PagerState::new()).await;
    println!("\n\nPager with query values (list)\n");
    paged_with_values_list(&session).await;
    println!("\n\nPager with query value (no list)\n");
    paged_with_value(&session).await;
    println!("\n\nFinished paged query tests\n");
}

async fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session
        .query(create_ks)
        .await
        .expect("Keyspace creation error");
}

async fn create_udt(session: &CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS test_ks.user (username text)";
    session
        .query(create_type_cql)
        .await
        .expect("Keyspace creation error");
}

async fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
         user test_ks.user, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn fill_table(session: &CurrentSession) {
    let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";

    for k in 100..110 {
        let row = RowStruct { key: k as i32 };

        session
            .query_with_values(insert_struct_cql, row.into_query_values())
            .await
            .expect("insert");
    }
}

async fn paged_selection_query(session: &CurrentSession) {
    let q = "SELECT * FROM test_ks.my_test_table;";
    let mut pager = session.paged(2);
    let mut query_pager = pager.query(q);

    loop {
        let rows = query_pager.next().await.expect("pager next");
        for row in rows {
            let my_row = RowStruct::try_from_row(row).expect("decode row");
            println!("row - {:?}", my_row);
        }

        if !query_pager.has_more() {
            break;
        }
    }
}

async fn paged_with_value(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.another_test_table (a int, b int, c int, d int, e int, primary key((a, b), c, d));";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");

    for v in 1..=10 {
        session
            .query_with_values(
                "INSERT INTO test_ks.another_test_table (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)",
                AnotherTestTable {
                    a: 1,
                    b: 1,
                    c: 2,
                    d: v,
                    e: v,
                }
                .into_query_values(),
            )
            .await
            .unwrap();
    }

    let q = "SELECT * FROM test_ks.another_test_table where a = ? and b = 1 and c = ?";
    let mut pager = session.paged(3);
    let mut query_pager = pager.query_with_param(
        q,
        QueryParamsBuilder::new()
            .with_values(query_values!(1, 2))
            .build(),
    );

    // Oddly enough, this returns false the first time...
    assert!(!query_pager.has_more());

    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(3, rows.len());
    let rows = query_pager.next().await.expect("pager next");
    assert_eq!(1, rows.len());

    assert!(!query_pager.has_more());
}

async fn paged_with_values_list(session: &CurrentSession) {
    let q = "SELECT * FROM test_ks.my_test_table where key in ?";
    let mut pager = session.paged(2);
    let mut query_pager = pager.query_with_param(
        q,
        QueryParamsBuilder::new()
            .with_values(query_values!(vec![100, 101, 102, 103, 104]))
            .build(),
    );

    // Macro instead of a function or closure, since problem with lifetimes
    macro_rules! assert_amount_query_pager {
        ($row_amount: expr) => {{
            let rows = query_pager.next().await.expect("pager next");

            assert_eq!($row_amount, rows.len());
        }};
    }

    println!("Testing values 100 and 101");
    assert_amount_query_pager!(2);
    assert!(query_pager.has_more());
    assert!(!query_pager.pager_state().cursor().unwrap().is_empty());
    println!("Testing values 102 and 103");
    assert_amount_query_pager!(2);
    assert!(query_pager.has_more());
    assert!(!query_pager.pager_state().cursor().unwrap().is_empty());
    println!("Testing value 104");
    assert_amount_query_pager!(1);
    // Now no more rows should be queried
    println!("Testing no more values are present");
    assert!(!query_pager.has_more());
    assert!(query_pager.pager_state().cursor().is_none());
}

async fn paged_selection_query_with_state(session: &CurrentSession, state: PagerState) {
    let mut st = state;

    loop {
        let q = "SELECT * FROM test_ks.my_test_table;";
        let mut pager = session.paged(2);
        let mut query_pager = pager.query_with_pager_state(q, st);

        let rows = query_pager.next().await.expect("pager next");
        for row in rows {
            let my_row = RowStruct::try_from_row(row).expect("decode row");
            println!("row - {:?}", my_row);
        }

        if !query_pager.has_more() {
            break;
        }

        st = query_pager.pager_state();
    }
}
