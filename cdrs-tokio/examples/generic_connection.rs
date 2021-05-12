use std::{
    collections::HashMap,
    net::IpAddr,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    result::Result,
    sync::Arc,
};

use cdrs_tokio::{
    authenticators::{SaslAuthenticator, StaticPasswordAuthenticator},
    cluster::session::Session,
    cluster::TcpConnectionPool,
    cluster::{ConnectionPool, GenericClusterConfig, TcpConnectionsManager},
    frame::AsBytes,
    load_balancing::RoundRobin,
    query::*,
    query_values,
    retry::DefaultRetryPolicy,
    transport::TransportTcp,
    types::from_cdrs::FromCdrsByName,
    types::prelude::*,
};

use cdrs_tokio_helpers_derive::*;

use async_trait::async_trait;
use cdrs_tokio::cluster::session::RetryPolicyWrapper;
use maplit::hashmap;

type CurrentSession = Session<RoundRobin<TcpConnectionPool>>;

/// Implements a cluster configuration where the addresses to
/// connect to are different from the ones configured by replacing
/// the masked part of the address with a different subnet.
///
/// This would allow running your connection through a proxy
/// or mock server while also using a production configuration
/// and having your load balancing configuration be aware of the
/// 'real' addresses.
///
/// This is just a simple use for the generic configuration. By
/// replacing the transport itself you can do much more.
struct VirtualClusterConfig {
    authenticator: Arc<dyn SaslAuthenticator + Sync + Send>,
    mask: Ipv4Addr,
    actual: Ipv4Addr,
}

#[derive(Clone)]
struct VirtualConnectionAddress(SocketAddrV4);

impl VirtualConnectionAddress {
    fn rewrite(&self, mask: &Ipv4Addr, actual: &Ipv4Addr) -> SocketAddr {
        let virt = self.0.ip().octets();
        let mask = mask.octets();
        let actual = actual.octets();
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(
                (virt[0] & !mask[0]) | (actual[0] & mask[0]),
                (virt[1] & !mask[1]) | (actual[1] & mask[1]),
                (virt[2] & !mask[2]) | (actual[2] & mask[2]),
                (virt[3] & !mask[3]) | (actual[3] & mask[3]),
            )),
            self.0.port(),
        )
    }
}

#[async_trait]
impl GenericClusterConfig for VirtualClusterConfig {
    type Transport = TransportTcp;
    type Address = VirtualConnectionAddress;
    type Error = cdrs_tokio::Error;

    async fn connect(
        &self,
        addr: VirtualConnectionAddress,
    ) -> Result<ConnectionPool<Self::Transport>, Self::Error> {
        // create a connection manager that points at the rewritten address so that's where it connects, but
        // then return a pool with the 'virtual' address for internal purposes.
        let manager = TcpConnectionsManager::new(
            addr.rewrite(&self.mask, &self.actual),
            self.authenticator.clone(),
        );
        Ok(ConnectionPool::new(
            bb8::Pool::builder().build(manager).await?,
            SocketAddr::V4(addr.0),
        ))
    }
}

#[tokio::main]
async fn main() {
    let user = "user";
    let password = "password";
    let authenticator = Arc::new(StaticPasswordAuthenticator::new(&user, &password));
    let mask = Ipv4Addr::new(255, 255, 255, 0);
    let actual = Ipv4Addr::new(127, 0, 0, 0);

    let cluster_config = VirtualClusterConfig {
        authenticator,
        mask,
        actual,
    };
    let nodes = [
        VirtualConnectionAddress(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 9042)),
        VirtualConnectionAddress(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 9043)),
    ];
    let load_balancing = RoundRobin::new();
    let compression = cdrs_tokio::compression::Compression::None;

    let mut no_compression = cdrs_tokio::cluster::connect_generic_static(
        &cluster_config,
        &nodes,
        load_balancing,
        compression,
        RetryPolicyWrapper(Box::new(DefaultRetryPolicy::default())),
    )
    .await
    .expect("session should be created");

    create_keyspace(&mut no_compression).await;
    create_udt(&mut no_compression).await;
    create_table(&mut no_compression).await;
    insert_struct(&mut no_compression).await;
    select_struct(&mut no_compression).await;
    update_struct(&mut no_compression).await;
    delete_struct(&mut no_compression).await;
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
    user: User,
    map: HashMap<String, User>,
    list: Vec<User>,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key, "user" => self.user, "map" => self.map, "list" => self.list)
    }
}

#[derive(Debug, Clone, PartialEq, IntoCdrsValue, TryFromUdt)]
struct User {
    username: String,
}

async fn create_keyspace(session: &mut CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session
        .query(create_ks)
        .await
        .expect("Keyspace creation error");
}

async fn create_udt(session: &mut CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS test_ks.user (username text)";
    session
        .query(create_type_cql)
        .await
        .expect("Keyspace creation error");
}

async fn create_table(session: &mut CurrentSession) {
    let create_table_cql =
    "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
     user frozen<test_ks.user>, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn insert_struct(session: &mut CurrentSession) {
    let row = RowStruct {
        key: 3i32,
        user: User {
            username: "John".to_string(),
        },
        map: hashmap! { "John".to_string() => User { username: "John".to_string() } },
        list: vec![User {
            username: "John".to_string(),
        }],
    };

    let insert_struct_cql = "INSERT INTO test_ks.my_test_table \
                             (key, user, map, list) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .await
        .expect("insert");
}

async fn select_struct(session: &mut CurrentSession) {
    let select_struct_cql = "SELECT * FROM test_ks.my_test_table";
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

async fn update_struct(session: &mut CurrentSession) {
    let update_struct_cql = "UPDATE test_ks.my_test_table SET user = ? WHERE key = ?";
    let upd_user = User {
        username: "Marry".to_string(),
    };
    let user_key = 1i32;
    session
        .query_with_values(update_struct_cql, query_values!(upd_user, user_key))
        .await
        .expect("update");
}

async fn delete_struct(session: &mut CurrentSession) {
    let delete_struct_cql = "DELETE FROM test_ks.my_test_table WHERE key = ?";
    let user_key = 1i32;
    session
        .query_with_values(delete_struct_cql, query_values!(user_key))
        .await
        .expect("delete");
}
