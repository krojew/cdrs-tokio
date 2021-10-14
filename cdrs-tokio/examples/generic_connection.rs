use std::{
    collections::HashMap,
    net::IpAddr,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use cdrs_tokio::{
    authenticators::{SaslAuthenticatorProvider, StaticPasswordAuthenticatorProvider},
    cluster::session::Session,
    cluster::{GenericClusterConfig, TcpConnectionManager},
    error::Result,
    load_balancing::RoundRobinBalancingStrategy,
    query::*,
    query_values,
    retry::DefaultRetryPolicy,
    transport::TransportTcp,
    types::from_cdrs::FromCdrsByName,
    types::prelude::*,
};

use cdrs_tokio_helpers_derive::*;

use cdrs_tokio::cluster::session::{
    ReconnectionPolicyWrapper, RetryPolicyWrapper, DEFAULT_TRANSPORT_BUFFER_SIZE,
};
use cdrs_tokio::cluster::{ConnectionManager, KeyspaceHolder};
use cdrs_tokio::compression::Compression;
use cdrs_tokio::frame::{Frame, Serialize};
use cdrs_tokio::future::BoxFuture;
use cdrs_tokio::retry::{ConstantReconnectionPolicy, ReconnectionPolicy};
use futures::FutureExt;
use maplit::hashmap;
use tokio::sync::mpsc::Sender;

type CurrentSession = Session<
    TransportTcp,
    VirtualConnectionManager,
    RoundRobinBalancingStrategy<TransportTcp, VirtualConnectionManager>,
>;

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
    authenticator: Arc<dyn SaslAuthenticatorProvider + Sync + Send>,
    mask: Ipv4Addr,
    actual: Ipv4Addr,
    keyspace_holder: Arc<KeyspaceHolder>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
}

fn rewrite(addr: SocketAddr, mask: &Ipv4Addr, actual: &Ipv4Addr) -> SocketAddr {
    match addr {
        SocketAddr::V4(addr) => {
            let virt = addr.ip().octets();
            let mask = mask.octets();
            let actual = actual.octets();
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(
                    (virt[0] & !mask[0]) | (actual[0] & mask[0]),
                    (virt[1] & !mask[1]) | (actual[1] & mask[1]),
                    (virt[2] & !mask[2]) | (actual[2] & mask[2]),
                    (virt[3] & !mask[3]) | (actual[3] & mask[3]),
                )),
                addr.port(),
            )
        }
        SocketAddr::V6(_) => {
            panic!("IpV6 is unsupported!");
        }
    }
}

struct VirtualConnectionManager {
    inner: TcpConnectionManager,
    mask: Ipv4Addr,
    actual: Ipv4Addr,
}

impl ConnectionManager<TransportTcp> for VirtualConnectionManager {
    fn connection(
        &self,
        event_handler: Option<Sender<Frame>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
    ) -> BoxFuture<Result<TransportTcp>> {
        self.inner.connection(
            event_handler,
            error_handler,
            rewrite(addr, &self.mask, &self.actual),
        )
    }
}

impl VirtualConnectionManager {
    async fn new(config: &VirtualClusterConfig) -> Result<Self> {
        Ok(VirtualConnectionManager {
            inner: TcpConnectionManager::new(
                config.authenticator.clone(),
                config.keyspace_holder.clone(),
                config.reconnection_policy.clone(),
                Compression::None,
                DEFAULT_TRANSPORT_BUFFER_SIZE,
                true,
            ),
            mask: config.mask,
            actual: config.actual,
        })
    }
}

impl GenericClusterConfig<TransportTcp, VirtualConnectionManager> for VirtualClusterConfig {
    fn create_manager(&self) -> BoxFuture<Result<VirtualConnectionManager>> {
        // create a connection manager that points at the rewritten address so that's where it connects, but
        // then return a manager with the 'virtual' address for internal purposes.
        VirtualConnectionManager::new(self).boxed()
    }

    fn event_channel_capacity(&self) -> usize {
        32
    }
}

#[tokio::main]
async fn main() {
    let user = "user";
    let password = "password";
    let authenticator = Arc::new(StaticPasswordAuthenticatorProvider::new(&user, &password));
    let mask = Ipv4Addr::new(255, 255, 255, 0);
    let actual = Ipv4Addr::new(127, 0, 0, 0);

    let reconnection_policy = Arc::new(ConstantReconnectionPolicy::default());

    let cluster_config = VirtualClusterConfig {
        authenticator,
        mask,
        actual,
        keyspace_holder: Default::default(),
        reconnection_policy: reconnection_policy.clone(),
    };
    let nodes = [
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9042),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9043),
    ];
    let load_balancing = RoundRobinBalancingStrategy::new();

    let mut session = cdrs_tokio::cluster::connect_generic(
        &cluster_config,
        nodes,
        load_balancing,
        RetryPolicyWrapper(Box::new(DefaultRetryPolicy::default())),
        ReconnectionPolicyWrapper(reconnection_policy),
    )
    .await
    .expect("session should be created");

    create_keyspace(&mut session).await;
    create_udt(&mut session).await;
    create_table(&mut session).await;
    insert_struct(&mut session).await;
    select_struct(&mut session).await;
    update_struct(&mut session).await;
    delete_struct(&mut session).await;
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
