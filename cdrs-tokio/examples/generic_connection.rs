use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfig;
use cdrs_tokio::cluster::session::{
    NodeDistanceEvaluatorWrapper, ReconnectionPolicyWrapper, RetryPolicyWrapper,
    DEFAULT_TRANSPORT_BUFFER_SIZE,
};
use cdrs_tokio::cluster::{ConnectionManager, KeyspaceHolder};
use cdrs_tokio::compression::Compression;
use cdrs_tokio::frame::{Envelope, Version};
use cdrs_tokio::frame_encoding::ProtocolFrameEncodingFactory;
use cdrs_tokio::future::BoxFuture;
use cdrs_tokio::load_balancing::node_distance_evaluator::AllLocalNodeDistanceEvaluator;
use cdrs_tokio::retry::{ConstantReconnectionPolicy, ReconnectionPolicy};
use cdrs_tokio::IntoCdrsValue;
use cdrs_tokio::{
    authenticators::{SaslAuthenticatorProvider, StaticPasswordAuthenticatorProvider},
    cluster::session::Session,
    cluster::{GenericClusterConfig, TcpConnectionManager},
    error::Result,
    load_balancing::RoundRobinLoadBalancingStrategy,
    query::*,
    query_values,
    retry::DefaultRetryPolicy,
    transport::TransportTcp,
    types::prelude::*,
    TryFromRow, TryFromUdt,
};
use futures::FutureExt;
use maplit::hashmap;
use std::{
    collections::HashMap,
    net::IpAddr,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::sync::mpsc::Sender;

type CurrentSession = Session<
    TransportTcp,
    VirtualConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, VirtualConnectionManager>,
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
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    version: Version,
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
    fn try_connection(
        &self,
        event_handler: Option<Sender<Envelope>>,
        error_handler: Option<Sender<Error>>,
        addr: SocketAddr,
        max_retries: usize,
    ) -> BoxFuture<Result<TransportTcp>> {
        self.inner.try_connection(
            event_handler,
            error_handler,
            rewrite(addr, &self.mask, &self.actual),
            max_retries,
        )
    }
}

impl VirtualConnectionManager {
    async fn new(
        config: &VirtualClusterConfig,
        keyspace_holder: Arc<KeyspaceHolder>,
    ) -> Result<Self> {
        Ok(VirtualConnectionManager {
            inner: TcpConnectionManager::new(
                config.authenticator.clone(),
                keyspace_holder,
                config.reconnection_policy.clone(),
                Box::<ProtocolFrameEncodingFactory>::default(),
                Compression::None,
                DEFAULT_TRANSPORT_BUFFER_SIZE,
                true,
                config.version,
                #[cfg(feature = "http-proxy")]
                None,
            ),
            mask: config.mask,
            actual: config.actual,
        })
    }
}

impl GenericClusterConfig<TransportTcp, VirtualConnectionManager> for VirtualClusterConfig {
    fn create_manager(
        &self,
        keyspace_holder: Arc<KeyspaceHolder>,
    ) -> BoxFuture<Result<VirtualConnectionManager>> {
        // create a connection manager that points at the rewritten address so that's where it connects, but
        // then return a manager with the 'virtual' address for internal purposes.
        VirtualConnectionManager::new(self, keyspace_holder).boxed()
    }

    fn event_channel_capacity(&self) -> usize {
        32
    }

    fn version(&self) -> Version {
        self.version
    }

    fn connection_pool_config(&self) -> ConnectionPoolConfig {
        Default::default()
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
        reconnection_policy: reconnection_policy.clone(),
        version: Version::V5,
    };
    let nodes = [
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9042),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9043),
    ];
    let load_balancing = RoundRobinLoadBalancingStrategy::new();

    let mut session = cdrs_tokio::cluster::connect_generic(
        &cluster_config,
        nodes,
        load_balancing,
        RetryPolicyWrapper(Box::<DefaultRetryPolicy>::default()),
        ReconnectionPolicyWrapper(reconnection_policy),
        NodeDistanceEvaluatorWrapper(Box::<AllLocalNodeDistanceEvaluator>::default()),
        None,
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

//noinspection DuplicatedCode
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

//noinspection DuplicatedCode
async fn select_struct(session: &mut CurrentSession) {
    let select_struct_cql = "SELECT * FROM test_ks.my_test_table";
    let rows = session
        .query(select_struct_cql)
        .await
        .expect("query")
        .response_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        println!("struct got: {my_row:?}");
    }
}

//noinspection DuplicatedCode
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
