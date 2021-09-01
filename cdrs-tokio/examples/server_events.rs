use std::sync::Arc;

use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
use cdrs_tokio::frame::events::{
    SchemaChangeTarget, SchemaChangeType, ServerEvent, SimpleServerEvent,
};
use cdrs_tokio::load_balancing::RoundRobin;

#[tokio::main]
async fn main() {
    let nodes = NodeTcpConfigBuilder::new()
        .with_node_address("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
        .build()
        .await
        .unwrap();
    let cluster_config = ClusterTcpConfig(nodes);
    let lb = RoundRobin::new();
    let no_compression = TcpSessionBuilder::new(lb, cluster_config).build();

    let (listener, stream) = no_compression
        .listen(
            "127.0.0.1:9042".parse().unwrap(),
            Arc::new(NoneAuthenticatorProvider),
            vec![SimpleServerEvent::SchemaChange],
        )
        .await
        .expect("listen error");

    tokio::spawn(listener.start());

    let new_tables = stream
        // inspects all events in a stream
        .inspect(|event| println!("inspect event {:?}", event))
        // filter by event's type: schema changes
        .filter(|event| event == &SimpleServerEvent::SchemaChange)
        // filter by event's specific information: new table was added
        .filter(|event| match event {
            ServerEvent::SchemaChange(ref event) => {
                event.change_type == SchemaChangeType::Created
                    && event.target == SchemaChangeTarget::Table
            }
            _ => false,
        });

    println!("Start listen for server events");

    for change in new_tables {
        println!("server event {:?}", change);
    }
}
