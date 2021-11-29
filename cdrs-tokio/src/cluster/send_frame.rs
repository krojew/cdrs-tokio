use crate::cluster::session::Session;
use crate::cluster::ConnectionManager;
use crate::load_balancing::{LoadBalancingStrategy, Request};
use crate::retry::{QueryInfo, RetryDecision};
use crate::transport::CdrsTransport;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::error;
use cassandra_protocol::frame::Frame;
use cassandra_protocol::query::query_params::Murmur3Token;

pub async fn send_frame<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + Send + Sync + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
>(
    session: &Session<T, CM, LB>,
    frame: Frame,
    is_idempotent: bool,
    keyspace: Option<&str>,
    token: Option<Murmur3Token>,
    routing_key: Option<&[u8]>,
    consistency: Option<Consistency>,
) -> error::Result<Frame> {
    let mut retry_session = session.retry_policy().new_session();

    let current_keyspace = session.current_keyspace();
    let request = Request::new(
        keyspace.or_else(|| current_keyspace.as_ref().map(|keyspace| &***keyspace)),
        token,
        routing_key,
        consistency,
    );
    let query_plan = session.query_plan(Some(request));

    'next_node: for node in query_plan {
        loop {
            let transport = node.persistent_connection().await?;
            match transport.write_frame(&frame).await {
                Ok(frame) => return Ok(frame),
                Err(error) => {
                    let query_info = QueryInfo {
                        error: &error,
                        is_idempotent,
                    };

                    match retry_session.decide(query_info) {
                        RetryDecision::RetrySameNode => continue,
                        RetryDecision::RetryNextNode => continue 'next_node,
                        RetryDecision::DontRetry => return Err(error),
                    }
                }
            }
        }
    }

    Err("No nodes available in query plan!".into())
}
