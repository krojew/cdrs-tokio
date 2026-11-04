use cassandra_protocol::error;
use cassandra_protocol::frame::Envelope;
use std::sync::Arc;

use crate::cluster::topology::Node;
use crate::cluster::ConnectionManager;
use crate::retry::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::CdrsTransport;

/// Mid-level interface for sending envelopes to the cluster. Uses a query plan to route the envelope
/// to the appropriate node, and retry policy for error handling. Returns `None` if no nodes were
/// present in the query plan.
pub async fn send_envelope<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static>(
    query_plan: impl Iterator<Item = Arc<Node<T, CM>>>,
    envelope: &Envelope,
    is_idempotent: bool,
    mut retry_session: Box<dyn RetrySession + Send + Sync>,
) -> Option<error::Result<Envelope>> {
    let mut result = None;

    'next_node: for node in query_plan {
        loop {
            let transport = node.persistent_connection().await;
            match transport {
                Ok(transport) => match transport.write_envelope(envelope, false).await {
                    Ok(envelope) => return Some(Ok(envelope)),
                    Err(error) => {
                        let query_info = QueryInfo {
                            error: &error,
                            is_idempotent,
                        };

                        match retry_session.decide(query_info) {
                            RetryDecision::RetrySameNode => continue,
                            RetryDecision::RetryNextNode => {
                                result = Some(Err(error));
                                continue 'next_node;
                            }
                            RetryDecision::DontRetry => return Some(Err(error)),
                        }
                    }
                },
                // save the error, but keep trying, since another node might be up
                Err(error) => {
                    result = Some(Err(error));
                    continue 'next_node;
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use cassandra_protocol::error::Error;
    use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
    use cassandra_protocol::frame::{Envelope, Version};
    use futures::future::ready;
    use futures::FutureExt;

    use crate::cluster::send_envelope::send_envelope;
    use crate::cluster::test_support::*;
    use crate::retry::{DefaultRetryPolicy, RetryPolicy};

    #[tokio::test]
    async fn should_return_last_error_when_every_node_asks_for_next_node() {
        let connection_manager = TestConnectionManager::new(|addr, _| {
            ready(Err(server_error(addr, ErrorType::Overloaded))).boxed()
        });
        let connection_pool_factory =
            create_connection_pool_factory(connection_manager, Version::V4);
        let nodes = vec![
            create_node(&connection_pool_factory, node_address(1)),
            create_node(&connection_pool_factory, node_address(2)),
        ];

        let result = send_envelope(
            nodes.into_iter(),
            &Envelope::new_req_options(Version::V4),
            true,
            DefaultRetryPolicy.new_session(),
        )
        .await;

        match result {
            Some(Err(Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Overloaded,
                        ..
                    },
                addr,
            })) => assert_eq!(addr, node_address(2)),
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_return_none_for_empty_query_plan() {
        let result = send_envelope(
            Vec::<std::sync::Arc<TestNode>>::new().into_iter(),
            &Envelope::new_req_options(Version::V4),
            true,
            DefaultRetryPolicy.new_session(),
        )
        .await;

        assert!(result.is_none());
    }
}
