use cassandra_protocol::error;
use cassandra_protocol::frame::Frame;
use std::sync::Arc;

use crate::cluster::topology::Node;
use crate::cluster::ConnectionManager;
use crate::retry::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::CdrsTransport;

/// Mid-level interface for sending frames to the cluster. Uses a query plan to route frame to
/// appropriate node, and retry policy for error handling. Returns `None` if no nodes were present
/// in the query plan.
pub async fn send_frame<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static>(
    query_plan: impl Iterator<Item = Arc<Node<T, CM>>>,
    frame: &Frame,
    is_idempotent: bool,
    mut retry_session: Box<dyn RetrySession + Send + Sync>,
) -> Option<error::Result<Frame>> {
    let mut errors = vec![];
    'next_node: for node in query_plan {
        loop {
            let transport = node.persistent_connection().await;
            match transport {
                Ok(transport) => match transport.write_frame(frame).await {
                    Ok(frame) => return Some(Ok(frame)),
                    Err(error) => {
                        let query_info = QueryInfo {
                            error: &error,
                            is_idempotent,
                        };

                        match retry_session.decide(query_info) {
                            RetryDecision::RetrySameNode => continue,
                            RetryDecision::RetryNextNode => continue 'next_node,
                            RetryDecision::DontRetry => return Some(Err(error)),
                        }
                    }
                },
                Err(error) => {
                    errors.push(error);
                    continue 'next_node;
                }
            }
        }
    }

    if errors.len() > 0 {
        return Some(Err(errors.remove(1)));
    }

    None
}
