use crate::cluster::session::Session;
use crate::cluster::{ConnectionManager, GetRetryPolicy};
use crate::error;
use crate::frame::{Flag, Frame};
use crate::load_balancing::{LoadBalancingStrategy, Request};
use crate::retry::{QueryInfo, RetryDecision};
use crate::transport::CdrsTransport;

pub fn prepare_flags(with_tracing: bool, with_warnings: bool) -> Vec<Flag> {
    let mut flags = vec![];

    if with_tracing {
        flags.push(Flag::Tracing);
    }

    if with_warnings {
        flags.push(Flag::Warning);
    }

    flags
}

pub(crate) async fn send_frame<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T> + Send + Sync + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
>(
    session: &Session<T, CM, LB>,
    frame: Frame,
    is_idempotent: bool,
    keyspace: Option<&str>,
) -> error::Result<Frame> {
    let mut retry_session = session.retry_policy().new_session();

    let current_keyspace = session.current_keyspace();
    let request =
        Request::new(keyspace.or_else(|| current_keyspace.as_ref().map(|keyspace| &***keyspace)));
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

    Err("No nodes in query plan!".into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn prepare_flags_test() {
        assert_eq!(prepare_flags(true, false), vec![Flag::Tracing]);
        assert_eq!(prepare_flags(false, true), vec![Flag::Warning]);
        assert_eq!(
            prepare_flags(true, true),
            vec![Flag::Tracing, Flag::Warning]
        );
    }
}
