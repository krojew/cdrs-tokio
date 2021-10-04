use crate::cluster::session::Session;
use crate::cluster::{ConnectionManager, GetRetryPolicy};
use crate::error;
use crate::error::Error;
use crate::frame::{Flag, Frame};
use crate::load_balancing::LoadBalancingStrategy;
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
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
>(
    sender: &Session<T, CM, LB>,
    frame: Frame,
    is_idempotent: bool,
) -> error::Result<Frame> {
    let mut retry_session = sender.retry_policy().new_session();

    'next_node: loop {
        let mut transport = sender
            .load_balanced_connection()
            .await
            .ok_or_else(|| Error::from("Unable to get transport"))??;

        loop {
            match transport.write_frame(&frame).await {
                Ok(frame) => return Ok(frame),
                Err(error) => {
                    let query_info = QueryInfo {
                        error: &error,
                        is_idempotent,
                    };

                    match retry_session.decide(query_info) {
                        RetryDecision::RetrySameNode => {
                            let new_transport = sender.node_connection(transport.addr()).await;
                            match new_transport {
                                Some(new_transport) => transport = new_transport?,
                                None => continue 'next_node,
                            }
                        }
                        RetryDecision::RetryNextNode => continue 'next_node,
                        RetryDecision::DontRetry => return Err(error),
                    }
                }
            }
        }
    }
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
