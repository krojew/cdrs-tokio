use crate::cluster::{GetConnection, GetRetryPolicy};
use crate::error;
use crate::error::Error;
use crate::frame::{Flag, Frame};
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

pub async fn send_frame<S: ?Sized, T>(
    sender: &S,
    frame: Frame,
    is_idempotent: bool,
) -> error::Result<Frame>
where
    S: GetConnection<T> + GetRetryPolicy,
    T: CdrsTransport + 'static,
{
    let mut retry_session = sender.retry_policy().new_session();

    'next_node: loop {
        let mut transport = sender
            .load_balanced_connection()
            .await
            .ok_or_else(|| Error::from("Unable to get transport"))??;

        loop {
            match transport.write_frame(frame.clone()).await {
                Ok(frame) => return Ok(frame),
                Err(error) => {
                    let query_info = QueryInfo {
                        error: &error,
                        is_idempotent,
                    };

                    match retry_session.decide(query_info) {
                        RetryDecision::RetrySameNode => {
                            transport = sender
                                .node_connection(transport.addr())
                                .await
                                .ok_or_else(|| Error::from("Unable to get transport"))??;
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
