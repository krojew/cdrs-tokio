use crate::cluster::{GetCompressor, GetConnection, GetRetryPolicy, ResponseCache};
use crate::error;
use crate::error::Error;
use crate::frame::frame_response::ResponseBody;
use crate::frame::frame_result::ResultKind;
use crate::frame::parser::parse_raw_frame;
use crate::frame::{Flag, Frame, FromBytes, Opcode, StreamId};
use crate::retry::{QueryInfo, RetryDecision};
use crate::transport::CdrsTransport;
use crate::types::INT_LEN;
use std::ops::Deref;

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
    frame_bytes: Vec<u8>,
    stream_id: StreamId,
    is_idempotent: bool,
) -> error::Result<Frame>
where
    S: GetConnection<T> + GetCompressor + ResponseCache + GetRetryPolicy,
    T: CdrsTransport + Unpin + 'static,
{
    let compression = sender.compressor();
    let mut retry_session = sender.retry_policy().new_session();

    'next_node: loop {
        let transport = sender
            .connection()
            .await
            .ok_or_else(|| Error::from("Unable to get transport"))?
            .pool();

        let pool = transport
            .get()
            .await
            .map_err(|error| Error::from(error.to_string()));

        let pool = match pool {
            Ok(pool) => pool,
            Err(error) => {
                let query_info = QueryInfo {
                    error: &error,
                    is_idempotent,
                };

                if retry_session.decide(query_info) == RetryDecision::DontRetry {
                    return Err(error);
                }

                continue;
            }
        };

        'same_node: loop {
            {
                let mut lock = pool.lock().await;
                loop {
                    if let Err(error) = lock.write_all(&frame_bytes).await {
                        let error = Error::from(error);
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

                    // TLS connections may not write data to the stream immediately,
                    // but may wait for more data. Ensure the data is sent.
                    // Otherwise Cassandra server will not send out a response.
                    if let Err(error) = lock.flush().await {
                        let error = Error::from(error);
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

                    break;
                }
            }

            loop {
                let frame = parse_raw_frame(pool.deref(), compression).await?;
                if let Some(frame) = sender.match_or_cache_response(stream_id, frame).await {
                    // in case we get a SetKeyspace result, we need to store current keyspace
                    // checks are done manually for speed
                    match frame.opcode {
                        Opcode::Result => {
                            let result_kind = ResultKind::from_bytes(&frame.body[..INT_LEN])?;
                            if result_kind == ResultKind::SetKeyspace {
                                let response_body = frame.body()?;
                                let set_keyspace = response_body
                                    .into_set_keyspace()
                                    .expect("SetKeyspace not found with SetKeyspace opcode!");

                                let transport = pool.lock().await;
                                transport
                                    .set_current_keyspace(set_keyspace.body.as_str())
                                    .await;
                            }
                        }
                        Opcode::Error => {
                            let error = convert_frame_into_error(frame);
                            let query_info = QueryInfo {
                                error: &error,
                                is_idempotent,
                            };

                            match retry_session.decide(query_info) {
                                RetryDecision::RetrySameNode => continue 'same_node,
                                RetryDecision::RetryNextNode => continue 'next_node,
                                RetryDecision::DontRetry => return Err(error),
                            }
                        }
                        _ => {}
                    }

                    return Ok(frame);
                }
            }
        }
    }
}

fn convert_frame_into_error(frame: Frame) -> Error {
    frame
        .body()
        .and_then(|err| -> Result<(), Error> {
            match err {
                ResponseBody::Error(err) => Err(Error::Server(err)),
                _ => unreachable!(),
            }
        })
        .unwrap_err()
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
