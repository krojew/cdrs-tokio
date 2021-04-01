use crate::cluster::{GetCompressor, GetConnection, ResponseCache};
use crate::error;
use crate::frame::frame_result::ResultKind;
use crate::frame::parser::from_connection;
use crate::frame::{Flag, Frame, FromBytes, Opcode, StreamId};
use crate::transport::CdrsTransport;
use crate::types::INT_LEN;

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
) -> error::Result<Frame>
where
    S: GetConnection<T> + GetCompressor + ResponseCache,
    T: CdrsTransport + Unpin + 'static,
{
    let compression = sender.compressor();

    let transport = sender
        .connection()
        .await
        .ok_or_else(|| error::Error::from("Unable to get transport"))?
        .pool();

    let pool = transport
        .get()
        .await
        .map_err(|error| error::Error::from(error.to_string()))?;

    pool.lock()
        .await
        .write_all(frame_bytes.as_slice())
        .await
        .map_err(error::Error::from)?;

    loop {
        let frame = from_connection(&pool, compression).await?;
        if let Some(frame) = sender.match_or_cache_response(stream_id, frame).await {
            // in case we get a SetKeyspace result, we need to store current keyspace
            // checks are done manually for speed
            if frame.opcode == Opcode::Result {
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

            return Ok(frame);
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
