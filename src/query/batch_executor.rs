use async_trait::async_trait;

use crate::cluster::{GetCompressor, GetConnection, GetRetryPolicy, ResponseCache};
use crate::error;
use crate::frame::traits::AsBytes;
use crate::frame::Frame;
use crate::query::batch_query_builder::QueryBatch;
use crate::transport::CdrsTransport;

use super::utils::{prepare_flags, send_frame};

#[async_trait]
pub trait BatchExecutor<T: CdrsTransport + Unpin + 'static>:
    GetConnection<T> + GetCompressor + ResponseCache + GetRetryPolicy + Sync
{
    async fn batch_with_params_tw(
        &self,
        batch: QueryBatch,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let flags = prepare_flags(with_tracing, with_warnings);
        let is_idempotent = batch.is_idempotent;

        let query_frame = Frame::new_req_batch(batch, flags);

        send_frame(
            self,
            query_frame.as_bytes(),
            query_frame.stream,
            is_idempotent,
        )
        .await
    }

    async fn batch_with_params(&self, batch: QueryBatch) -> error::Result<Frame> {
        self.batch_with_params_tw(batch, false, false).await
    }
}
