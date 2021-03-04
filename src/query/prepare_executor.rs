use std::sync::RwLock;

use async_trait::async_trait;

use crate::cluster::{GetCompressor, GetConnection, ResponseCache};
use crate::error;
use crate::frame::frame_result::BodyResResultPrepared;
use crate::frame::{AsBytes, Frame};
use crate::query::PreparedQuery;
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};

#[async_trait]
pub trait PrepareExecutor<
    T: CDRSTransport + Unpin + 'static,
>: GetConnection<T> + GetCompressor + ResponseCache + Sync
{
    /// It prepares a query for execution, along with query itself the
    /// method takes `with_tracing` and `with_warnings` flags to get
    /// tracing information and warnings. Return the raw prepared
    /// query result.
    async fn prepare_raw_tw<Q: ToString + Sync + Send>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<BodyResResultPrepared> {
        let flags = prepare_flags(with_tracing, with_warnings);

        let query_frame = Frame::new_req_prepare(query.to_string(), flags);

        send_frame(self, query_frame.as_bytes(), query_frame.stream)
            .await
            .and_then(|response| response.get_body())
            .map(|body| {
                body.into_prepared()
                    .expect("CDRS BUG: cannot convert frame into prepared")
            })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Return the raw prepared query result.
    async fn prepare_raw<Q: ToString + Sync + Send>(
        &self,
        query: Q,
    ) -> error::Result<BodyResResultPrepared> {
        self.prepare_raw_tw(query, false, false).await
    }

    /// It prepares a query for execution, along with query itself
    /// the method takes `with_tracing` and `with_warnings` flags
    /// to get tracing information and warnings. Return the prepared
    /// query ID.
    async fn prepare_tw<Q: ToString + Sync + Send>(
        &self,
        query: Q,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<PreparedQuery> {
        let s = query.to_string();
        self.prepare_raw_tw(query, with_tracing, with_warnings)
            .await
            .map(|x| PreparedQuery {
                id: RwLock::new(x.id),
                query: s,
            })
    }

    /// It prepares query without additional tracing information and warnings.
    /// Return the prepared query ID.
    async fn prepare<Q: ToString + Sync + Send>(&self, query: Q) -> error::Result<PreparedQuery>
    where
        Self: Sync,
    {
        self.prepare_tw(query, false, false).await
    }
}
