use async_trait::async_trait;

use crate::cluster::{GetCompressor, GetConnection, ResponseCache};
use crate::error;
use crate::frame::{AsBytes, Frame};
use crate::query::{PrepareExecutor, PreparedQuery, QueryParams, QueryParamsBuilder, QueryValues};
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};
use std::ops::Deref;

#[async_trait]
pub trait ExecExecutor<T: CDRSTransport + Unpin + 'static>:
    GetConnection<T> + GetCompressor + PrepareExecutor<T> + ResponseCache + Sync
{
    async fn exec_with_params_tw(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let flags = prepare_flags(with_tracing, with_warnings);
        let options_frame = Frame::new_req_execute(
            prepared
                .id
                .read()
                .expect("Cannot read prepared query id!")
                .deref(),
            &query_parameters,
            flags,
        );

        let mut result = send_frame(self, options_frame.as_bytes(), options_frame.stream).await;
        if let Err(error::Error::Server(error)) = &result {
            // if query is unprepared
            if error.error_code == 0x2500 {
                if let Ok(new) = self.prepare_raw(&prepared.query).await {
                    *prepared
                        .id
                        .write()
                        .expect("Cannot write prepared query id!") = new.id.clone();
                    let flags = prepare_flags(with_tracing, with_warnings);
                    let options_frame = Frame::new_req_execute(&new.id, &query_parameters, flags);
                    result = send_frame(self, options_frame.as_bytes(), options_frame.stream).await;
                }
            }
        }
        result
    }

    async fn exec_with_params(
        &self,
        prepared: &PreparedQuery,
        query_parameters: QueryParams,
    ) -> error::Result<Frame> {
        self.exec_with_params_tw(prepared, query_parameters, false, false)
            .await
    }

    async fn exec_with_values_tw<V: Into<QueryValues> + Sync + Send>(
        &self,
        prepared: &PreparedQuery,
        values: V,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params_builder = QueryParamsBuilder::new();
        let query_params = query_params_builder.values(values.into()).finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
            .await
    }

    async fn exec_with_values<V: Into<QueryValues> + Sync + Send>(
        &self,
        prepared: &PreparedQuery,
        values: V,
    ) -> error::Result<Frame> {
        self.exec_with_values_tw(prepared, values, false, false)
            .await
    }

    async fn exec_tw(
        &self,
        prepared: &PreparedQuery,
        with_tracing: bool,
        with_warnings: bool,
    ) -> error::Result<Frame> {
        let query_params = QueryParamsBuilder::new().finalize();
        self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
            .await
    }

    async fn exec(&self, prepared: &PreparedQuery) -> error::Result<Frame>
    where
        Self: Sync,
    {
        self.exec_tw(prepared, false, false).await
    }
}
