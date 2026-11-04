use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::error;
use cassandra_protocol::frame::message_result::RowsMetadataFlags;
use cassandra_protocol::query::{PreparedQuery, QueryParams, QueryParamsBuilder};
use cassandra_protocol::types::rows::Row;
use cassandra_protocol::types::CBytes;

use crate::cluster::session::Session;
use crate::cluster::ConnectionManager;
use crate::load_balancing::LoadBalancingStrategy;
use crate::statement::StatementParams;
use crate::transport::CdrsTransport;

pub struct SessionPager<
    'a,
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    page_size: i32,
    session: &'a Session<T, CM, LB>,
}

impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > SessionPager<'a, T, CM, LB>
{
    pub fn new(session: &'a Session<T, CM, LB>, page_size: i32) -> SessionPager<'a, T, CM, LB> {
        SessionPager { session, page_size }
    }

    pub fn query_with_pager_state<Q>(
        &'a mut self,
        query: Q,
        state: PagerState,
    ) -> QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
    where
        Q: ToString,
    {
        self.query_with_pager_state_params(query, state, Default::default())
    }

    pub fn query_with_pager_state_params<Q>(
        &'a mut self,
        query: Q,
        state: PagerState,
        qp: QueryParams,
    ) -> QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
    where
        Q: ToString,
    {
        QueryPager {
            pager: self,
            pager_state: state,
            query,
            query_params: qp,
        }
    }

    pub fn query<Q>(&'a mut self, query: Q) -> QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
    where
        Q: ToString,
    {
        self.query_with_params(
            query,
            QueryParamsBuilder::new()
                .with_consistency(Consistency::One)
                .build(),
        )
    }

    pub fn query_with_params<Q>(
        &'a mut self,
        query: Q,
        qp: QueryParams,
    ) -> QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
    where
        Q: ToString,
    {
        self.query_with_pager_state_params(query, PagerState::new(), qp)
    }

    pub fn exec_with_pager_state(
        &'a mut self,
        query: &'a PreparedQuery,
        state: PagerState,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        self.exec_with_pager_state_params(query, state, Default::default())
    }

    /// Pages a prepared statement executed with given parameters, starting from given state.
    /// Page size and paging state are controlled by the pager.
    pub fn exec_with_pager_state_params(
        &'a mut self,
        query: &'a PreparedQuery,
        state: PagerState,
        params: StatementParams,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        ExecPager {
            pager: self,
            pager_state: state,
            query,
            params,
        }
    }

    pub fn exec(
        &'a mut self,
        query: &'a PreparedQuery,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        self.exec_with_pager_state(query, PagerState::new())
    }

    /// Pages a prepared statement executed with given parameters. Page size and paging state are
    /// controlled by the pager.
    pub fn exec_with_params(
        &'a mut self,
        query: &'a PreparedQuery,
        params: StatementParams,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        self.exec_with_pager_state_params(query, PagerState::new(), params)
    }
}

pub struct QueryPager<'a, Q: ToString, P: 'a> {
    pager: &'a mut P,
    pager_state: PagerState,
    query: Q,
    query_params: QueryParams,
}

impl<
        'a,
        Q: ToString,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync + 'static,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
    > QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
{
    pub fn into_pager_state(self) -> PagerState {
        self.pager_state
    }

    pub async fn next(&mut self) -> error::Result<Vec<Row>> {
        let mut query_params = self.query_params.clone();
        query_params.page_size = Some(self.pager.page_size);
        query_params
            .paging_state
            .clone_from(&self.pager_state.cursor);

        if let Some(values) = &query_params.values {
            query_params.with_names = values.has_names();
        }

        let params = StatementParams {
            keyspace: query_params.keyspace.clone(),
            query_params,
            ..Default::default()
        };

        let query = self.query.to_string();

        let body = self
            .pager
            .session
            .query_with_params(query, params)
            .await
            .and_then(|envelope| envelope.response_body())?;

        let metadata = body
            .as_rows_metadata()
            .ok_or("Pager query should yield a vector of rows")?;

        self.pager_state.has_more_pages =
            Some(metadata.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES));
        self.pager_state.cursor.clone_from(&metadata.paging_state);

        body.into_rows()
            .ok_or_else(|| "Pager query should yield a vector of rows".into())
    }

    pub fn has_more(&self) -> bool {
        self.pager_state.has_more_pages.unwrap_or(false)
    }

    /// This method returns a copy of pager state so
    /// the state may be used later for continuing paging.
    pub fn pager_state(&self) -> PagerState {
        self.pager_state.clone()
    }
}

pub struct ExecPager<'a, P: 'a> {
    pager: &'a mut P,
    pager_state: PagerState,
    query: &'a PreparedQuery,
    params: StatementParams,
}

impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync + 'static,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static,
    > ExecPager<'a, SessionPager<'a, T, CM, LB>>
{
    pub fn into_pager_state(self) -> PagerState {
        self.pager_state
    }

    pub async fn next(&mut self) -> error::Result<Vec<Row>> {
        let mut params = self.params.clone();
        params.query_params.page_size = Some(self.pager.page_size);
        params
            .query_params
            .paging_state
            .clone_from(&self.pager_state.cursor);

        let body = self
            .pager
            .session
            .exec_with_params(self.query, &params)
            .await
            .and_then(|envelope| envelope.response_body())?;

        let metadata = body
            .as_rows_metadata()
            .ok_or("Pager query should yield a vector of rows")?;

        self.pager_state.has_more_pages =
            Some(metadata.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES));
        self.pager_state.cursor.clone_from(&metadata.paging_state);

        body.into_rows()
            .ok_or_else(|| "Pager query should yield a vector of rows".into())
    }

    #[inline]
    pub fn has_more(&self) -> bool {
        self.pager_state.has_more_pages.unwrap_or(false)
    }

    /// This method returns a copy of pager state so
    /// the state may be used later for continuing paging.
    #[inline]
    pub fn pager_state(&self) -> PagerState {
        self.pager_state.clone()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct PagerState {
    cursor: Option<CBytes>,
    has_more_pages: Option<bool>,
}

impl PagerState {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_with_cursor(cursor: CBytes) -> Self {
        PagerState {
            cursor: Some(cursor),
            has_more_pages: None,
        }
    }

    pub fn new_with_cursor_and_more_flag(cursor: CBytes, has_more: bool) -> Self {
        PagerState {
            cursor: Some(cursor),
            has_more_pages: Some(has_more),
        }
    }

    #[inline]
    pub fn has_more(&self) -> bool {
        self.has_more_pages.unwrap_or(false)
    }

    #[inline]
    pub fn cursor(&self) -> Option<CBytes> {
        self.cursor.clone()
    }

    #[inline]
    pub fn into_cursor(self) -> Option<CBytes> {
        self.cursor
    }
}

#[cfg(test)]
mod tests {
    use arc_swap::ArcSwapOption;
    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::frame::message_request::RequestBody;
    use cassandra_protocol::frame::message_result::{ColType, ResResultBody, RowsMetadataFlags};
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::{PreparedQuery, QueryParams, QueryParamsBuilder, QueryValues};
    use cassandra_protocol::types::value::Value;
    use cassandra_protocol::types::{CBytes, CBytesShort};
    use futures::future::ready;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};

    use crate::cluster::session::test_session::*;
    use crate::cluster::test_support::*;
    use crate::statement::StatementParamsBuilder;

    const PAGE_SIZE: i32 = 10;
    const PAGING_STATE: [u8; 2] = [7, 8];

    type ParamsLog = Arc<Mutex<Vec<QueryParams>>>;

    // Serves two pages of rows and records parameters of every request.
    fn paging_connection_manager(params: ParamsLog, version: Version) -> TestConnectionManager {
        TestConnectionManager::new(move |_, envelope| {
            let query_params = match envelope.request_body() {
                Ok(RequestBody::Query(query)) => query.query_params,
                Ok(RequestBody::Execute(execute)) => execute.query_parameters,
                other => panic!("Unexpected request: {:?}", other),
            };

            let is_first_page = query_params.paging_state.is_none();
            params.lock().unwrap().push(query_params);

            let mut body = rows_body(
                vec![("id", simple_type(ColType::Int))],
                vec![vec![CBytes::new(vec![0, 0, 0, 1])]],
                version,
            );

            if is_first_page {
                body.metadata
                    .flags
                    .insert(RowsMetadataFlags::HAS_MORE_PAGES);
                body.metadata.paging_state = Some(CBytes::new(PAGING_STATE.to_vec()));
            }

            ready(Ok(result_response(ResResultBody::Rows(body), version))).boxed()
        })
    }

    fn values() -> QueryValues {
        QueryValues::SimpleValues(vec![Value::Some(vec![0, 0, 0, 5])])
    }

    // Checks if every page was requested with expected params, apart from paging ones.
    fn assert_paged_with(params: &ParamsLog, expected: &QueryParams) {
        let params = params.lock().unwrap();
        assert_eq!(params.len(), 2);

        for (index, page_params) in params.iter().enumerate() {
            assert_eq!(page_params.page_size, Some(PAGE_SIZE));
            assert_eq!(
                page_params.paging_state,
                if index == 0 {
                    None
                } else {
                    Some(CBytes::new(PAGING_STATE.to_vec()))
                }
            );

            assert_eq!(
                &QueryParams {
                    page_size: None,
                    paging_state: None,
                    ..page_params.clone()
                },
                expected
            );
        }
    }

    #[tokio::test]
    async fn should_keep_all_query_params_across_pages() {
        let version = Version::V5;
        let params = ParamsLog::default();
        let session = create_session(
            &[node_address(1)],
            paging_connection_manager(params.clone(), version),
            TestRoundRobin::new(),
            None,
            version,
        );

        let query_params = QueryParamsBuilder::new()
            .with_consistency(Consistency::Quorum)
            .with_values(values())
            .with_serial_consistency(Consistency::LocalSerial)
            .with_timestamp(123)
            .with_keyspace("ks".into())
            .with_now_in_seconds(456)
            .build();

        let mut session_pager = session.paged(PAGE_SIZE);
        let mut pager =
            session_pager.query_with_params("SELECT * FROM table", query_params.clone());

        pager.next().await.unwrap();
        assert!(pager.has_more());
        pager.next().await.unwrap();
        assert!(!pager.has_more());

        assert_paged_with(&params, &query_params);
    }

    #[tokio::test]
    async fn should_keep_statement_params_across_pages_of_prepared_statement() {
        let version = Version::V4;
        let params = ParamsLog::default();
        let session = create_session(
            &[node_address(1)],
            paging_connection_manager(params.clone(), version),
            TestRoundRobin::new(),
            None,
            version,
        );

        let prepared = PreparedQuery {
            id: CBytesShort::new(vec![1]),
            query: "SELECT * FROM ks.table WHERE id = ?".into(),
            keyspace: None,
            pk_indexes: vec![0],
            result_metadata_id: ArcSwapOption::empty(),
        };

        let statement_params = StatementParamsBuilder::new()
            .with_consistency(Consistency::Quorum)
            .with_values(values())
            .with_serial_consistency(Consistency::LocalSerial)
            .with_timestamp(123)
            .build();

        let mut session_pager = session.paged(PAGE_SIZE);
        let mut pager = session_pager.exec_with_params(&prepared, statement_params.clone());

        pager.next().await.unwrap();
        assert!(pager.has_more());
        pager.next().await.unwrap();
        assert!(!pager.has_more());

        assert_paged_with(&params, &statement_params.query_params);
    }
}
