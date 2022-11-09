use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::error;
use cassandra_protocol::frame::message_result::RowsMetadataFlags;
use cassandra_protocol::query::{PreparedQuery, QueryParams, QueryParamsBuilder, QueryValues};
use cassandra_protocol::types::rows::Row;
use cassandra_protocol::types::CBytes;

use crate::cluster::session::Session;
use crate::cluster::ConnectionManager;
use crate::load_balancing::LoadBalancingStrategy;
use crate::statement::StatementParamsBuilder;
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
            qv: qp.values,
            consistency: qp.consistency,
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

    #[deprecated(note = "Please use query_with_params() instead.")]
    pub fn query_with_param<Q>(
        &'a mut self,
        query: Q,
        qp: QueryParams,
    ) -> QueryPager<'a, Q, SessionPager<'a, T, CM, LB>>
    where
        Q: ToString,
    {
        self.query_with_params(query, qp)
    }

    pub fn exec_with_pager_state(
        &'a mut self,
        query: &'a PreparedQuery,
        state: PagerState,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        ExecPager {
            pager: self,
            pager_state: state,
            query,
        }
    }

    pub fn exec(
        &'a mut self,
        query: &'a PreparedQuery,
    ) -> ExecPager<'a, SessionPager<'a, T, CM, LB>> {
        self.exec_with_pager_state(query, PagerState::new())
    }
}

pub struct QueryPager<'a, Q: ToString, P: 'a> {
    pager: &'a mut P,
    pager_state: PagerState,
    query: Q,
    qv: Option<QueryValues>,
    consistency: Consistency,
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
        let mut params = StatementParamsBuilder::new()
            .with_consistency(self.consistency)
            .with_page_size(self.pager.page_size);

        if let Some(qv) = &self.qv {
            params = params.with_values(qv.clone());
        }
        if let Some(cursor) = &self.pager_state.cursor {
            params = params.with_paging_state(cursor.clone());
        }
        let query = self.query.to_string();

        let body = self
            .pager
            .session
            .query_with_params(query, params.build())
            .await
            .and_then(|envelope| envelope.response_body())?;

        let metadata = body
            .as_rows_metadata()
            .ok_or("Pager query should yield a vector of rows")?;

        self.pager_state.has_more_pages =
            Some(metadata.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES));
        self.pager_state.cursor = metadata.paging_state.clone();

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
        let mut params = StatementParamsBuilder::new().with_page_size(self.pager.page_size);
        if let Some(cursor) = &self.pager_state.cursor {
            params = params.with_paging_state(cursor.clone());
        }

        let body = self
            .pager
            .session
            .exec_with_params(self.query, &params.build())
            .await
            .and_then(|envelope| envelope.response_body())?;

        let metadata = body
            .as_rows_metadata()
            .ok_or("Pager query should yield a vector of rows")?;

        self.pager_state.has_more_pages =
            Some(metadata.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES));
        self.pager_state.cursor = metadata.paging_state.clone();

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

    #[deprecated(note = "Use new_with_cursor().")]
    pub fn with_cursor(cursor: CBytes) -> Self {
        Self::new_with_cursor(cursor)
    }

    #[deprecated(note = "Use new_with_cursor_and_more_flag().")]
    pub fn with_cursor_and_more_flag(cursor: CBytes, has_more: bool) -> Self {
        Self::new_with_cursor_and_more_flag(cursor, has_more)
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
