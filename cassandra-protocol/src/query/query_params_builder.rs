use super::{QueryFlags, QueryParams, QueryValues};
use crate::consistency::Consistency;
use crate::query::query_params::Murmur3Token;
use crate::types::value::Value;
use crate::types::CBytes;

#[derive(Debug, Default)]
pub struct QueryParamsBuilder {
    consistency: Consistency,
    flags: Option<QueryFlags>,
    values: Option<QueryValues>,
    with_names: bool,
    page_size: Option<i32>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
    is_idempotent: bool,
    keyspace: Option<String>,
    token: Option<Murmur3Token>,
    routing_key: Option<Vec<Value>>,
}

impl QueryParamsBuilder {
    /// Factory function that returns new `QueryBuilder`.
    pub fn new() -> QueryParamsBuilder {
        Default::default()
    }

    /// Sets new query consistency
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    // Sets new flags.
    builder_opt_field!(flags, QueryFlags);

    /// Sets new query values
    pub fn values(mut self, values: QueryValues) -> Self {
        self.with_names = values.has_names();
        self.values = Some(values);
        self.flags = self.flags.or_else(|| {
            let mut flags = QueryFlags::VALUE;
            if self.with_names {
                flags.insert(QueryFlags::WITH_NAMES_FOR_VALUES);
            }
            Some(flags)
        });

        self
    }

    /// Sets the "with names for values" flag
    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    /// Sets new query consistency
    pub fn page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self.flags = self.flags.or(Some(QueryFlags::PAGE_SIZE));

        self
    }

    /// Sets new query consistency
    pub fn paging_state(mut self, state: CBytes) -> Self {
        self.paging_state = Some(state);
        self.flags = self.flags.or(Some(QueryFlags::WITH_PAGING_STATE));

        self
    }

    builder_opt_field!(serial_consistency, Consistency);
    builder_opt_field!(timestamp, i64);
    builder_opt_field!(keyspace, String);
    builder_opt_field!(token, Murmur3Token);
    builder_opt_field!(routing_key, Vec<Value>);

    /// Marks the query as idempotent or not
    pub fn idempotent(mut self, value: bool) -> Self {
        self.is_idempotent = value;
        self
    }

    /// Finalizes query building process and returns query itself
    pub fn finalize(self) -> QueryParams {
        QueryParams {
            consistency: self.consistency,
            values: self.values,
            with_names: self.with_names,
            page_size: self.page_size,
            paging_state: self.paging_state,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
            is_idempotent: self.is_idempotent,
            keyspace: self.keyspace,
            token: self.token,
            routing_key: self.routing_key,
        }
    }
}
