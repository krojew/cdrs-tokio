use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::query::{QueryFlags, QueryParams, QueryValues};
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::value::Value;
use cassandra_protocol::types::CBytes;

use crate::statement::StatementParams;

#[derive(Debug, Default)]
pub struct StatementParamsBuilder {
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
    tracing: bool,
    warnings: bool,
}

impl StatementParamsBuilder {
    pub fn new() -> StatementParamsBuilder {
        Default::default()
    }

    /// Sets new statement consistency
    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    // Sets new flags.
    pub fn with_flags(mut self, flags: QueryFlags) -> Self {
        self.flags = Some(flags);
        self
    }

    /// Sets new statement values.
    pub fn with_values(mut self, values: QueryValues) -> Self {
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

    /// Sets the "with names for values" flag.
    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    /// Sets new statement consistency.
    pub fn with_page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self.flags = self.flags.or(Some(QueryFlags::PAGE_SIZE));

        self
    }

    /// Sets new paging state.
    pub fn with_paging_state(mut self, state: CBytes) -> Self {
        self.paging_state = Some(state);
        self.flags = self.flags.or(Some(QueryFlags::WITH_PAGING_STATE));

        self
    }

    /// Sets new serial consistency.
    pub fn with_serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency = Some(serial_consistency);
        self
    }

    /// Sets new timestamp.
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets new keyspace.
    pub fn with_keyspace(mut self, keyspace: String) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    /// Sets new token for routing.
    pub fn with_token(mut self, token: Murmur3Token) -> Self {
        self.token = Some(token);
        self
    }

    /// Sets new explicit routing key.
    pub fn with_routing_key(mut self, routing_key: Vec<Value>) -> Self {
        self.routing_key = Some(routing_key);
        self
    }

    /// Marks the statement as idempotent or not
    pub fn idempotent(mut self, value: bool) -> Self {
        self.is_idempotent = value;
        self
    }

    pub fn build(self) -> StatementParams {
        StatementParams {
            query_params: QueryParams {
                consistency: self.consistency,
                values: self.values,
                with_names: self.with_names,
                page_size: self.page_size,
                paging_state: self.paging_state,
                serial_consistency: self.serial_consistency,
                timestamp: self.timestamp,
            },
            is_idempotent: self.is_idempotent,
            keyspace: self.keyspace,
            token: self.token,
            routing_key: self.routing_key,
            tracing: self.tracing,
            warnings: self.warnings,
        }
    }
}
