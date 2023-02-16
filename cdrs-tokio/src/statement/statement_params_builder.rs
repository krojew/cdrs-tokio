use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::query::{QueryFlags, QueryParams, QueryValues};
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::value::Value;
use cassandra_protocol::types::{CBytes, CInt, CLong};
use derivative::Derivative;
use std::sync::Arc;

use crate::retry::RetryPolicy;
use crate::speculative_execution::SpeculativeExecutionPolicy;
use crate::statement::StatementParams;

#[derive(Default, Derivative)]
#[derivative(Debug)]
pub struct StatementParamsBuilder {
    consistency: Consistency,
    flags: Option<QueryFlags>,
    values: Option<QueryValues>,
    with_names: bool,
    page_size: Option<CInt>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<CLong>,
    is_idempotent: bool,
    keyspace: Option<String>,
    now_in_seconds: Option<CInt>,
    token: Option<Murmur3Token>,
    routing_key: Option<Vec<Value>>,
    tracing: bool,
    warnings: bool,
    #[derivative(Debug = "ignore")]
    speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy + Send + Sync>>,
    #[derivative(Debug = "ignore")]
    retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
    beta_protocol: bool,
}

impl StatementParamsBuilder {
    pub fn new() -> StatementParamsBuilder {
        Default::default()
    }

    /// Sets new statement consistency
    #[must_use]
    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    // Sets new flags.
    #[must_use]
    pub fn with_flags(mut self, flags: QueryFlags) -> Self {
        self.flags = Some(flags);
        self
    }

    /// Sets new statement values.
    #[must_use]
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
    #[must_use]
    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    /// Sets new statement consistency.
    #[must_use]
    pub fn with_page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self.flags = self.flags.or(Some(QueryFlags::PAGE_SIZE));

        self
    }

    /// Sets new paging state.
    #[must_use]
    pub fn with_paging_state(mut self, state: CBytes) -> Self {
        self.paging_state = Some(state);
        self.flags = self.flags.or(Some(QueryFlags::WITH_PAGING_STATE));

        self
    }

    /// Sets new serial consistency.
    #[must_use]
    pub fn with_serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency = Some(serial_consistency);
        self
    }

    /// Sets new timestamp.
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets new keyspace.
    #[must_use]
    pub fn with_keyspace(mut self, keyspace: String) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    /// Sets new token for routing.
    #[must_use]
    pub fn with_token(mut self, token: Murmur3Token) -> Self {
        self.token = Some(token);
        self
    }

    /// Sets new explicit routing key.
    #[must_use]
    pub fn with_routing_key(mut self, routing_key: Vec<Value>) -> Self {
        self.routing_key = Some(routing_key);
        self
    }

    /// Marks the statement as idempotent or not
    #[must_use]
    pub fn idempotent(mut self, value: bool) -> Self {
        self.is_idempotent = value;
        self
    }

    /// Sets custom statement speculative execution policy.
    #[must_use]
    pub fn with_speculative_execution_policy(
        mut self,
        speculative_execution_policy: Arc<dyn SpeculativeExecutionPolicy + Send + Sync>,
    ) -> Self {
        self.speculative_execution_policy = Some(speculative_execution_policy);
        self
    }

    /// Sets custom statement retry policy.
    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy + Send + Sync>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Sets beta protocol usage flag
    #[must_use]
    pub fn with_beta_protocol(mut self, beta_protocol: bool) -> Self {
        self.beta_protocol = beta_protocol;
        self
    }

    /// Sets "now" in seconds.
    #[must_use]
    pub fn with_now_in_seconds(mut self, now_in_seconds: CInt) -> Self {
        self.now_in_seconds = Some(now_in_seconds);
        self
    }

    #[must_use]
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
                keyspace: self.keyspace.clone(),
                now_in_seconds: self.now_in_seconds,
            },
            is_idempotent: self.is_idempotent,
            keyspace: self.keyspace,
            token: self.token,
            routing_key: self.routing_key,
            tracing: self.tracing,
            warnings: self.warnings,
            speculative_execution_policy: self.speculative_execution_policy,
            retry_policy: self.retry_policy,
            beta_protocol: self.beta_protocol,
        }
    }
}
