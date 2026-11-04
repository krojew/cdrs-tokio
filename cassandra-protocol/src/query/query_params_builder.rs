use super::{QueryFlags, QueryParams, QueryValues};
use crate::consistency::Consistency;
use crate::types::{CBytes, CInt, CLong};

#[derive(Debug, Default)]
pub struct QueryParamsBuilder {
    consistency: Consistency,
    values: Option<QueryValues>,
    page_size: Option<CInt>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<CLong>,
    keyspace: Option<String>,
    now_in_seconds: Option<CInt>,
}

impl QueryParamsBuilder {
    /// Factory function that returns new `QueryBuilder`.
    pub fn new() -> QueryParamsBuilder {
        Default::default()
    }

    /// Sets new query consistency
    #[must_use]
    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Has no effect - query flags are derived from the other parameters.
    #[deprecated(note = "has no effect - query flags are derived from the other parameters")]
    #[must_use]
    pub fn with_flags(self, _flags: QueryFlags) -> Self {
        self
    }

    /// Sets new query values.
    #[must_use]
    pub fn with_values(mut self, values: QueryValues) -> Self {
        self.values = Some(values);
        self
    }

    /// Has no effect - the "with names for values" flag is derived from the values.
    #[deprecated(note = "has no effect - the flag is derived from the values")]
    #[must_use]
    pub fn with_names(self, _with_names: bool) -> Self {
        self
    }

    /// Sets new query consistency.
    #[must_use]
    pub fn with_page_size(mut self, size: CInt) -> Self {
        self.page_size = Some(size);

        self
    }

    /// Sets new query consistency.
    #[must_use]
    pub fn with_paging_state(mut self, state: CBytes) -> Self {
        self.paging_state = Some(state);

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
    pub fn with_timestamp(mut self, timestamp: CLong) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Overrides used keyspace.
    #[must_use]
    pub fn with_keyspace(mut self, keyspace: String) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    /// Sets "now" in seconds.
    #[must_use]
    pub fn with_now_in_seconds(mut self, now_in_seconds: CInt) -> Self {
        self.now_in_seconds = Some(now_in_seconds);
        self
    }

    /// Finalizes query building process and returns query itself
    #[must_use]
    pub fn build(self) -> QueryParams {
        let with_names = self.values.as_ref().is_some_and(QueryValues::has_names);

        #[allow(deprecated)]
        QueryParams {
            consistency: self.consistency,
            values: self.values,
            with_names,
            page_size: self.page_size,
            paging_state: self.paging_state,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
            keyspace: self.keyspace,
            now_in_seconds: self.now_in_seconds,
        }
    }
}
