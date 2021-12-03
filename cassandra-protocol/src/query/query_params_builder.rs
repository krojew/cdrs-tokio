use super::{QueryFlags, QueryParams, QueryValues};
use crate::consistency::Consistency;
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
}

impl QueryParamsBuilder {
    /// Factory function that returns new `QueryBuilder`.
    pub fn new() -> QueryParamsBuilder {
        Default::default()
    }

    /// Sets new query consistency
    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    // Sets new flags.
    pub fn with_flags(mut self, flags: QueryFlags) -> Self {
        self.flags = Some(flags);
        self
    }

    /// Sets new query values.
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

    /// Sets the "with names for values" flag
    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    /// Sets new query consistency.
    pub fn with_page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self.flags = self.flags.or(Some(QueryFlags::PAGE_SIZE));

        self
    }

    /// Sets new query consistency.
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

    /// Finalizes query building process and returns query itself
    pub fn build(self) -> QueryParams {
        QueryParams {
            consistency: self.consistency,
            values: self.values,
            with_names: self.with_names,
            page_size: self.page_size,
            paging_state: self.paging_state,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
        }
    }
}
