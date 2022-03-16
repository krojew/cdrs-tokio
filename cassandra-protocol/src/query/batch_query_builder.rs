use crate::consistency::Consistency;
use crate::error::{Error as CError, Result as CResult};
use crate::frame::frame_batch::{BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch};
use crate::query::{PreparedQuery, QueryFlags, QueryValues};

pub type QueryBatch = BodyReqBatch;

#[derive(Debug)]
pub struct BatchQueryBuilder {
    batch_type: BatchType,
    queries: Vec<BatchQuery>,
    consistency: Consistency,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

impl Default for BatchQueryBuilder {
    fn default() -> Self {
        BatchQueryBuilder {
            batch_type: BatchType::Logged,
            queries: vec![],
            consistency: Consistency::One,
            serial_consistency: None,
            timestamp: None,
        }
    }
}

impl BatchQueryBuilder {
    pub fn new() -> BatchQueryBuilder {
        Default::default()
    }

    pub fn with_batch_type(mut self, batch_type: BatchType) -> Self {
        self.batch_type = batch_type;
        self
    }

    /// Add a query (non-prepared one)
    pub fn add_query<T: Into<String>>(mut self, query: T, values: QueryValues) -> Self {
        self.queries.push(BatchQuery {
            subject: BatchQuerySubj::QueryString(query.into()),
            values,
        });
        self
    }

    /// Add a query (prepared one)
    pub fn add_query_prepared(mut self, query: &PreparedQuery, values: QueryValues) -> Self {
        self.queries.push(BatchQuery {
            subject: BatchQuerySubj::PreparedId(query.id.clone()),
            values,
        });
        self
    }

    pub fn clear_queries(mut self) -> Self {
        self.queries = vec![];
        self
    }

    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    pub fn with_serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency = Some(serial_consistency);
        self
    }

    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> CResult<BodyReqBatch> {
        let mut flags = QueryFlags::empty();

        if self.serial_consistency.is_some() {
            flags.insert(QueryFlags::WITH_SERIAL_CONSISTENCY);
        }

        if self.timestamp.is_some() {
            flags.insert(QueryFlags::WITH_DEFAULT_TIMESTAMP);
        }

        let with_names_for_values = self.queries.iter().all(|q| q.values.has_names());

        if !with_names_for_values {
            let some_names_for_values = self.queries.iter().any(|q| q.values.has_names());

            if some_names_for_values {
                return Err(CError::General(String::from(
                    "Inconsistent query values - mixed \
                     with and without names values",
                )));
            }
        }

        if with_names_for_values {
            flags.insert(QueryFlags::WITH_NAMES_FOR_VALUES);
        }

        Ok(BodyReqBatch {
            batch_type: self.batch_type,
            queries: self.queries,
            query_flags: flags,
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
        })
    }
}
