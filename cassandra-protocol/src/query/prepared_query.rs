use arc_swap::ArcSwapOption;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::types::CBytesShort;

/// A statement prepared on the server. Created with [`PreparedQuery::new`].
#[derive(Debug)]
#[non_exhaustive]
pub struct PreparedQuery {
    pub id: CBytesShort,
    pub query: String,
    pub keyspace: Option<String>,
    pub pk_indexes: Vec<i16>,
    result_metadata_id: ArcSwapOption<CBytesShort>,
}

impl PreparedQuery {
    /// Creates a prepared statement from the server-assigned `id`, the statement text, its
    /// keyspace, partition key bind marker indexes and result metadata id. Fields added to the
    /// struct later are initialized to their defaults.
    pub fn new(
        id: CBytesShort,
        query: impl Into<String>,
        keyspace: Option<String>,
        pk_indexes: Vec<i16>,
        result_metadata_id: Option<CBytesShort>,
    ) -> Self {
        PreparedQuery {
            id,
            query: query.into(),
            keyspace,
            pk_indexes,
            result_metadata_id: ArcSwapOption::new(result_metadata_id.map(Arc::new)),
        }
    }

    /// Returns the id of the result metadata the server associates with this statement. Present
    /// since protocol v5.
    pub fn result_metadata_id(&self) -> Option<Arc<CBytesShort>> {
        self.result_metadata_id.load_full()
    }

    /// Replaces the result metadata id, as required when a protocol v5 response announces changed
    /// result metadata. Takes `&self`, so a statement shared between tasks can be updated in
    /// place.
    pub fn set_result_metadata_id(&self, result_metadata_id: Option<CBytesShort>) {
        self.result_metadata_id
            .store(result_metadata_id.map(Arc::new));
    }
}

impl Clone for PreparedQuery {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            query: self.query.clone(),
            keyspace: self.keyspace.clone(),
            pk_indexes: self.pk_indexes.clone(),
            result_metadata_id: ArcSwapOption::new(self.result_metadata_id.load().clone()),
        }
    }
}

impl PartialEq for PreparedQuery {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && *self.result_metadata_id.load() == *other.result_metadata_id.load()
    }
}

impl Eq for PreparedQuery {}

impl PartialOrd for PreparedQuery {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PreparedQuery {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match self.id.cmp(&other.id) {
            Ordering::Equal => self
                .result_metadata_id
                .load()
                .cmp(&other.result_metadata_id.load()),
            result => result,
        }
    }
}

impl Hash for PreparedQuery {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.result_metadata_id.load().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_prepared_query() {
        let query = PreparedQuery::new(
            CBytesShort::new(vec![1]),
            "SELECT * FROM ks.table WHERE id = ?",
            Some("ks".into()),
            vec![0],
            Some(CBytesShort::new(vec![2])),
        );

        assert_eq!(query.id, CBytesShort::new(vec![1]));
        assert_eq!(query.query, "SELECT * FROM ks.table WHERE id = ?");
        assert_eq!(query.keyspace.as_deref(), Some("ks"));
        assert_eq!(query.pk_indexes, vec![0]);
        assert_eq!(
            query.result_metadata_id().as_deref(),
            Some(&CBytesShort::new(vec![2]))
        );
    }

    #[test]
    fn should_replace_result_metadata_id() {
        let query = PreparedQuery::new(
            CBytesShort::new(vec![1]),
            "SELECT * FROM ks.table",
            None,
            vec![],
            Some(CBytesShort::new(vec![2])),
        );
        let clone = query.clone();

        query.set_result_metadata_id(Some(CBytesShort::new(vec![3])));
        assert_eq!(
            query.result_metadata_id().as_deref(),
            Some(&CBytesShort::new(vec![3]))
        );
        assert_eq!(
            clone.result_metadata_id().as_deref(),
            Some(&CBytesShort::new(vec![2]))
        );
        assert_ne!(query, clone);

        query.set_result_metadata_id(None);
        assert!(query.result_metadata_id().is_none());
    }
}
