use arc_swap::ArcSwapOption;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::types::CBytesShort;

#[derive(Debug)]
pub struct PreparedQuery {
    pub id: CBytesShort,
    pub query: String,
    pub keyspace: Option<String>,
    pub pk_indexes: Vec<i16>,
    pub result_metadata_id: ArcSwapOption<CBytesShort>,
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
