use std::sync::RwLock;

use crate::types::CBytesShort;

#[derive(Debug)]
pub struct PreparedQuery {
    pub id: RwLock<CBytesShort>,
    pub query: String,
    pub keyspace: Option<String>,
    pub pk_indexes: Vec<i16>,
}

impl Clone for PreparedQuery {
    fn clone(&self) -> Self {
        PreparedQuery {
            id: RwLock::new(
                self.id
                    .read()
                    .expect("Cannot read prepared query id!")
                    .clone(),
            ),
            query: self.query.clone(),
            keyspace: self.keyspace.clone(),
            pk_indexes: self.pk_indexes.clone(),
        }
    }
}
