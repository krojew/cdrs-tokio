use crate::types::CBytesShort;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PreparedQuery {
    pub id: CBytesShort,
    pub query: String,
    pub keyspace: Option<String>,
    pub pk_indexes: Vec<i16>,
}
