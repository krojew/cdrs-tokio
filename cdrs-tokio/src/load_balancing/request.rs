use derive_more::Constructor;

use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::query::query_params::Murmur3Token;

/// A request executed by a `Session`.
#[derive(Constructor, Clone, Debug)]
pub struct Request<'a> {
    pub keyspace: Option<&'a str>,
    pub token: Option<Murmur3Token>,
    pub routing_key: Option<&'a [u8]>,
    pub consistency: Option<Consistency>,
}
