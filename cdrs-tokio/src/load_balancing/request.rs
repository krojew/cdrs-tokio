use derive_more::Constructor;

use crate::cluster::Murmur3Token;
use crate::consistency::Consistency;

/// A request executed by a `Session`.
#[derive(Constructor, Clone, Debug)]
pub struct Request<'a> {
    pub keyspace: Option<&'a str>,
    pub token: Option<Murmur3Token>,
    pub routing_key: Option<&'a [u8]>,
    pub consistency: Option<Consistency>,
}
