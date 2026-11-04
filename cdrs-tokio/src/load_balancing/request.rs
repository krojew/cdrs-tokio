use cassandra_protocol::consistency::Consistency;

use crate::cluster::Murmur3Token;

/// A request executed by a `Session`.
///
/// New fields may be added in future releases. To build a request, e.g. for testing a custom
/// [`LoadBalancingStrategy`](crate::load_balancing::LoadBalancingStrategy), start from
/// [`Request::default`] and assign the fields of interest.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Request<'a> {
    pub keyspace: Option<&'a str>,
    pub token: Option<Murmur3Token>,
    pub routing_key: Option<&'a [u8]>,
    pub consistency: Option<Consistency>,
}
