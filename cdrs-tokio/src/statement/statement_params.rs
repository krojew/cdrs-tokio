use crate::retry::RetryPolicy;
use cassandra_protocol::query::QueryParams;
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::value::Value;
use derivative::Derivative;
use std::sync::Arc;

use crate::speculative_execution::SpeculativeExecutionPolicy;

/// Parameters of Query for query operation.
#[derive(Default, Clone, Derivative)]
#[derivative(Debug)]
pub struct StatementParams {
    /// Protocol-level parameters.
    pub query_params: QueryParams,
    /// Is the query idempotent.
    pub is_idempotent: bool,
    /// Query keyspace. If not using a global one, setting it explicitly might help the load
    /// balancer use more appropriate nodes. Note: prepared statements with keyspace information
    /// take precedence over this field.
    pub keyspace: Option<String>,
    /// The token to use for token-aware routing. A load balancer may use this information to
    /// determine which nodes to contact. Takes precedence over `routing_key`.
    pub token: Option<Murmur3Token>,
    /// The partition key to use for token-aware routing. A load balancer may use this information
    /// to determine which nodes to contact. Alternative to `token`. Note: prepared statements
    /// with bound primary key values take precedence over this field.
    pub routing_key: Option<Vec<Value>>,
    /// Should tracing be enabled.
    pub tracing: bool,
    /// Should warnings be enabled.
    pub warnings: bool,
    /// Custom statement speculative execution policy.
    #[derivative(Debug = "ignore")]
    pub speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy + Send + Sync>>,
    /// Custom statement retry policy.
    #[derivative(Debug = "ignore")]
    pub retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
    /// Enable beta protocol features. Server will respond with ERROR if protocol version is marked
    /// as beta on server and client does not provide this flag.
    pub beta_protocol: bool,
}
