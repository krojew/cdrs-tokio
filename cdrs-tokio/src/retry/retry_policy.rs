use derive_more::Display;

use cassandra_protocol::error::Error;
use cassandra_protocol::frame::message_error::{
    ErrorBody, ErrorType, ReadTimeoutError, WriteTimeoutError, WriteType,
};

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Copy, Clone, Display)]
pub enum RetryDecision {
    RetrySameNode,
    RetryNextNode,
    DontRetry,
}

/// Information about a failed query.
///
/// New fields may be added in future releases. To build it, e.g. for testing a custom
/// [`RetrySession`], use [`QueryInfo::new`].
#[non_exhaustive]
pub struct QueryInfo<'a> {
    pub error: &'a Error,
    pub is_idempotent: bool,
}

impl<'a> QueryInfo<'a> {
    /// Creates information about a failed query. Fields added in future releases get their
    /// default values.
    pub fn new(error: &'a Error, is_idempotent: bool) -> Self {
        QueryInfo {
            error,
            is_idempotent,
        }
    }
}

/// Query-specific information about current state of retrying.
pub trait RetrySession {
    /// Decide what to do with the failing query.
    fn decide(&mut self, query_info: QueryInfo) -> RetryDecision;
}

/// Retry policy determines what to do in case of communication error.
pub trait RetryPolicy {
    /// Called for each new query, starts a session of deciding about retries.
    fn new_session(&self) -> Box<dyn RetrySession + Send + Sync>;
}

/// Forwards all errors directly to the user, never retries
#[derive(Default, Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct FallthroughRetryPolicy;

impl RetryPolicy for FallthroughRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession + Send + Sync> {
        Box::<FallthroughRetrySession>::default()
    }
}

#[derive(Default, Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct FallthroughRetrySession;

impl RetrySession for FallthroughRetrySession {
    fn decide(&mut self, _query_info: QueryInfo) -> RetryDecision {
        RetryDecision::DontRetry
    }
}

/// Default retry policy - retries when there is a high chance that a retry might help.  
/// Behaviour based on [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/retries/)
#[derive(Default, Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession + Send + Sync> {
        Box::<DefaultRetrySession>::default()
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultRetrySession {
    was_unavailable_retry: bool,
    was_read_timeout_retry: bool,
    was_write_timeout_retry: bool,
}

impl RetrySession for DefaultRetrySession {
    fn decide(&mut self, query_info: QueryInfo) -> RetryDecision {
        match query_info.error {
            Error::Io(_)
            | Error::General(_)
            | Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Overloaded,
                        ..
                    },
                ..
            }
            | Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Server,
                        ..
                    },
                ..
            }
            | Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Truncate,
                        ..
                    },
                ..
            } => {
                if query_info.is_idempotent {
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::Unavailable(_),
                        ..
                    },
                ..
            } => {
                if !self.was_unavailable_retry {
                    self.was_unavailable_retry = true;
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::ReadTimeout(error @ ReadTimeoutError { .. }),
                        ..
                    },
                ..
            } => {
                if !self.was_read_timeout_retry
                    && error.received >= error.block_for
                    && !error.replica_has_responded()
                {
                    self.was_read_timeout_retry = true;
                    RetryDecision::RetrySameNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::WriteTimeout(error @ WriteTimeoutError { .. }),
                        ..
                    },
                ..
            } => {
                if !self.was_write_timeout_retry
                    && query_info.is_idempotent
                    && error.write_type == WriteType::BatchLog
                {
                    self.was_write_timeout_retry = true;
                    RetryDecision::RetrySameNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server {
                body:
                    ErrorBody {
                        ty: ErrorType::IsBootstrapping,
                        ..
                    },
                ..
            } => RetryDecision::RetryNextNode,
            _ => RetryDecision::DontRetry,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::error::Error;
    use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType, ReadTimeoutError};

    use super::*;

    fn read_timeout_error(received: i32, block_for: i32, data_present: bool) -> Error {
        let error = ReadTimeoutError::new(Consistency::Quorum, received, block_for, data_present);

        Error::Server {
            body: ErrorBody::new("read timeout", ErrorType::ReadTimeout(error)),
            addr: SocketAddr::from(([127, 0, 0, 1], 9042)),
        }
    }

    fn decide_once(error: &Error) -> RetryDecision {
        DefaultRetrySession::default().decide(QueryInfo::new(error, true))
    }

    #[test]
    fn read_timeout_should_retry_same_node_when_enough_replicas_responded_without_data() {
        assert_eq!(
            decide_once(&read_timeout_error(2, 2, false)),
            RetryDecision::RetrySameNode
        );
        assert_eq!(
            decide_once(&read_timeout_error(3, 2, false)),
            RetryDecision::RetrySameNode
        );
    }

    #[test]
    fn read_timeout_should_not_retry_when_data_was_retrieved() {
        assert_eq!(
            decide_once(&read_timeout_error(2, 2, true)),
            RetryDecision::DontRetry
        );
    }

    #[test]
    fn read_timeout_should_not_retry_when_not_enough_replicas_responded() {
        assert_eq!(
            decide_once(&read_timeout_error(1, 2, false)),
            RetryDecision::DontRetry
        );
        assert_eq!(
            decide_once(&read_timeout_error(1, 2, true)),
            RetryDecision::DontRetry
        );
    }

    #[test]
    fn read_timeout_should_retry_only_once() {
        let error = read_timeout_error(2, 2, false);
        let mut session = DefaultRetrySession::default();

        let query_info = || QueryInfo::new(&error, true);

        assert_eq!(session.decide(query_info()), RetryDecision::RetrySameNode);
        assert_eq!(session.decide(query_info()), RetryDecision::DontRetry);
    }
}
