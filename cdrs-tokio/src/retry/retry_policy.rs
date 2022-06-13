use derive_more::Display;

use cassandra_protocol::error::Error;
use cassandra_protocol::frame::message_error::{
    AdditionalErrorInfo, ErrorBody, ReadTimeoutError, WriteTimeoutError, WriteType,
};

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Copy, Clone, Display)]
pub enum RetryDecision {
    RetrySameNode,
    RetryNextNode,
    DontRetry,
}

/// Information about a failed query.
pub struct QueryInfo<'a> {
    pub error: &'a Error,
    pub is_idempotent: bool,
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
#[derive(Default)]
pub struct FallthroughRetryPolicy;

impl RetryPolicy for FallthroughRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession + Send + Sync> {
        Box::new(FallthroughRetrySession::default())
    }
}

#[derive(Default)]
pub struct FallthroughRetrySession;

impl RetrySession for FallthroughRetrySession {
    fn decide(&mut self, _query_info: QueryInfo) -> RetryDecision {
        RetryDecision::DontRetry
    }
}

/// Default retry policy - retries when there is a high chance that a retry might help.  
/// Behaviour based on [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/retries/)
#[derive(Default)]
pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession + Send + Sync> {
        Box::new(DefaultRetrySession::default())
    }
}

#[derive(Default)]
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
            | Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::Overloaded,
                ..
            })
            | Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::Server,
                ..
            })
            | Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::Truncate,
                ..
            }) => {
                if query_info.is_idempotent {
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::Unavailable(_),
                ..
            }) => {
                if !self.was_unavailable_retry {
                    self.was_unavailable_retry = true;
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::ReadTimeout(error @ ReadTimeoutError { .. }),
                ..
            }) => {
                if !self.was_read_timeout_retry
                    && error.received >= error.block_for
                    && error.replica_has_responded()
                {
                    self.was_read_timeout_retry = true;
                    RetryDecision::RetrySameNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::WriteTimeout(error @ WriteTimeoutError { .. }),
                ..
            }) => {
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
            Error::Server(ErrorBody {
                additional_info: AdditionalErrorInfo::IsBootstrapping,
                ..
            }) => RetryDecision::RetryNextNode,
            _ => RetryDecision::DontRetry,
        }
    }
}
