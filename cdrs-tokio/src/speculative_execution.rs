//! Pre-emptively query another node if the current one takes too long to respond.
//!
//! Sometimes a Cassandra node might be experiencing difficulties (ex: long GC pause) and take
//! longer than usual to reply. Queries sent to that node will experience bad latency.
//!
//! One thing we can do to improve that is pre-emptively start a second execution of the query
//! against another node, before the first node has replied or errored out. If that second node
//! replies faster, we can send the response back to the client. We also cancel the first execution.
//!
//! Turning on speculative executions doesn't change the driver's retry behavior. Each parallel
//! execution will trigger retries independently.

use derive_more::Constructor;
use std::time::Duration;

/// Current speculative execution context.
#[derive(Constructor, Debug)]
pub struct Context {
    pub running_executions: usize,
}

/// The policy that decides if the driver will send speculative queries to the next nodes when the
/// current node takes too long to respond. If a query is not idempotent, the driver will never
/// schedule speculative executions for it, because there is no way to guarantee that only one node
/// will apply the mutation.
pub trait SpeculativeExecutionPolicy {
    /// Returns the time until a speculative request is sent to the next node. `None` means there
    /// should not be another execution.
    fn execution_interval(&self, context: &Context) -> Option<Duration>;
}

/// A policy that schedules a configurable number of speculative executions, separated by a fixed
/// delay.
#[derive(Debug, Clone, Copy, Constructor)]
pub struct ConstantSpeculativeExecutionPolicy {
    max_executions: usize,
    delay: Duration,
}

impl SpeculativeExecutionPolicy for ConstantSpeculativeExecutionPolicy {
    fn execution_interval(&self, context: &Context) -> Option<Duration> {
        if context.running_executions < self.max_executions {
            Some(self.delay)
        } else {
            None
        }
    }
}
