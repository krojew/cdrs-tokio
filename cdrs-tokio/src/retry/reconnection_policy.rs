use std::time::Duration;

const DEFAULT_BASE_DELAY: Duration = Duration::from_secs(1);

/// Determines the time for the next reconnection attempt when trying to reconnect to a node.
pub trait ReconnectionSchedule {
    /// Returns next reconnect delay or `None` if not attempt should be made.
    fn next_delay(&mut self) -> Option<Duration>;
}

/// Creates reconnection schedules when trying to re-establish connections.
pub trait ReconnectionPolicy {
    /// Creates new schedule when a connection needs to be re-established.
    fn new_node_schedule(&self) -> Box<dyn ReconnectionSchedule + Send + Sync>;
}

/// Schedules reconnection at constant interval.
#[derive(Copy, Clone)]
pub struct ConstantReconnectionPolicy {
    base_delay: Duration,
}

impl Default for ConstantReconnectionPolicy {
    fn default() -> Self {
        ConstantReconnectionPolicy::new(DEFAULT_BASE_DELAY)
    }
}

impl ReconnectionPolicy for ConstantReconnectionPolicy {
    fn new_node_schedule(&self) -> Box<dyn ReconnectionSchedule + Send + Sync> {
        Box::new(ConstantReconnectionSchedule::new(self.base_delay))
    }
}

impl ConstantReconnectionPolicy {
    pub fn new(base_delay: Duration) -> Self {
        ConstantReconnectionPolicy { base_delay }
    }
}

struct ConstantReconnectionSchedule {
    base_delay: Duration,
}

impl ReconnectionSchedule for ConstantReconnectionSchedule {
    fn next_delay(&mut self) -> Option<Duration> {
        Some(self.base_delay)
    }
}

impl ConstantReconnectionSchedule {
    pub fn new(base_delay: Duration) -> Self {
        ConstantReconnectionSchedule { base_delay }
    }
}

/// Never schedules reconnections.
#[derive(Default, Copy, Clone)]
pub struct NeverReconnectionPolicy;

impl ReconnectionPolicy for NeverReconnectionPolicy {
    fn new_node_schedule(&self) -> Box<dyn ReconnectionSchedule + Send + Sync> {
        Box::new(NeverReconnectionSchedule)
    }
}

struct NeverReconnectionSchedule;

impl ReconnectionSchedule for NeverReconnectionSchedule {
    fn next_delay(&mut self) -> Option<Duration> {
        None
    }
}
