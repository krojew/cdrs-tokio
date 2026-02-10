use derive_more::Constructor;
#[cfg(test)]
use mockall::automock;
use rand::{rng, RngExt};
use std::time::Duration;

const DEFAULT_BASE_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(60);

/// Determines the time for the next reconnection attempt when trying to reconnect to a node.
pub trait ReconnectionSchedule {
    /// Returns next reconnect delay or `None` if not attempt should be made.
    fn next_delay(&mut self) -> Option<Duration>;
}

/// Creates reconnection schedules when trying to re-establish connections.
#[cfg_attr(test, automock)]
pub trait ReconnectionPolicy {
    /// Creates new schedule when a connection needs to be re-established.
    fn new_node_schedule(&self) -> Box<dyn ReconnectionSchedule + Send + Sync>;
}

/// Schedules reconnection at constant interval.
#[derive(Copy, Clone, Constructor, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
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

#[derive(Constructor)]
struct ConstantReconnectionSchedule {
    base_delay: Duration,
}

impl ReconnectionSchedule for ConstantReconnectionSchedule {
    fn next_delay(&mut self) -> Option<Duration> {
        Some(self.base_delay)
    }
}

/// Never schedules reconnections.
#[derive(Default, Copy, Clone, Debug, PartialEq, Ord, PartialOrd, Eq, Hash)]
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

/// A reconnection policy that waits exponentially longer between each reconnection attempt (but
/// keeps a constant delay once a maximum delay is reached). The delay will increase exponentially,
/// with an added jitter.
#[derive(Copy, Clone, Constructor, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ExponentialReconnectionPolicy {
    base_delay: Duration,
    max_delay: Duration,
    max_attempts: usize,
}

impl ReconnectionPolicy for ExponentialReconnectionPolicy {
    fn new_node_schedule(&self) -> Box<dyn ReconnectionSchedule + Send + Sync> {
        Box::new(ExponentialReconnectionSchedule::new(
            self.base_delay,
            self.max_delay,
            self.max_attempts,
        ))
    }
}

impl Default for ExponentialReconnectionPolicy {
    fn default() -> Self {
        let base_delay = DEFAULT_BASE_DELAY.as_millis() as i64;
        let ceil = u32::from((base_delay & (base_delay - 1)) != 0);

        ExponentialReconnectionPolicy::new(
            DEFAULT_BASE_DELAY,
            DEFAULT_MAX_DELAY,
            (64 - (i64::MAX / base_delay).leading_zeros() - ceil) as usize,
        )
    }
}

struct ExponentialReconnectionSchedule {
    base_delay: Duration,
    max_delay: Duration,
    max_attempts: usize,
    attempt: usize,
}

impl ReconnectionSchedule for ExponentialReconnectionSchedule {
    fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt == self.max_attempts {
            return Some(self.max_delay);
        }

        self.attempt += 1;

        let delay = self
            .base_delay
            .saturating_mul(1u32.checked_shl(self.attempt as u32).unwrap_or(u32::MAX))
            .min(self.max_delay);

        let jitter = rng().random_range(85..116);

        Some(
            (delay / 100)
                .saturating_mul(jitter)
                .clamp(self.base_delay, self.max_delay),
        )
    }
}

impl ExponentialReconnectionSchedule {
    pub fn new(base_delay: Duration, max_delay: Duration, max_attempts: usize) -> Self {
        ExponentialReconnectionSchedule {
            base_delay,
            max_delay,
            max_attempts,
            attempt: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::retry::reconnection_policy::ExponentialReconnectionSchedule;
    use crate::retry::ReconnectionSchedule;

    #[test]
    fn should_reach_max_exponential_delay_without_panic() {
        let mut schedule = ExponentialReconnectionSchedule {
            base_delay: Default::default(),
            max_delay: Default::default(),
            max_attempts: usize::MAX,
            attempt: usize::MAX - 1,
        };

        schedule.next_delay();
    }
}
