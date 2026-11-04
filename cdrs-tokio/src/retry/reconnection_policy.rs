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

/// A reconnection policy that waits exponentially longer between each reconnection attempt
/// (but keeps a constant delay once a maximum delay is reached). The delay will increase
/// exponentially, with an added jitter.
///
/// Note: by design this policy retries forever. The delay grows by a factor of two on each
/// attempt up to `max_delay`, then stays near `max_delay` indefinitely. The `max_attempts`
/// field controls only when the delay saturates - it is the number of doublings before
/// `base_delay` reaches `max_delay`, NOT a hard cap on the number of reconnection attempts.
/// Use [`NeverReconnectionPolicy`] if you want to avoid reconnections entirely.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ExponentialReconnectionPolicy {
    base_delay: Duration,
    max_delay: Duration,
    /// Attempts after which the delay has saturated at [`Self::max_delay`]. Subsequent
    /// reconnection attempts continue to be scheduled near `max_delay` indefinitely.
    max_attempts: usize,
}

impl ExponentialReconnectionPolicy {
    /// Creates a new policy.
    ///
    /// # Panics
    ///
    /// Panics if `base_delay` is greater than `max_delay`.
    #[inline]
    pub const fn new(base_delay: Duration, max_delay: Duration, max_attempts: usize) -> Self {
        assert!(
            base_delay.as_nanos() <= max_delay.as_nanos(),
            "ExponentialReconnectionPolicy base_delay must not be greater than max_delay!"
        );

        ExponentialReconnectionPolicy {
            base_delay,
            max_delay,
            max_attempts,
        }
    }
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
        // once saturated, keep scheduling around the max delay forever
        let delay = if self.attempt >= self.max_attempts {
            self.max_delay
        } else {
            self.attempt += 1;

            self.base_delay
                .saturating_mul(1u32.checked_shl(self.attempt as u32).unwrap_or(u32::MAX))
                .min(self.max_delay)
        };

        // Apply +/-15% jitter so a flock of clients reconnecting at the same
        // time don't all retry in lockstep. The jitter is sampled within
        // [base_delay, max_delay] rather than clamped afterward, so delays
        // near a bound don't pile up on it.
        let spread = delay / 100 * 15;
        let low = delay.saturating_sub(spread).max(self.base_delay);
        let high = delay.saturating_add(spread).min(self.max_delay);
        let jittered = low
            + high
                .saturating_sub(low)
                .mul_f64(rng().random_range(0.0..=1.0));

        Some(jittered.clamp(self.base_delay, self.max_delay))
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
    use crate::retry::{ExponentialReconnectionPolicy, ReconnectionSchedule};

    #[test]
    #[should_panic(expected = "base_delay must not be greater than max_delay")]
    fn exponential_policy_rejects_base_delay_above_max_delay() {
        use std::time::Duration;
        ExponentialReconnectionPolicy::new(Duration::from_secs(2), Duration::from_secs(1), 1);
    }

    #[test]
    fn exponential_policy_accepts_equal_base_and_max_delay() {
        use std::time::Duration;
        ExponentialReconnectionPolicy::new(Duration::from_secs(1), Duration::from_secs(1), 1);
    }

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

    // Once the schedule's attempt counter has reached or surpassed
    // max_attempts, every subsequent call must return a delay - never None
    // and never something larger than max_delay.
    #[test]
    fn saturated_schedule_keeps_returning_delay_up_to_max() {
        use std::time::Duration;
        let max_delay = Duration::from_secs(60);
        let mut schedule = ExponentialReconnectionSchedule {
            base_delay: Duration::from_secs(1),
            max_delay,
            // the attempt counter is past max_attempts, so the saturation
            // branch is exercised instead of doubling the delay again
            max_attempts: 5,
            attempt: 6,
        };

        for _ in 0..3 {
            let delay = schedule.next_delay().unwrap();
            assert!(delay <= max_delay, "{:?}", delay);
            assert!(delay >= max_delay / 2, "{:?}", delay);
        }
    }

    fn assert_jittered_within_bounds(mut schedule: ExponentialReconnectionSchedule) {
        let (base_delay, max_delay) = (schedule.base_delay, schedule.max_delay);
        let delays: Vec<_> = (0..1000).map(|_| schedule.next_delay().unwrap()).collect();

        for delay in &delays {
            assert!(*delay >= base_delay && *delay <= max_delay, "{:?}", delay);
        }

        let distinct = delays
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(distinct > 100, "only {} distinct delays", distinct);

        let at_max = delays.iter().filter(|delay| **delay == max_delay).count();
        assert!(at_max < 10, "{} delays clamped to max", at_max);
    }

    #[test]
    fn saturated_schedule_applies_jitter() {
        use std::time::Duration;
        assert_jittered_within_bounds(ExponentialReconnectionSchedule {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: 5,
            attempt: 5,
        });
    }

    #[test]
    fn capped_schedule_applies_jitter_below_max() {
        use std::time::Duration;
        // 2^7 seconds exceeds the max delay, but the schedule is not saturated yet
        let mut schedule = ExponentialReconnectionSchedule {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: usize::MAX,
            attempt: 6,
        };

        let delays: Vec<_> = (0..1000)
            .map(|_| {
                schedule.attempt = 6;
                schedule.next_delay().unwrap()
            })
            .collect();

        let at_max = delays
            .iter()
            .filter(|delay| **delay == schedule.max_delay)
            .count();
        assert!(at_max < 10, "{} delays clamped to max", at_max);
        assert!(delays.iter().all(|delay| *delay <= schedule.max_delay));
    }
}
