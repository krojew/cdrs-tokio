use integer_encoding::VarInt;
use std::io::{Cursor, Write};
use thiserror::Error;

use crate::frame::{Serialize, Version};

/// Possible `Duration` creation error.
#[derive(Debug, Error, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum DurationCreationError {
    #[error(
        "All values must be either negative or positive, got {months} months, {days} days, {nanoseconds} nanoseconds"
    )]
    MixedPositiveAndNegative {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
}

/// Cassandra Duration type. A duration stores separately months, days, and seconds due to the fact
/// that the number of days in a month varies, and a day can have 23 or 25 hours if a daylight
/// saving is involved.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Duration {
    months: i32,
    days: i32,
    nanoseconds: i64,
}

impl Duration {
    pub fn new(months: i32, days: i32, nanoseconds: i64) -> Result<Self, DurationCreationError> {
        if (months < 0 || days < 0 || nanoseconds < 0)
            && (months > 0 || days > 0 || nanoseconds > 0)
        {
            Err(DurationCreationError::MixedPositiveAndNegative {
                months,
                days,
                nanoseconds,
            })
        } else {
            Ok(Self {
                months,
                days,
                nanoseconds,
            })
        }
    }

    pub fn months(&self) -> i32 {
        self.months
    }

    pub fn days(&self) -> i32 {
        self.days
    }

    pub fn nanoseconds(&self) -> i64 {
        self.nanoseconds
    }
}

impl Serialize for Duration {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, _version: Version) {
        let month_space = self.months.required_space();
        let day_space = self.days.required_space();

        let mut buffer = vec![0u8; month_space + day_space + self.nanoseconds.required_space()];

        self.months.encode_var(&mut buffer);
        self.days.encode_var(&mut buffer[month_space..]);
        self.nanoseconds
            .encode_var(&mut buffer[(month_space + day_space)..]);

        let _ = cursor.write(&buffer);
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::{Serialize, Version};
    use crate::types::duration::Duration;

    #[test]
    fn should_serialize_duration() {
        let duration = Duration::new(100, 200, 300).unwrap();
        assert_eq!(
            duration.serialize_to_vec(Version::V5),
            vec![200, 1, 144, 3, 216, 4]
        );
    }
}
