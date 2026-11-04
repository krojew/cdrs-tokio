use std::io::{self, Cursor, Write};
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
        let mut buffer = Vec::with_capacity(3 * MAX_VINT_LEN);

        write_vint(self.months as i64, &mut buffer);
        write_vint(self.days as i64, &mut buffer);
        write_vint(self.nanoseconds, &mut buffer);

        let _ = cursor.write(&buffer);
    }
}

const MAX_VINT_LEN: usize = 9;

/// Appends a CQL `[vint]`: the zig-zag encoded value written as an `[unsigned vint]`.
fn write_vint(value: i64, buffer: &mut Vec<u8>) {
    write_unsigned_vint(((value << 1) ^ (value >> 63)) as u64, buffer);
}

/// Appends a CQL `[unsigned vint]`: big-endian, with the number of extra bytes given by the count
/// of leading one bits in the first byte.
fn write_unsigned_vint(value: u64, buffer: &mut Vec<u8>) {
    let magnitude = (value | 1).leading_zeros() as usize;
    // 7 value bits per byte for up to 8 bytes; a 9-byte vint carries all 64 bits after 0xFF
    let size = (639 - magnitude * 9) >> 6;

    if size == MAX_VINT_LEN {
        buffer.push(0xFF);
        buffer.extend_from_slice(&value.to_be_bytes());
        return;
    }

    let extra_bytes = size - 1;
    let start = buffer.len();
    buffer.extend_from_slice(&value.to_be_bytes()[8 - size..]);
    buffer[start] |= !(0xFFu8 >> extra_bytes);
}

/// Reads a CQL `[vint]` from the start of `bytes`, returning the value and the number of bytes
/// consumed.
pub(crate) fn read_vint(bytes: &[u8]) -> io::Result<(i64, usize)> {
    let (value, len) = read_unsigned_vint(bytes)?;
    Ok((((value >> 1) as i64) ^ -((value & 1) as i64), len))
}

fn read_unsigned_vint(bytes: &[u8]) -> io::Result<(u64, usize)> {
    let first_byte = *bytes
        .first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing vint"))?;

    let extra_bytes = first_byte.leading_ones() as usize;
    let rest = bytes
        .get(1..=extra_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated vint"))?;

    let first_byte_mask = (0xFFu16 >> extra_bytes) as u8;
    let value = rest
        .iter()
        .fold((first_byte & first_byte_mask) as u64, |value, byte| {
            (value << 8) | *byte as u64
        });

    Ok((value, 1 + extra_bytes))
}

#[cfg(test)]
mod tests {
    use crate::frame::{Serialize, Version};
    use crate::types::data_serialization_types::decode_duration;
    use crate::types::duration::Duration;

    // Encodings produced by Cassandra's VIntCoding.writeVInt: zig-zag, then an
    // unsigned vint whose first byte's leading one bits count the extra bytes,
    // with the value stored big-endian. 128000 zig-zags to 256000, the example
    // from native_protocol_v5.spec section 3.
    #[allow(clippy::type_complexity)]
    const VECTORS: &[((i32, i32, i64), &[u8])] = &[
        ((0, 0, 0), &[0x00, 0x00, 0x00]),
        ((1, 2, 3), &[0x02, 0x04, 0x06]),
        ((-1, -2, -3), &[0x01, 0x03, 0x05]),
        ((63, 64, 0), &[0x7E, 0x80, 0x80, 0x00]),
        ((-64, -65, 0), &[0x7F, 0x80, 0x81, 0x00]),
        ((100, 200, 300), &[0x80, 0xC8, 0x81, 0x90, 0x82, 0x58]),
        ((0, 0, 8191), &[0x00, 0x00, 0xBF, 0xFE]),
        ((0, 0, 8192), &[0x00, 0x00, 0xC0, 0x40, 0x00]),
        ((0, 0, 128000), &[0x00, 0x00, 0xC3, 0xE8, 0x00]),
        (
            (0, 0, -(1 << 55)),
            &[0x00, 0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ),
        (
            (0, 0, 1 << 55),
            &[
                0x00, 0x00, 0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        ),
        (
            (i32::MAX, i32::MAX, i64::MAX),
            &[
                0xF0, 0xFF, 0xFF, 0xFF, 0xFE, 0xF0, 0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
            ],
        ),
        (
            (i32::MIN, i32::MIN, i64::MIN),
            &[
                0xF0, 0xFF, 0xFF, 0xFF, 0xFF, 0xF0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            ],
        ),
    ];

    #[test]
    fn should_serialize_duration() {
        let duration = Duration::new(100, 200, 300).unwrap();
        assert_eq!(
            duration.serialize_to_vec(Version::V5),
            vec![0x80, 0xC8, 0x81, 0x90, 0x82, 0x58]
        );
    }

    #[test]
    fn should_serialize_duration_as_cql_vints() {
        for &((months, days, nanoseconds), expected) in VECTORS {
            let duration = Duration::new(months, days, nanoseconds).unwrap();
            assert_eq!(
                duration.serialize_to_vec(Version::V5),
                expected,
                "{duration:?}"
            );
        }
    }

    #[test]
    fn should_decode_duration_from_cql_vints() {
        for &((months, days, nanoseconds), bytes) in VECTORS {
            assert_eq!(
                decode_duration(bytes).unwrap(),
                Duration::new(months, days, nanoseconds).unwrap(),
                "{bytes:02X?}"
            );
        }
    }

    #[test]
    fn should_reject_truncated_duration() {
        for &(_, bytes) in VECTORS {
            for len in 0..bytes.len() {
                assert!(
                    decode_duration(&bytes[..len]).is_err(),
                    "{:02X?}",
                    &bytes[..len]
                );
            }
        }
    }

    #[test]
    fn should_reject_out_of_range_duration_months_and_days() {
        // zig-zag of i32::MAX + 1 needs a full 5-byte vint and does not fit i32
        let too_many_months = [0xF1, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(decode_duration(&too_many_months).is_err());

        let too_many_days = [0x00, 0xF1, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(decode_duration(&too_many_days).is_err());
    }
}
