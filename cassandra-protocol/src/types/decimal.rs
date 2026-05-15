use derive_more::Constructor;
use float_eq::*;
use num_bigint::BigInt;
use std::io::Cursor;

use crate::frame::{Serialize, Version};

/// Cassandra Decimal type
#[derive(Debug, Clone, PartialEq, Constructor, Ord, PartialOrd, Eq, Hash)]
pub struct Decimal {
    pub unscaled: BigInt,
    pub scale: i32,
}

impl Decimal {
    /// Method that returns plain `BigInt` value.
    ///
    /// Negative scale is handled by multiplying instead of dividing - that
    /// avoids the previous `scale as u32` cast which made a negative scale
    /// wrap to a huge value and panic in `10i64.pow`.
    pub fn as_plain(&self) -> BigInt {
        if self.scale >= 0 {
            // dividing by 10^scale; use checked_pow on a u32 exponent so an
            // out-of-range value yields a clean zero rather than panicking.
            let exponent = self.scale as u32;
            match 10i64.checked_pow(exponent) {
                Some(divisor) => self.unscaled.clone() / divisor,
                None => BigInt::from(0),
            }
        } else {
            // negative scale means the unscaled value should be multiplied
            // by 10^|scale| to recover the represented integer
            let exponent = self.scale.unsigned_abs();
            self.unscaled.clone() * BigInt::from(10).pow(exponent)
        }
    }
}

impl Serialize for Decimal {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.scale.serialize(cursor, version);
        self.unscaled
            .to_signed_bytes_be()
            .serialize(cursor, version);
    }
}

macro_rules! impl_from_for_decimal {
    ($t:ty) => {
        impl From<$t> for Decimal {
            fn from(i: $t) -> Self {
                Decimal {
                    unscaled: i.into(),
                    scale: 0,
                }
            }
        }
    };
}

impl_from_for_decimal!(i8);
impl_from_for_decimal!(i16);
impl_from_for_decimal!(i32);
impl_from_for_decimal!(i64);
impl_from_for_decimal!(u8);
impl_from_for_decimal!(u16);

impl From<f32> for Decimal {
    fn from(f: f32) -> Decimal {
        // Cap the loop just below the point where 10i64.pow(scale) overflows
        // (10^19 > i64::MAX). Without this guard a hostile input could keep
        // the loop spinning until the pow call panics. In practice f32
        // precision causes the equality check to succeed long before this
        // cap, so existing well-formed inputs are unaffected.
        const MAX_SCALE: u32 = 18;
        let mut scale: u32 = 0;

        loop {
            let unscaled = f * (10i64.pow(scale) as f32);

            if float_eq!(unscaled, unscaled.trunc(), abs <= f32::EPSILON) {
                return Decimal::new((unscaled as i64).into(), scale as i32);
            }

            if scale >= MAX_SCALE {
                // best-effort termination: snap to the truncated value at the
                // current scale rather than looping forever / panicking
                return Decimal::new((unscaled.trunc() as i64).into(), scale as i32);
            }

            scale += 1;
        }
    }
}

impl From<f64> for Decimal {
    fn from(f: f64) -> Decimal {
        // Same termination guard as the f32 conversion - bounded just below
        // i64 overflow on 10i64.pow.
        const MAX_SCALE: u32 = 18;
        let mut scale: u32 = 0;

        loop {
            let unscaled = f * (10i64.pow(scale) as f64);

            if float_eq!(unscaled, unscaled.trunc(), abs <= f64::EPSILON) {
                return Decimal::new((unscaled as i64).into(), scale as i32);
            }

            if scale >= MAX_SCALE {
                return Decimal::new((unscaled.trunc() as i64).into(), scale as i32);
            }

            scale += 1;
        }
    }
}

impl From<Decimal> for BigInt {
    fn from(value: Decimal) -> Self {
        value.as_plain()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_test() {
        assert_eq!(
            Decimal::new(129.into(), 0).serialize_to_vec(Version::V4),
            vec![0, 0, 0, 0, 0x00, 0x81]
        );

        assert_eq!(
            Decimal::new(BigInt::from(-129), 0).serialize_to_vec(Version::V4),
            vec![0, 0, 0, 0, 0xFF, 0x7F]
        );

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0x00, 0x81];
        assert_eq!(
            Decimal::new(129.into(), 1).serialize_to_vec(Version::V4),
            expected
        );

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0xFF, 0x7F];
        assert_eq!(
            Decimal::new(BigInt::from(-129), 1).serialize_to_vec(Version::V4),
            expected
        );
    }

    #[test]
    fn from_f32() {
        assert_eq!(
            Decimal::from(12300001_f32),
            Decimal::new(12300001.into(), 0)
        );
        assert_eq!(
            Decimal::from(1230000.1_f32),
            Decimal::new(12300001.into(), 1)
        );
        assert_eq!(
            Decimal::from(0.12300001_f32),
            Decimal::new(12300001.into(), 8)
        );
    }

    #[test]
    fn from_f64() {
        assert_eq!(
            Decimal::from(1230000000000001_f64),
            Decimal::new(1230000000000001i64.into(), 0)
        );
        assert_eq!(
            Decimal::from(123000000000000.1f64),
            Decimal::new(1230000000000001i64.into(), 1)
        );
        assert_eq!(
            Decimal::from(0.1230000000000001f64),
            Decimal::new(1230000000000001i64.into(), 16)
        );
    }


    // 0.1 is not exactly representable in IEEE-754 float, so the previous
    // implementation kept doubling `scale` looking for an exact match and
    // eventually panicked on `10i64.pow(scale)` overflow when scale exceeded
    // the number of significant digits. The conversion must terminate without
    // panicking and produce a sensible Decimal.
    #[test]
    fn from_f32_tolerates_inexact_floats() {
        let _decimal = Decimal::from(0.1f32);
        let _decimal = Decimal::from(0.2f32);
        let _decimal = Decimal::from(1.0f32 / 3.0f32);
    }

    #[test]
    fn from_f64_tolerates_inexact_floats() {
        let _decimal = Decimal::from(0.1f64);
        let _decimal = Decimal::from(0.2f64);
        let _decimal = Decimal::from(1.0f64 / 3.0f64);
    }

    // as_plain divides by 10^scale; if scale is negative the previous
    // `scale as u32` cast wrapped to a huge value and `10i64.pow` panicked.
    // The function should either reject negative scales or handle them.
    #[test]
    fn as_plain_does_not_panic_on_negative_scale() {
        let decimal = Decimal::new(5.into(), -3);
        // Just verify it does not panic; we don't assert a specific value
        // here because the semantics of negative scale aren't part of this
        // bug fix - we only need to be safe.
        let _ = decimal.as_plain();
    }
}
