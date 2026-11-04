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
    /// Method that returns plain `BigInt` value, with any fractional part truncated toward zero.
    ///
    /// Negative scale is handled by multiplying instead of dividing.
    pub fn as_plain(&self) -> BigInt {
        if self.scale >= 0 {
            let exponent = self.scale as u32;
            // 10^scale >= 2^scale > |unscaled| once scale reaches the bit length, so the result
            // is zero without computing an arbitrarily large power
            if u64::from(exponent) >= self.unscaled.bits() {
                return BigInt::from(0);
            }

            self.unscaled.clone() / BigInt::from(10).pow(exponent)
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

    fn approximate_value(decimal: &Decimal) -> f64 {
        decimal.unscaled.to_string().parse::<f64>().unwrap() / 10f64.powi(decimal.scale)
    }

    // 0.1 is not exactly representable in IEEE-754 float, so an exact match
    // between the scaled value and its truncation may never occur. The
    // conversion must terminate and produce a close approximation.
    #[test]
    fn from_f32_tolerates_inexact_floats() {
        assert_eq!(Decimal::from(0.1f32), Decimal::new(1.into(), 1));
        assert_eq!(Decimal::from(0.2f32), Decimal::new(2.into(), 1));

        let third = Decimal::from(1.0f32 / 3.0f32);
        assert!(third.scale <= 18, "{:?}", third);
        assert!(
            (approximate_value(&third) - 1.0 / 3.0).abs() < 1e-6,
            "{:?}",
            third
        );
    }

    #[test]
    fn from_f64_tolerates_inexact_floats() {
        assert_eq!(Decimal::from(0.1f64), Decimal::new(1.into(), 1));
        assert_eq!(Decimal::from(0.2f64), Decimal::new(2.into(), 1));

        let third = Decimal::from(1.0f64 / 3.0f64);
        assert!(third.scale <= 18, "{:?}", third);
        assert!(
            (approximate_value(&third) - 1.0 / 3.0).abs() < 1e-12,
            "{:?}",
            third
        );
    }

    #[test]
    fn as_plain_handles_negative_scale() {
        assert_eq!(Decimal::new(5.into(), -3).as_plain(), BigInt::from(5000));
        assert_eq!(
            Decimal::new(BigInt::from(-5), -3).as_plain(),
            BigInt::from(-5000)
        );
    }

    #[test]
    fn as_plain_truncates_fraction() {
        assert_eq!(
            Decimal::new(123456789.into(), 3).as_plain(),
            BigInt::from(123456)
        );
        assert_eq!(
            Decimal::new(BigInt::from(-123456789), 3).as_plain(),
            BigInt::from(-123456)
        );
        assert_eq!(Decimal::new(5.into(), 30).as_plain(), BigInt::from(0));
    }

    #[test]
    fn as_plain_handles_scale_beyond_i64_range() {
        let unscaled = BigInt::from(12345) * BigInt::from(10).pow(19);
        assert_eq!(
            Decimal::new(unscaled.clone(), 19).as_plain(),
            BigInt::from(12345)
        );
        assert_eq!(Decimal::new(-unscaled, 19).as_plain(), BigInt::from(-12345));

        let unscaled = BigInt::from(7) * BigInt::from(10).pow(40) + 3;
        assert_eq!(Decimal::new(unscaled, 40).as_plain(), BigInt::from(7));
    }

    #[test]
    fn as_plain_with_huge_scale_is_zero() {
        assert_eq!(
            Decimal::new(BigInt::from(i64::MAX), i32::MAX).as_plain(),
            BigInt::from(0)
        );
    }
}
