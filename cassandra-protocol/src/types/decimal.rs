use derive_more::Constructor;
use num_bigint::{BigInt, Sign};
use std::convert::TryFrom;
use std::io::Cursor;
use thiserror::Error as ThisError;

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
        // any scale leaves zero unchanged, and a large negative one would otherwise compute an
        // arbitrarily large power of ten only to multiply it by zero
        if self.unscaled.sign() == Sign::NoSign {
            return BigInt::from(0);
        }

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

impl Decimal {
    /// Builds a decimal from the `{:e}` formatting of a finite float, which yields the shortest
    /// digit sequence that round-trips to the same float.
    fn from_float_exponential(exponential: &str) -> Decimal {
        let (mantissa, exponent) = exponential
            .split_once('e')
            .expect("exponential float formatting always contains an exponent");
        let exponent: i32 = exponent
            .parse()
            .expect("exponential float formatting yields an integer exponent");
        let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let unscaled: BigInt = format!("{integer}{fraction}")
            .parse()
            .expect("exponential float formatting yields decimal digits");

        let scale = fraction.len() as i32 - exponent;
        if scale < 0 {
            Decimal::new(unscaled * BigInt::from(10).pow(scale.unsigned_abs()), 0)
        } else {
            Decimal::new(unscaled, scale)
        }
    }
}

/// Error converting a NaN or infinite float into a [`Decimal`], which CQL decimals cannot
/// represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ThisError)]
#[error("Cannot convert a NaN or infinite float into a decimal")]
pub struct NonFiniteFloatError;

/// Converts a finite float into the decimal with the fewest digits that round-trips to the same
/// float, e.g. `0.1` becomes `1` with scale `1`. Negative zero becomes zero.
///
/// NaN and infinities yield [`NonFiniteFloatError`].
impl TryFrom<f32> for Decimal {
    type Error = NonFiniteFloatError;

    fn try_from(f: f32) -> Result<Decimal, NonFiniteFloatError> {
        if f.is_finite() {
            Ok(Decimal::from_float_exponential(&format!("{f:e}")))
        } else {
            Err(NonFiniteFloatError)
        }
    }
}

/// Converts a finite float into the decimal with the fewest digits that round-trips to the same
/// float, e.g. `0.1` becomes `1` with scale `1`. Negative zero becomes zero.
///
/// NaN and infinities yield [`NonFiniteFloatError`].
impl TryFrom<f64> for Decimal {
    type Error = NonFiniteFloatError;

    fn try_from(f: f64) -> Result<Decimal, NonFiniteFloatError> {
        if f.is_finite() {
            Ok(Decimal::from_float_exponential(&format!("{f:e}")))
        } else {
            Err(NonFiniteFloatError)
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
            Decimal::try_from(12300001_f32).unwrap(),
            Decimal::new(12300001.into(), 0)
        );
        assert_eq!(
            Decimal::try_from(1230000.1_f32).unwrap(),
            Decimal::new(12300001.into(), 1)
        );
        assert_eq!(
            Decimal::try_from(0.12300001_f32).unwrap(),
            Decimal::new(12300001.into(), 8)
        );
    }

    #[test]
    fn from_f64() {
        assert_eq!(
            Decimal::try_from(1230000000000001_f64).unwrap(),
            Decimal::new(1230000000000001i64.into(), 0)
        );
        assert_eq!(
            Decimal::try_from(123000000000000.1f64).unwrap(),
            Decimal::new(1230000000000001i64.into(), 1)
        );
        assert_eq!(
            Decimal::try_from(0.1230000000000001f64).unwrap(),
            Decimal::new(1230000000000001i64.into(), 16)
        );
    }

    fn approximate_value(decimal: &Decimal) -> f64 {
        decimal.unscaled.to_string().parse::<f64>().unwrap() / 10f64.powi(decimal.scale)
    }

    // 0.1 and 1/3 are not exactly representable in IEEE-754 floats; they must
    // convert to the shortest digits that round-trip to the same float rather
    // than to the exact binary value.
    #[test]
    fn from_f32_tolerates_inexact_floats() {
        assert_eq!(
            Decimal::try_from(0.1f32).unwrap(),
            Decimal::new(1.into(), 1)
        );
        assert_eq!(
            Decimal::try_from(0.2f32).unwrap(),
            Decimal::new(2.into(), 1)
        );

        let third = Decimal::try_from(1.0f32 / 3.0f32).unwrap();
        assert!(third.scale <= 18, "{:?}", third);
        assert!(
            (approximate_value(&third) - 1.0 / 3.0).abs() < 1e-6,
            "{:?}",
            third
        );
    }

    #[test]
    fn from_f64_tolerates_inexact_floats() {
        assert_eq!(
            Decimal::try_from(0.1f64).unwrap(),
            Decimal::new(1.into(), 1)
        );
        assert_eq!(
            Decimal::try_from(0.2f64).unwrap(),
            Decimal::new(2.into(), 1)
        );

        let third = Decimal::try_from(1.0f64 / 3.0f64).unwrap();
        assert!(third.scale <= 18, "{:?}", third);
        assert!(
            (approximate_value(&third) - 1.0 / 3.0).abs() < 1e-12,
            "{:?}",
            third
        );
    }

    #[test]
    fn from_f64_keeps_values_beyond_i64_range() {
        assert_eq!(
            Decimal::try_from(1e20f64).unwrap(),
            Decimal::new(BigInt::from(10).pow(20), 0)
        );
        assert_eq!(
            Decimal::try_from(-1e19f64).unwrap(),
            Decimal::new(-BigInt::from(10).pow(19), 0)
        );
    }

    #[test]
    fn from_f64_keeps_all_significant_digits() {
        assert_eq!(
            Decimal::try_from(1000000000000000.5f64).unwrap(),
            Decimal::new(10000000000000005i64.into(), 1)
        );
        assert_eq!(
            Decimal::try_from(-0.5f64).unwrap(),
            Decimal::new((-5).into(), 1)
        );
    }

    #[test]
    fn from_f64_keeps_small_values() {
        assert_eq!(
            Decimal::try_from(1e-30f64).unwrap(),
            Decimal::new(1.into(), 30)
        );
        assert_eq!(
            Decimal::try_from(5e-324f64).unwrap(),
            Decimal::new(5.into(), 324)
        );
    }

    #[test]
    fn from_f32_keeps_values_beyond_i64_range() {
        assert_eq!(
            Decimal::try_from(3e38f32).unwrap(),
            Decimal::new(BigInt::from(3) * BigInt::from(10).pow(38), 0)
        );
        assert_eq!(
            Decimal::try_from(1e-30f32).unwrap(),
            Decimal::new(1.into(), 30)
        );
    }

    #[test]
    fn from_non_finite_floats_is_error() {
        assert_eq!(Decimal::try_from(f64::NAN), Err(NonFiniteFloatError));
        assert_eq!(Decimal::try_from(f64::INFINITY), Err(NonFiniteFloatError));
        assert_eq!(
            Decimal::try_from(f64::NEG_INFINITY),
            Err(NonFiniteFloatError)
        );
        assert_eq!(Decimal::try_from(f32::NAN), Err(NonFiniteFloatError));
        assert_eq!(Decimal::try_from(f32::INFINITY), Err(NonFiniteFloatError));
        assert_eq!(
            Decimal::try_from(f32::NEG_INFINITY),
            Err(NonFiniteFloatError)
        );
    }

    #[test]
    fn non_finite_float_error_is_std_error() {
        let error: Box<dyn std::error::Error> = Box::new(NonFiniteFloatError);
        assert_eq!(
            error.to_string(),
            "Cannot convert a NaN or infinite float into a decimal"
        );
    }

    #[test]
    fn from_negative_zero() {
        assert_eq!(
            Decimal::try_from(-0.0f64).unwrap(),
            Decimal::new(0.into(), 0)
        );
        assert_eq!(
            Decimal::try_from(-0.0f32).unwrap(),
            Decimal::new(0.into(), 0)
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
    fn as_plain_of_zero_with_huge_negative_scale_is_zero() {
        assert_eq!(Decimal::new(0.into(), i32::MIN).as_plain(), BigInt::from(0));
    }

    #[test]
    fn as_plain_with_huge_scale_is_zero() {
        assert_eq!(
            Decimal::new(BigInt::from(i64::MAX), i32::MAX).as_plain(),
            BigInt::from(0)
        );
    }
}
