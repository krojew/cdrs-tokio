use derive_more::Constructor;
use float_eq::*;
use num::BigInt;
use std::io::Cursor;

use crate::frame::Serialize;

/// Cassandra Decimal type
#[derive(Debug, Clone, PartialEq, Constructor, Ord, PartialOrd, Eq, Hash)]
pub struct Decimal {
    pub unscaled: BigInt,
    pub scale: i32,
}

impl Decimal {
    /// Method that returns plain `f64` value.
    pub fn as_plain(&self) -> BigInt {
        self.unscaled.clone() / 10i64.pow(self.scale as u32)
    }
}

impl Serialize for Decimal {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.scale.serialize(cursor);
        self.unscaled.to_signed_bytes_be().serialize(cursor);
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
        let mut scale = 0;

        loop {
            let unscaled = f * (10i64.pow(scale) as f32);

            if float_eq!(unscaled, unscaled.trunc(), abs <= f32::EPSILON) {
                return Decimal::new((unscaled as i64).into(), scale as i32);
            }

            scale += 1;
        }
    }
}

impl From<f64> for Decimal {
    fn from(f: f64) -> Decimal {
        let mut scale = 0;

        loop {
            let unscaled = f * (10i64.pow(scale) as f64);

            if float_eq!(unscaled, unscaled.trunc(), abs <= f64::EPSILON) {
                return Decimal::new((unscaled as i64).into(), scale as i32);
            }

            scale += 1;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_test() {
        assert_eq!(
            Decimal::new(129.into(), 0).serialize_to_vec(),
            vec![0, 0, 0, 0, 0x00, 0x81]
        );

        assert_eq!(
            Decimal::new(BigInt::from(-129), 0).serialize_to_vec(),
            vec![0, 0, 0, 0, 0xFF, 0x7F]
        );

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0x00, 0x81];
        assert_eq!(Decimal::new(129.into(), 1).serialize_to_vec(), expected);

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0xFF, 0x7F];
        assert_eq!(
            Decimal::new(BigInt::from(-129), 1).serialize_to_vec(),
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
}
