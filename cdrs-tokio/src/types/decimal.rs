use float_eq::*;
use num::BigInt;

use super::{to_int, to_varint};
use crate::frame::traits::AsBytes;
use crate::types::INT_LEN;

/// Cassandra Decimal type
#[derive(Debug, Clone, PartialEq)]
pub struct Decimal {
    pub unscaled: BigInt,
    pub scale: i32,
}

impl Decimal {
    pub fn new(unscaled: BigInt, scale: i32) -> Self {
        Decimal { unscaled, scale }
    }

    /// Method that returns plain `f64` value.
    pub fn as_plain(&self) -> BigInt {
        self.unscaled.clone() / 10i64.pow(self.scale as u32)
    }
}

impl AsBytes for Decimal {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(INT_LEN);
        bytes.extend(to_int(self.scale));
        bytes.extend(to_varint(self.unscaled.clone()));

        bytes
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
    fn into_cbytes_test() {
        assert_eq!(
            Decimal::new(129.into(), 0).as_bytes(),
            vec![0, 0, 0, 0, 0x00, 0x81]
        );

        assert_eq!(
            Decimal::new(BigInt::from(-129), 0).as_bytes(),
            vec![0, 0, 0, 0, 0xFF, 0x7F]
        );

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0x00, 0x81];
        assert_eq!(Decimal::new(129.into(), 1).as_bytes(), expected);

        let expected: Vec<u8> = vec![0, 0, 0, 1, 0xFF, 0x7F];
        assert_eq!(Decimal::new(BigInt::from(-129), 1).as_bytes(), expected);
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
