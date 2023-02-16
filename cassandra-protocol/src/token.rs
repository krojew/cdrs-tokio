use crate::error::Error;
use bytes::Buf;
use derive_more::Constructor;
use std::cmp::min;
use std::convert::TryFrom;
use std::num::Wrapping;

const C1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
const C2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

/// A token on the ring. Only Murmur3 tokens are supported for now.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Constructor)]
pub struct Murmur3Token {
    pub value: i64,
}

impl Murmur3Token {
    // based on buggy Cassandra implementation
    pub fn generate(mut routing_key: &[u8]) -> Self {
        let length = routing_key.len();

        let mut h1: Wrapping<i64> = Wrapping(0);
        let mut h2: Wrapping<i64> = Wrapping(0);

        while routing_key.len() >= 16 {
            let mut k1 = Wrapping(routing_key.get_i64_le());
            let mut k2 = Wrapping(routing_key.get_i64_le());

            k1 *= C1;
            k1 = rotl64(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * Wrapping(5) + Wrapping(0x52dce729);

            k2 *= C2;
            k2 = rotl64(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * Wrapping(5) + Wrapping(0x38495ab5);
        }

        let mut k1 = Wrapping(0_i64);
        let mut k2 = Wrapping(0_i64);

        debug_assert!(routing_key.len() < 16);

        if routing_key.len() > 8 {
            for i in (8..routing_key.len()).rev() {
                k2 ^= Wrapping(routing_key[i] as i8 as i64) << ((i - 8) * 8);
            }

            k2 *= C2;
            k2 = rotl64(k2, 33);
            k2 *= C1;
            h2 ^= k2;
        }

        if !routing_key.is_empty() {
            for i in (0..min(8, routing_key.len())).rev() {
                k1 ^= Wrapping(routing_key[i] as i8 as i64) << (i * 8);
            }

            k1 *= C1;
            k1 = rotl64(k1, 31);
            k1 *= C2;
            h1 ^= k1;
        }

        h1 ^= Wrapping(length as i64);
        h2 ^= Wrapping(length as i64);

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;

        Murmur3Token::new(h1.0)
    }
}

impl TryFrom<String> for Murmur3Token {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse()
            .map_err(|error| format!("Error parsing token: {error}").into())
            .map(Murmur3Token::new)
    }
}

impl From<i64> for Murmur3Token {
    fn from(value: i64) -> Self {
        Murmur3Token::new(value)
    }
}

#[inline]
fn rotl64(v: Wrapping<i64>, n: u32) -> Wrapping<i64> {
    Wrapping((v.0 << n) | (v.0 as u64 >> (64 - n)) as i64)
}

#[inline]
fn fmix(mut k: Wrapping<i64>) -> Wrapping<i64> {
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xff51afd7ed558ccd_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xc4ceb9fe1a85ec53_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);

    k
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_generate_murmur3_token() {
        for s in [
            ("testvalue", 5965290492934326460),
            ("testvalue123", 1518494936189046133),
            ("example_key", -7813763279771224608),
            ("château", 9114062196463836094),
        ] {
            let generated_token = Murmur3Token::generate(s.0.as_bytes());
            assert_eq!(generated_token.value, s.1);
        }
    }
}
