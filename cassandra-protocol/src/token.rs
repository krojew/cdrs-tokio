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
        // Murmur3Partitioner maps an empty key to the minimum token
        if routing_key.is_empty() {
            return Murmur3Token::new(i64::MIN);
        }

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

        // the minimum token is reserved, so Murmur3Partitioner moves a hash landing on it to the
        // maximum token
        if h1.0 == i64::MIN {
            Murmur3Token::new(i64::MAX)
        } else {
            Murmur3Token::new(h1.0)
        }
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

    // From gocql internal/murmur/murmur_test.go (TestMurmur3H1, TestMurmur3H1_CassandraSign,
    // BenchmarkMurmur3H1).
    #[test]
    fn test_generate_murmur3_token_gocql_vectors() {
        let series: [u64; 19] = [
            0x2ac9debed546a380,
            0x649e4eaa7fc1708e,
            0xce68f60d7c353bdb,
            0x0f95757ce7f38254,
            0x0f04e459497f3fc1,
            0x88c0a92586be0a27,
            0x13eb9fb82606f7a6,
            0x8236039b7387354d,
            0x4c1e87519fe738ba,
            0x3f9652ac3effeb24,
            0x3f33760ded9006c6,
            0xaed70a6631854cb1,
            0x8a299a8f8e0e2da7,
            0x624b675c779249a6,
            0xa4b203bb1d90b9a3,
            0xa3293ad698ecb99a,
            0xbc740023dbd50048,
            0x3fe5ab9837d25cdd,
            0x2d0338c1ca87d132,
        ];
        let digits = "0123456789012345678";
        for (i, expected) in series.iter().enumerate() {
            let key = &digits.as_bytes()[..=i];
            assert_eq!(
                Murmur3Token::generate(key).value,
                *expected as i64,
                "{:?}",
                key
            );
        }

        for (key, expected) in [
            ("hello", 0xcbd8a7b341bd9b02_u64),
            ("hello, world", 0x342fac623a5ebc8e),
            ("19 Jan 2038 at 3:14:07 AM", 0xb89e5988b737affc),
            (
                "The quick brown fox jumps over the lazy dog.",
                0xcd99481f9ee902c9,
            ),
        ] {
            assert_eq!(
                Murmur3Token::generate(key.as_bytes()).value,
                expected as i64,
                "{}",
                key
            );
        }

        let signed_key = [
            0x00, 0x10, 0x43, 0x27, 0x52, 0x9f, 0xb6, 0x45, 0xdd, 0x00, 0xb8, 0x83, 0xec, 0x39,
            0xae, 0x44, 0x8b, 0xb8, 0x00, 0x00, 0x04, 0x00, 0x06, 0x6a, 0x6b, 0x00,
        ];
        assert_eq!(
            Murmur3Token::generate(&signed_key).value,
            -9223371632693506265
        );

        let long_key: Vec<u8> = (0..1024).map(|i| i as u8).collect();
        assert_eq!(Murmur3Token::generate(&long_key).value, 7627370222079200297);
    }

    // From the DataStax python-driver tests/unit/test_metadata.py (Murmur3TokensTest).
    #[test]
    fn test_generate_murmur3_token_python_driver_vectors() {
        assert_eq!(Murmur3Token::generate(b"123").value, -7468325962851647638);
        assert_eq!(
            Murmur3Token::generate(&b"\x00\xff\x10\xfa\x99".repeat(10)).value,
            5837342703291459765
        );
        assert_eq!(
            Murmur3Token::generate(&[0xfe; 8]).value,
            -8927430733708461935
        );
        assert_eq!(
            Murmur3Token::generate(&[0x10; 8]).value,
            1446172840243228796
        );
    }

    // Computed with Cassandra's MurmurHash.hash3_x64_128 and Murmur3Partitioner.getToken for keys
    // of one full block followed by every tail length, using bytes with the high bit set.
    #[test]
    fn test_generate_murmur3_token_block_with_every_tail_length() {
        let expected: [i64; 16] = [
            98493579079920379,
            468209433970893934,
            -5392407157453564661,
            6166294579111480802,
            -5601928453503323687,
            7790259223999475574,
            7142400945347608507,
            3575278787307344037,
            -7820917022413328062,
            -3138480301959228827,
            -5043898629676150093,
            6893181342285309456,
            -2834769081185784227,
            -60608620664882271,
            3799801908474491645,
            7904706896177658798,
        ];

        for (tail, expected) in expected.iter().enumerate() {
            let key: Vec<u8> = (0..16 + tail).map(|i| (i * 37 + 200) as u8).collect();
            assert_eq!(
                Murmur3Token::generate(&key).value,
                *expected,
                "{:02x?}",
                key
            );
        }
    }

    #[test]
    fn test_generate_murmur3_token_empty_key_is_minimum() {
        assert_eq!(Murmur3Token::generate(&[]).value, i64::MIN);
    }

    // The key is Cassandra's MurmurHash.inv_hash3_x64_128 of a hash whose first half is i64::MIN,
    // which Murmur3Partitioner reserves for the minimum token.
    #[test]
    fn test_generate_murmur3_token_normalizes_minimum_hash() {
        let key = [
            0x65, 0x3C, 0xBE, 0xFB, 0x85, 0xEC, 0x31, 0x11, 0xB4, 0xE3, 0x8F, 0xA9, 0xBC, 0x7C,
            0xBC, 0xAE,
        ];
        assert_eq!(Murmur3Token::generate(&key).value, i64::MAX);
    }
}
