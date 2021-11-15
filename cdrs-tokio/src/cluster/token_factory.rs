use bytes::Buf;
use std::cmp::min;
use std::num::Wrapping;

use cassandra_protocol::query::query_params::Murmur3Token;

const C1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
const C2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

// based on buggy Cassandra implementation
pub fn generate_murmur3_token(mut routing_key: &[u8]) -> Murmur3Token {
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
