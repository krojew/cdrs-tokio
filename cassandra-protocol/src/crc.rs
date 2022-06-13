use crc32fast::Hasher;

const CRC24_POLY: i32 = 0x1974f0b;
const CRC24_INIT: i32 = 0x875060;

/// Computes crc24 value of `bytes`.
pub fn crc24(bytes: &[u8]) -> i32 {
    bytes.iter().fold(CRC24_INIT, |mut crc, byte| {
        crc ^= (*byte as i32) << 16;

        for _ in 0..8 {
            crc <<= 1;
            if (crc & 0x1000000) != 0 {
                crc ^= CRC24_POLY;
            }
        }

        crc
    })
}

/// Computes crc32 value of `bytes`.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&[0xfa, 0x2d, 0x55, 0xca]); // Cassandra appends a few bytes and forgets to mention it in the spec...
    hasher.update(bytes);
    hasher.finalize()
}
