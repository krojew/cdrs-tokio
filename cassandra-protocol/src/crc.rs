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

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values come from the frames in the Apache Cassandra Java driver test suite
    // (BytesToSegmentDecoderTest), where both checksums are stored in little-endian order.

    #[test]
    fn crc24_matches_reference_values() {
        assert_eq!(crc24(&[0x04, 0x00, 0x02]), 0xf2f901);
        assert_eq!(crc24(&[0xff, 0xff, 0x03]), 0x474025);
        assert_eq!(crc24(&[0x12, 0x00, 0x20, 0x00, 0x04]), 0x4fc991);
    }

    #[test]
    fn crc32_matches_reference_values() {
        assert_eq!(crc32(&[0, 0, 0, 1]), 0x2d02d61f);
        assert_eq!(crc32(&[1; 131071]), 0x132f5ca0);
        assert_eq!(
            crc32(&[0xf0, 0x01, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1]),
            0x907fd62b
        );
    }
}
