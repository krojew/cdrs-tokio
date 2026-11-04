use crate::crc::{crc24, crc32};
use crate::frame::{
    COMPRESSED_FRAME_HEADER_LENGTH, FRAME_TRAILER_LENGTH, PAYLOAD_SIZE_LIMIT,
    UNCOMPRESSED_FRAME_HEADER_LENGTH,
};
use lz4_flex::block::get_maximum_output_size;
use lz4_flex::{compress, compress_into};

#[inline]
fn put3b(buffer: &mut [u8], value: i32) {
    let value = value.to_le_bytes();
    buffer[0] = value[0];
    buffer[1] = value[1];
    buffer[2] = value[2];
}

#[inline]
fn add_trailer(buffer: &mut Vec<u8>, payload_start: usize) {
    buffer.reserve(4);

    let crc = crc32(&buffer[payload_start..]).to_le_bytes();

    buffer.push(crc[0]);
    buffer.push(crc[1]);
    buffer.push(crc[2]);
    buffer.push(crc[3]);
}

/// An encoder for frames. Since protocol *v5*, frames became "envelopes" and a frame now can contain
/// multiple complete envelopes (self-contained frame) or a part of one bigger envelope.
///
/// Encoders are stateful and can either:
/// 1. Have multiple self-contained envelopes added.
/// 2. Have a single non self-contained envelope added.
///
/// In either case, the encoder is assumed to have the buffer ready to accept envelopes before
/// adding the first one or after calling [`reset_buffer`]. At some point, the frame can become
/// finalized (which is the only possible case when adding a non self-contained envelope) and the
/// returned buffer is assumed to be immutable and ready to be sent.  
pub trait FrameEncoder {
    /// Determines if payload of given size can fit in current frame buffer.
    fn can_fit(&self, len: usize) -> bool;

    /// Resets the internal state and prepares it for encoding envelopes.
    fn reset(&mut self);

    /// Adds a self-contained envelope to current frame.
    fn add_envelope(&mut self, envelope: Vec<u8>);

    /// Finalizes a self-contained encoded frame in the buffer.
    fn finalize_self_contained(&mut self) -> &[u8];

    /// Appends a large envelope and finalizes non self-contained encoded frame in the buffer.
    /// Copies as much envelope data as possible and returns new envelope buffer start.
    fn finalize_non_self_contained(&mut self, envelope: &[u8]) -> (usize, &[u8]);

    /// Checks if current frame contains any envelopes.
    fn has_envelopes(&self) -> bool;
}

/// Pre-V5 frame encoder which simply encodes one envelope directly in the buffer.
#[derive(Clone, Debug, Default)]
pub struct LegacyFrameEncoder {
    buffer: Vec<u8>,
}

impl FrameEncoder for LegacyFrameEncoder {
    #[inline]
    fn can_fit(&self, _len: usize) -> bool {
        // we support only one envelope per frame
        self.buffer.is_empty()
    }

    #[inline]
    fn reset(&mut self) {
        self.buffer.clear();
    }

    #[inline]
    fn add_envelope(&mut self, envelope: Vec<u8>) {
        self.buffer = envelope;
    }

    #[inline]
    fn finalize_self_contained(&mut self) -> &[u8] {
        &self.buffer
    }

    #[inline]
    fn finalize_non_self_contained(&mut self, envelope: &[u8]) -> (usize, &[u8]) {
        // attempting to finalize a non self-contained frame via the legacy encoder - while this
        // will work, the legacy encoder doesn't distinguish such frames and all are considered
        // self-contained

        self.buffer.clear();
        self.buffer.extend_from_slice(envelope);

        (envelope.len(), &self.buffer)
    }

    #[inline]
    fn has_envelopes(&self) -> bool {
        !self.buffer.is_empty()
    }
}

/// Post-V5 encoder with support for envelope frames with CRC checksum.
#[derive(Clone, Debug)]
pub struct UncompressedFrameEncoder {
    buffer: Vec<u8>,
}

impl FrameEncoder for UncompressedFrameEncoder {
    #[inline]
    fn can_fit(&self, len: usize) -> bool {
        (self.buffer.len() - UNCOMPRESSED_FRAME_HEADER_LENGTH).saturating_add(len)
            < PAYLOAD_SIZE_LIMIT
    }

    #[inline]
    fn reset(&mut self) {
        self.buffer.truncate(UNCOMPRESSED_FRAME_HEADER_LENGTH);
    }

    #[inline]
    fn add_envelope(&mut self, mut envelope: Vec<u8>) {
        self.buffer.append(&mut envelope);
    }

    fn finalize_self_contained(&mut self) -> &[u8] {
        self.write_header(true);
        add_trailer(&mut self.buffer, UNCOMPRESSED_FRAME_HEADER_LENGTH);

        &self.buffer
    }

    fn finalize_non_self_contained(&mut self, envelope: &[u8]) -> (usize, &[u8]) {
        let max_size = envelope.len().min(PAYLOAD_SIZE_LIMIT - 1);

        self.buffer.extend_from_slice(&envelope[..max_size]);
        self.buffer.reserve(FRAME_TRAILER_LENGTH);

        self.write_header(false);
        add_trailer(&mut self.buffer, UNCOMPRESSED_FRAME_HEADER_LENGTH);

        (max_size, &self.buffer)
    }

    #[inline]
    fn has_envelopes(&self) -> bool {
        self.buffer.len() > UNCOMPRESSED_FRAME_HEADER_LENGTH
    }
}

impl Default for UncompressedFrameEncoder {
    fn default() -> Self {
        let buffer = vec![0; UNCOMPRESSED_FRAME_HEADER_LENGTH];
        Self { buffer }
    }
}

impl UncompressedFrameEncoder {
    fn write_header(&mut self, self_contained: bool) {
        let len = self.buffer.len();
        debug_assert!(
            len < (PAYLOAD_SIZE_LIMIT + UNCOMPRESSED_FRAME_HEADER_LENGTH),
            "len: {} max: {}",
            len,
            PAYLOAD_SIZE_LIMIT + UNCOMPRESSED_FRAME_HEADER_LENGTH
        );

        let mut len = (len - UNCOMPRESSED_FRAME_HEADER_LENGTH) as u64;
        if self_contained {
            len |= 1 << 17;
        }

        put3b(self.buffer.as_mut_slice(), len as i32);
        put3b(&mut self.buffer[3..], crc24(&len.to_le_bytes()[..3]));
    }
}

/// Post-V5 Lz4 encoder with support for envelope frames with CRC checksum.
#[derive(Clone, Debug)]
pub struct Lz4FrameEncoder {
    buffer: Vec<u8>,
}

impl FrameEncoder for Lz4FrameEncoder {
    #[inline]
    fn can_fit(&self, len: usize) -> bool {
        // we don't know the whole compressed payload size, so we need to be conservative and expect
        // the worst case
        get_maximum_output_size(
            (self.buffer.len() - COMPRESSED_FRAME_HEADER_LENGTH).saturating_add(len),
        ) < PAYLOAD_SIZE_LIMIT
    }

    #[inline]
    fn reset(&mut self) {
        self.buffer.truncate(COMPRESSED_FRAME_HEADER_LENGTH);
    }

    #[inline]
    fn add_envelope(&mut self, mut envelope: Vec<u8>) {
        self.buffer.append(&mut envelope);
    }

    fn finalize_self_contained(&mut self) -> &[u8] {
        let uncompressed_size = self.buffer.len() - COMPRESSED_FRAME_HEADER_LENGTH;
        let mut compressed_payload = compress(&self.buffer[COMPRESSED_FRAME_HEADER_LENGTH..]);

        self.buffer.truncate(COMPRESSED_FRAME_HEADER_LENGTH);
        self.buffer.append(&mut compressed_payload);

        self.write_header(uncompressed_size, true);
        add_trailer(&mut self.buffer, COMPRESSED_FRAME_HEADER_LENGTH);

        &self.buffer
    }

    fn finalize_non_self_contained(&mut self, envelope: &[u8]) -> (usize, &[u8]) {
        let mut uncompressed_size = envelope.len().min(PAYLOAD_SIZE_LIMIT - 1);
        let offset = uncompressed_size;

        self.buffer.resize(
            get_maximum_output_size(uncompressed_size)
                + COMPRESSED_FRAME_HEADER_LENGTH
                + FRAME_TRAILER_LENGTH, // add space for trailer, so we don't allocate later
            0,
        );

        let mut compressed_size = compress_into(
            &envelope[..uncompressed_size],
            &mut self.buffer[COMPRESSED_FRAME_HEADER_LENGTH..],
        )
        .unwrap(); // we can safely unwrap, since we have at least the amount of space needed

        if compressed_size >= PAYLOAD_SIZE_LIMIT {
            // compressed size can exceed source size, therefore can exceed max payload size
            // Java driver simply ignores compression at this point, so ¯\_(ツ)_/¯
            self.buffer[COMPRESSED_FRAME_HEADER_LENGTH
                ..(COMPRESSED_FRAME_HEADER_LENGTH + uncompressed_size)]
                .copy_from_slice(&envelope[..uncompressed_size]);

            compressed_size = uncompressed_size;
            uncompressed_size = 0; // compressed size of 0 means no compression
        }

        self.buffer
            .truncate(COMPRESSED_FRAME_HEADER_LENGTH + compressed_size);

        self.write_header(uncompressed_size, false);
        add_trailer(&mut self.buffer, COMPRESSED_FRAME_HEADER_LENGTH);

        (offset, &self.buffer)
    }

    #[inline]
    fn has_envelopes(&self) -> bool {
        self.buffer.len() > COMPRESSED_FRAME_HEADER_LENGTH
    }
}

impl Default for Lz4FrameEncoder {
    fn default() -> Self {
        let buffer = vec![0; COMPRESSED_FRAME_HEADER_LENGTH];
        Self { buffer }
    }
}

impl Lz4FrameEncoder {
    fn write_header(&mut self, uncompressed_size: usize, self_contained: bool) {
        let len = self.buffer.len();
        debug_assert!(len < (PAYLOAD_SIZE_LIMIT + COMPRESSED_FRAME_HEADER_LENGTH));

        let mut header =
            (len - COMPRESSED_FRAME_HEADER_LENGTH) as u64 | ((uncompressed_size as u64) << 17);

        if self_contained {
            header |= 1 << 34;
        }

        let crc = crc24(&header.to_le_bytes()[..5]) as u64;

        let header = header | (crc << 40);
        self.buffer[..8].copy_from_slice(&header.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::Compression;
    use crate::frame::frame_decoder::{FrameDecoder, Lz4FrameDecoder, UncompressedFrameDecoder};
    use crate::frame::{Direction, Envelope, Flags, Opcode, Version, MAX_FRAME_SIZE};
    use std::convert::TryInto;

    fn make_envelope(body: Vec<u8>) -> (Envelope, Vec<u8>) {
        let envelope = Envelope {
            version: Version::V5,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 1,
            body,
            tracing_id: None,
            warnings: vec![],
            custom_payload: None,
        };
        let encoded = envelope.encode_with(Compression::None).unwrap();

        (envelope, encoded)
    }

    // xorshift output, which lz4 cannot compress
    fn incompressible_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn split_into_frames(encoder: &mut dyn FrameEncoder, envelope: &[u8]) -> Vec<Vec<u8>> {
        let mut frames = vec![];
        let mut start = 0;
        while start < envelope.len() {
            encoder.reset();

            let (consumed, frame) = encoder.finalize_non_self_contained(&envelope[start..]);
            assert!(consumed > 0);

            frames.push(frame.to_vec());
            start += consumed;
        }

        frames
    }

    // Frames from the Apache Cassandra Java driver test suite (BytesToSegmentDecoderTest). LZ4
    // output is not canonical, so the compressed reference frame is only checked when decoding.
    #[test]
    fn uncompressed_encoder_produces_reference_frames() {
        let mut encoder = UncompressedFrameEncoder::default();
        encoder.add_envelope(vec![0, 0, 0, 1]);
        assert_eq!(
            encoder.finalize_self_contained(),
            &[0x04, 0x00, 0x02, 0x01, 0xf9, 0xf2, 0, 0, 0, 1, 0x1f, 0xd6, 0x02, 0x2d]
        );

        let mut encoder = UncompressedFrameEncoder::default();
        assert!(encoder.can_fit(PAYLOAD_SIZE_LIMIT - 1));
        encoder.add_envelope(vec![1; PAYLOAD_SIZE_LIMIT - 1]);
        let frame = encoder.finalize_self_contained();
        assert_eq!(&frame[..6], &[0xff, 0xff, 0x03, 0x25, 0x40, 0x47]);
        assert_eq!(&frame[frame.len() - 4..], &[0xa0, 0x5c, 0x2f, 0x13]);
    }

    #[test]
    fn uncompressed_encoder_splits_large_envelope_into_frames() {
        let (envelope, encoded) = make_envelope(vec![0xab; 3 * PAYLOAD_SIZE_LIMIT]);

        let frames = split_into_frames(&mut UncompressedFrameEncoder::default(), &encoded);
        assert_eq!(frames.len(), encoded.len().div_ceil(PAYLOAD_SIZE_LIMIT - 1));

        for frame in &frames {
            assert!(frame.len() <= MAX_FRAME_SIZE);

            let header = u32::from_le_bytes([frame[0], frame[1], frame[2], 0]);
            assert_eq!(header & (1 << 17), 0, "frame must not be self-contained");
            assert_eq!(
                (header & 0x1ffff) as usize,
                frame.len() - UNCOMPRESSED_FRAME_HEADER_LENGTH - FRAME_TRAILER_LENGTH
            );
        }

        let mut wire = frames.concat();
        let envelopes = UncompressedFrameDecoder::default()
            .consume(&mut wire, Compression::None)
            .unwrap();
        assert_eq!(envelopes, vec![envelope]);
    }

    #[test]
    fn lz4_encoder_splits_large_envelope_into_frames() {
        for body in [
            vec![0xab; 3 * PAYLOAD_SIZE_LIMIT],
            incompressible_bytes(3 * PAYLOAD_SIZE_LIMIT),
        ] {
            let (envelope, encoded) = make_envelope(body);

            let frames = split_into_frames(&mut Lz4FrameEncoder::default(), &encoded);
            assert_eq!(frames.len(), encoded.len().div_ceil(PAYLOAD_SIZE_LIMIT - 1));

            for frame in &frames {
                assert!(frame.len() <= MAX_FRAME_SIZE);

                let header = u64::from_le_bytes(frame[..8].try_into().unwrap());
                assert_eq!(header & (1 << 34), 0, "frame must not be self-contained");
                assert_eq!(
                    (header & 0x1ffff) as usize,
                    frame.len() - COMPRESSED_FRAME_HEADER_LENGTH - FRAME_TRAILER_LENGTH
                );
            }

            let mut wire = frames.concat();
            let envelopes = Lz4FrameDecoder::default()
                .consume(&mut wire, Compression::None)
                .unwrap();
            assert_eq!(envelopes, vec![envelope]);
        }
    }
}
