use crate::compression::{Compression, CompressionError};
use crate::crc::{crc24, crc32};
use crate::error::{Error, Result};
use crate::frame::{
    checked_body_len, Envelope, ParseEnvelopeError, COMPRESSED_FRAME_HEADER_LENGTH,
    ENVELOPE_HEADER_LEN, FRAME_TRAILER_LENGTH, MAX_FRAME_SIZE, PAYLOAD_SIZE_LIMIT,
    UNCOMPRESSED_FRAME_HEADER_LENGTH,
};
use lz4_flex::decompress;
use std::convert::TryInto;
use std::io;

#[inline]
fn create_unexpected_self_contained_error() -> Error {
    "Found self-contained frame while waiting for non self-contained continuation!".into()
}

#[inline]
fn create_header_crc_mismatch_error(computed_crc: i32, header_crc24: i32) -> Error {
    format!("Header CRC mismatch - expected {header_crc24}, found {computed_crc}.",).into()
}

#[inline]
fn create_payload_crc_mismatch_error(computed_crc: u32, payload_crc32: u32) -> Error {
    format!("Payload CRC mismatch - read {payload_crc32}, computed {computed_crc}.",).into()
}

fn extract_envelopes(buffer: &[u8], compression: Compression) -> Result<(usize, Vec<Envelope>)> {
    let mut current_pos = 0;
    let mut envelopes = vec![];

    loop {
        match Envelope::from_buffer(&buffer[current_pos..], compression) {
            Ok(envelope) => {
                envelopes.push(envelope.envelope);
                current_pos += envelope.envelope_len;
            }
            Err(ParseEnvelopeError::NotEnoughBytes) => break,
            Err(error) => return Err(error.to_string().into()),
        }
    }

    Ok((current_pos, envelopes))
}

fn try_decode_envelopes_with_spare_data(
    buffer: &mut Vec<u8>,
    compression: Compression,
) -> Result<(Vec<Envelope>, Vec<u8>)> {
    let (current_pos, envelopes) = extract_envelopes(buffer.as_slice(), compression)?;
    Ok((envelopes, buffer.split_off(current_pos)))
}

fn try_decode_envelopes_without_spare_data(buffer: &[u8]) -> Result<Vec<Envelope>> {
    let (current_pos, envelopes) = extract_envelopes(buffer, Compression::None)?;
    if current_pos != buffer.len() {
        return Err(format!(
            "Self-contained frame ends with an incomplete envelope ({} trailing bytes).",
            buffer.len() - current_pos
        )
        .into());
    }

    Ok(envelopes)
}

/// A decoder for frames. Since protocol v5, frames became "envelopes" and a frame now can contain
/// multiple complete envelopes (self-contained frame) or a part of one bigger envelope.
pub trait FrameDecoder {
    /// Consumes some data and returns decoded envelopes. Decoders can be stateful, so data can be
    /// buffered until envelopes can be parsed.
    /// The buffer passed in should be cleared of consumed data by the decoder.
    fn consume(&mut self, data: &mut Vec<u8>, compression: Compression) -> Result<Vec<Envelope>>;
}

/// Pre-V5 frame decoder which simply decodes one envelope directly into a buffer.
#[derive(Clone, Debug)]
pub struct LegacyFrameDecoder {
    buffer: Vec<u8>,
}

impl Default for LegacyFrameDecoder {
    fn default() -> Self {
        Self {
            buffer: Vec::with_capacity(MAX_FRAME_SIZE),
        }
    }
}

impl FrameDecoder for LegacyFrameDecoder {
    fn consume(&mut self, data: &mut Vec<u8>, compression: Compression) -> Result<Vec<Envelope>> {
        if self.buffer.is_empty() {
            // optimistic case
            let (envelopes, buffer) = try_decode_envelopes_with_spare_data(data, compression)?;

            self.buffer = buffer;
            data.clear();

            return Ok(envelopes);
        }

        self.buffer.append(data);

        let (envelopes, buffer) =
            try_decode_envelopes_with_spare_data(&mut self.buffer, compression)?;

        self.buffer = buffer;
        Ok(envelopes)
    }
}

/// Post-V5 Lz4 decoder with support for envelope frames with CRC checksum.
#[derive(Clone, Debug, Default)]
pub struct Lz4FrameDecoder {
    inner_decoder: GenericFrameDecoder,
}

impl FrameDecoder for Lz4FrameDecoder {
    //noinspection DuplicatedCode
    #[inline]
    fn consume(&mut self, data: &mut Vec<u8>, _compression: Compression) -> Result<Vec<Envelope>> {
        self.inner_decoder.consume(data, Self::try_decode_frame)
    }
}

impl Lz4FrameDecoder {
    fn try_decode_frame(buffer: &mut Vec<u8>) -> Result<Option<(bool, Vec<u8>)>> {
        let buffer_len = buffer.len();
        if buffer_len < COMPRESSED_FRAME_HEADER_LENGTH {
            return Ok(None);
        }

        let header =
            i64::from_le_bytes(buffer[..COMPRESSED_FRAME_HEADER_LENGTH].try_into().unwrap());

        let header_crc24 = ((header >> 40) & 0xffffff) as i32;
        let computed_crc = crc24(&header.to_le_bytes()[..5]);

        if header_crc24 != computed_crc {
            return Err(create_header_crc_mismatch_error(computed_crc, header_crc24));
        }

        let compressed_length = (header & 0x1ffff) as usize;
        let compressed_payload_end = compressed_length + COMPRESSED_FRAME_HEADER_LENGTH;

        let frame_end = compressed_payload_end + FRAME_TRAILER_LENGTH;
        if buffer_len < frame_end {
            return Ok(None);
        }

        let compressed_payload_crc32 = u32::from_le_bytes(
            buffer[compressed_payload_end..frame_end]
                .try_into()
                .unwrap(),
        );

        let computed_crc = crc32(&buffer[COMPRESSED_FRAME_HEADER_LENGTH..compressed_payload_end]);

        if compressed_payload_crc32 != computed_crc {
            return Err(create_payload_crc_mismatch_error(
                computed_crc,
                compressed_payload_crc32,
            ));
        }

        let self_contained = (header & (1 << 34)) != 0;
        let uncompressed_length = ((header >> 17) & 0x1ffff) as usize;

        if uncompressed_length == 0 {
            // protocol spec 2.2:
            // An uncompressed length of 0 signals that the compressed payload should be used as-is
            // and not decompressed.
            let payload = buffer[COMPRESSED_FRAME_HEADER_LENGTH..compressed_payload_end].into();
            buffer.drain(..frame_end);

            return Ok(Some((self_contained, payload)));
        }

        let payload = decompress(
            &buffer[COMPRESSED_FRAME_HEADER_LENGTH..compressed_payload_end],
            uncompressed_length,
        )
        .map_err(|error| CompressionError::Lz4(io::Error::other(error)))?;

        if payload.len() != uncompressed_length {
            return Err(CompressionError::DecompressedLengthMismatch {
                expected: uncompressed_length,
                actual: payload.len(),
            }
            .into());
        }

        buffer.drain(..frame_end);
        Ok(Some((self_contained, payload)))
    }
}

/// Post-V5 decoder with support for envelope frames with CRC checksum.
#[derive(Clone, Debug, Default)]
pub struct UncompressedFrameDecoder {
    inner_decoder: GenericFrameDecoder,
}

impl FrameDecoder for UncompressedFrameDecoder {
    //noinspection DuplicatedCode
    #[inline]
    fn consume(&mut self, data: &mut Vec<u8>, _compression: Compression) -> Result<Vec<Envelope>> {
        self.inner_decoder.consume(data, Self::try_decode_frame)
    }
}

impl UncompressedFrameDecoder {
    fn try_decode_frame(buffer: &mut Vec<u8>) -> Result<Option<(bool, Vec<u8>)>> {
        let buffer_len = buffer.len();
        if buffer_len < UNCOMPRESSED_FRAME_HEADER_LENGTH {
            return Ok(None);
        }

        let header = if buffer_len >= 8 {
            i64::from_le_bytes(buffer[..8].try_into().unwrap()) & 0xffffffffffff
        } else {
            let mut header = 0;
            for (i, byte) in buffer[..UNCOMPRESSED_FRAME_HEADER_LENGTH]
                .iter()
                .enumerate()
            {
                header |= (*byte as i64) << (8 * i as i64);
            }

            header
        };

        let header_crc24 = ((header >> 24) & 0xffffff) as i32;
        let computed_crc = crc24(&header.to_le_bytes()[..3]);

        if header_crc24 != computed_crc {
            return Err(create_header_crc_mismatch_error(computed_crc, header_crc24));
        }

        let payload_length = (header & 0x1ffff) as usize;
        let payload_end = UNCOMPRESSED_FRAME_HEADER_LENGTH + payload_length;

        let frame_end = payload_end + FRAME_TRAILER_LENGTH;
        if buffer_len < frame_end {
            return Ok(None);
        }

        let payload_crc32 = u32::from_le_bytes(buffer[payload_end..frame_end].try_into().unwrap());

        let computed_crc = crc32(&buffer[UNCOMPRESSED_FRAME_HEADER_LENGTH..payload_end]);
        if payload_crc32 != computed_crc {
            return Err(create_payload_crc_mismatch_error(
                computed_crc,
                payload_crc32,
            ));
        }

        let self_contained = (header & (1 << 17)) != 0;

        let payload = buffer[UNCOMPRESSED_FRAME_HEADER_LENGTH..payload_end].into();
        buffer.drain(..frame_end);

        Ok(Some((self_contained, payload)))
    }
}

#[derive(Clone, Debug)]
struct GenericFrameDecoder {
    frame_buffer: Vec<u8>,
    payload_buffer: Vec<u8>,
    expected_payload_len: Option<usize>,
}

impl Default for GenericFrameDecoder {
    fn default() -> Self {
        Self {
            frame_buffer: Vec::with_capacity(MAX_FRAME_SIZE),
            payload_buffer: Vec::with_capacity(PAYLOAD_SIZE_LIMIT * 2),
            expected_payload_len: None,
        }
    }
}

impl GenericFrameDecoder {
    fn extract_non_self_contained_envelopes(&mut self) -> Result<Vec<Envelope>> {
        if let Some(expected_payload_len) = self.expected_payload_len {
            // The Cassandra wire format encodes the body length in bytes 5..9
            // of the envelope header (after version/flags/stream/opcode). The
            // FULL envelope on the wire is therefore ENVELOPE_HEADER_LEN bytes
            // of header plus expected_payload_len bytes of body, so the buffer
            // must contain at least that many bytes before we can decode it.
            // Without this header offset we would attempt to decode while the
            // body was still partial, lose the partial data on the buffer
            // truncation below, and mis-frame the next envelope.
            let total_envelope_len = ENVELOPE_HEADER_LEN + expected_payload_len;
            if self.payload_buffer.len() < total_envelope_len {
                return Ok(vec![]);
            }

            // Use extract_envelopes directly so we know exactly how many bytes
            // got consumed. drain(..consumed) preserves any trailing bytes
            // that may belong to the next envelope - they could legitimately
            // be there if a producer packed bytes from envelope N+1 into the
            // tail of the non-self-contained sequence for envelope N. The
            // previous code simply called clear() and silently lost them.
            let (consumed, envelopes) = extract_envelopes(&self.payload_buffer, Compression::None)?;
            self.payload_buffer.drain(..consumed);

            // Reset envelope-tracking state so the next call re-parses the
            // body length from whatever envelope header remains at the start
            // of the buffer (or waits for one if the buffer is empty / a
            // partial header). Without this reset the next sequence would be
            // gated against the previous envelope's length.
            self.expected_payload_len = None;
            return Ok(envelopes);
        }

        if let Some(expected_payload_len) = self.extract_expected_payload_len()? {
            self.expected_payload_len = Some(expected_payload_len);
            self.extract_non_self_contained_envelopes()
        } else {
            Ok(vec![])
        }
    }

    fn extract_expected_payload_len(&self) -> Result<Option<usize>> {
        if self.payload_buffer.len() < ENVELOPE_HEADER_LEN {
            return Ok(None);
        }

        let body_len = i32::from_be_bytes(self.payload_buffer[5..9].try_into().unwrap());
        checked_body_len(body_len).map(Some).ok_or_else(|| {
            ParseEnvelopeError::InvalidBodyLength(body_len)
                .to_string()
                .into()
        })
    }

    fn handle_frame(
        &mut self,
        envelopes: &mut Vec<Envelope>,
        self_contained: bool,
        frame: &mut Vec<u8>,
    ) -> Result<()> {
        if self_contained {
            if !self.payload_buffer.is_empty() {
                return Err(create_unexpected_self_contained_error());
            }

            envelopes.append(&mut try_decode_envelopes_without_spare_data(frame)?);
        } else {
            self.payload_buffer.append(frame);
            envelopes.append(&mut self.extract_non_self_contained_envelopes()?);
        }

        Ok(())
    }

    fn consume(
        &mut self,
        data: &mut Vec<u8>,
        try_decode_frame: impl Fn(&mut Vec<u8>) -> Result<Option<(bool, Vec<u8>)>>,
    ) -> Result<Vec<Envelope>> {
        let mut envelopes = vec![];

        if self.frame_buffer.is_empty() {
            // optimistic case
            while !data.is_empty() {
                if let Some((self_contained, mut frame)) = try_decode_frame(data)? {
                    self.handle_frame(&mut envelopes, self_contained, &mut frame)?;
                } else {
                    // we have some data, but not a full frame yet
                    self.frame_buffer.append(data);
                    break;
                }
            }
        } else {
            self.frame_buffer.append(data);

            while !self.frame_buffer.is_empty() {
                if let Some((self_contained, mut frame)) = try_decode_frame(&mut self.frame_buffer)?
                {
                    self.handle_frame(&mut envelopes, self_contained, &mut frame)?;
                } else {
                    break;
                }
            }
        }

        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::frame_encoder::{FrameEncoder, Lz4FrameEncoder, UncompressedFrameEncoder};
    use crate::frame::{Direction, Envelope, Flags, Opcode, Version, MAX_ENVELOPE_BODY_SIZE};

    // Build a body of `size` bytes filled with the supplied byte. We pick the
    // body size to be larger than PAYLOAD_SIZE_LIMIT so the encoder is forced
    // to emit non-self-contained frames.
    fn make_envelope(stream_id: i16, fill: u8, body_size: usize) -> Vec<u8> {
        Envelope {
            version: Version::V5,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id,
            body: vec![fill; body_size],
            tracing_id: None,
            warnings: vec![],
            custom_payload: None,
        }
        .encode_with(Compression::None)
        .unwrap()
    }

    // Encode one envelope (which is too large to fit in a single frame) as a
    // sequence of non-self-contained frames. Each frame has its own header and
    // CRC trailer so we can simply concatenate them on the wire.
    fn encode_as_non_self_contained(envelope: &[u8]) -> Vec<u8> {
        encode_as_non_self_contained_with(&mut UncompressedFrameEncoder::default(), envelope)
    }

    fn encode_as_non_self_contained_with(
        encoder: &mut dyn FrameEncoder,
        envelope: &[u8],
    ) -> Vec<u8> {
        let mut wire = vec![];
        let mut start = 0;
        while start < envelope.len() {
            let (consumed, frame) = encoder.finalize_non_self_contained(&envelope[start..]);
            wire.extend_from_slice(frame);
            start += consumed;
            encoder.reset();
        }
        wire
    }

    #[test]
    fn decoder_recovers_two_consecutive_non_self_contained_envelopes() {
        // Use a body just over PAYLOAD_SIZE_LIMIT so each envelope spans two
        // frames; the second envelope is deliberately a different (smaller)
        // size to expose any stale `expected_payload_len` carryover.
        let envelope_a = make_envelope(1, 0xAA, PAYLOAD_SIZE_LIMIT + 100);
        let envelope_b = make_envelope(2, 0xBB, PAYLOAD_SIZE_LIMIT + 50);

        let mut wire = encode_as_non_self_contained(&envelope_a);
        wire.extend_from_slice(&encode_as_non_self_contained(&envelope_b));

        let mut decoder = UncompressedFrameDecoder::default();
        let envelopes = decoder
            .consume(&mut wire, Compression::None)
            .expect("decoder must accept two consecutive non-self-contained envelopes");

        // we expect to recover both envelopes intact, in order
        assert_eq!(envelopes.len(), 2, "should decode both envelopes");
        assert_eq!(envelopes[0].stream_id, 1);
        assert_eq!(envelopes[0].body, vec![0xAA; PAYLOAD_SIZE_LIMIT + 100]);
        assert_eq!(envelopes[1].stream_id, 2);
        assert_eq!(envelopes[1].body, vec![0xBB; PAYLOAD_SIZE_LIMIT + 50]);
    }

    // The reviewer pointed out a defensive gap: when payload_buffer holds a
    // complete envelope plus the start of the next envelope (because a
    // hypothetical producer packed bytes across envelope boundaries inside
    // non-self-contained frames), the previous code called clear() on the
    // buffer after decoding the first envelope, losing the trailing bytes
    // that begin the next envelope.
    //
    // We construct that exact scenario by hand-packing one non-self-contained
    // frame whose payload is `envelope_a + envelope_b[..partial]`, followed
    // by another non-self-contained frame carrying `envelope_b[partial..]`.
    // A correct decoder must reconstruct both envelopes; a buggy one drops
    // envelope_b's prefix on `clear()` and then fails to parse the second
    // envelope from a misaligned start.
    #[test]
    fn decoder_preserves_trailing_bytes_across_non_self_contained_frames() {
        let envelope_a = make_envelope(1, 0xAA, 100);
        let envelope_b = make_envelope(2, 0xBB, 200);

        // Frame 1 carries envelope_a in full PLUS the first 100 bytes of
        // envelope_b. Both fit comfortably under PAYLOAD_SIZE_LIMIT, so
        // finalize_non_self_contained packs them into a single frame.
        let half_b = 100usize;
        let mut packed = envelope_a.clone();
        packed.extend_from_slice(&envelope_b[..half_b]);

        let mut encoder = UncompressedFrameEncoder::default();
        let (consumed, frame1_slice) = encoder.finalize_non_self_contained(&packed);
        assert_eq!(
            consumed,
            packed.len(),
            "test setup: whole packed slice must fit"
        );
        let frame1: Vec<u8> = frame1_slice.to_vec();
        encoder.reset();

        // Frame 2 carries the remaining bytes of envelope_b.
        let (_, frame2_slice) = encoder.finalize_non_self_contained(&envelope_b[half_b..]);
        let frame2: Vec<u8> = frame2_slice.to_vec();

        let mut wire = frame1;
        wire.extend_from_slice(&frame2);

        let mut decoder = UncompressedFrameDecoder::default();
        let envelopes = decoder
            .consume(&mut wire, Compression::None)
            .expect("decoder must accept the cross-boundary packed frames");

        assert_eq!(envelopes.len(), 2, "both envelopes must be recovered");
        assert_eq!(envelopes[0].stream_id, 1);
        assert_eq!(envelopes[0].body, vec![0xAA; 100]);
        assert_eq!(envelopes[1].stream_id, 2);
        assert_eq!(envelopes[1].body, vec![0xBB; 200]);
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

    fn make_incompressible_envelope(body_size: usize) -> Vec<u8> {
        Envelope {
            version: Version::V5,
            direction: Direction::Request,
            flags: Flags::empty(),
            opcode: Opcode::Query,
            stream_id: 1,
            body: incompressible_bytes(body_size),
            tracing_id: None,
            warnings: vec![],
            custom_payload: None,
        }
        .encode_with(Compression::None)
        .unwrap()
    }

    fn make_lz4_frame(payload: &[u8], uncompressed_length: usize, self_contained: bool) -> Vec<u8> {
        let mut header = payload.len() as u64 | ((uncompressed_length as u64) << 17);
        if self_contained {
            header |= 1 << 34;
        }
        header |= (crc24(&header.to_le_bytes()[..5]) as u64) << 40;

        let mut frame = header.to_le_bytes().to_vec();
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&crc32(payload).to_le_bytes());
        frame
    }

    #[test]
    fn lz4_decoder_rejects_uncompressed_length_mismatch() {
        let envelope = make_envelope(1, 0xAA, 100);
        let compressed = lz4_flex::compress(&envelope);

        let declared_length = envelope.len() + 50;
        let mut wire = make_lz4_frame(&compressed, declared_length, true);
        let result = Lz4FrameDecoder::default().consume(&mut wire, Compression::None);
        assert!(
            matches!(
                result,
                Err(Error::Compression(CompressionError::DecompressedLengthMismatch {
                    expected,
                    actual,
                })) if expected == declared_length && actual == envelope.len()
            ),
            "{:?}",
            result
        );

        // a declared length below the real one leaves lz4 without room for the output
        let mut wire = make_lz4_frame(&compressed, envelope.len() - 50, true);
        let result = Lz4FrameDecoder::default().consume(&mut wire, Compression::None);
        assert!(result.is_err(), "{:?}", result);

        let mut wire = make_lz4_frame(&compressed, envelope.len(), true);
        let envelopes = Lz4FrameDecoder::default()
            .consume(&mut wire, Compression::None)
            .unwrap();
        assert_eq!(envelopes.len(), 1);
    }

    #[test]
    fn decoders_keep_read_buffer_capacity() {
        let small_envelope = make_envelope(1, 0xAA, 100);

        let mut encoder = UncompressedFrameEncoder::default();
        encoder.add_envelope(small_envelope.clone());
        let uncompressed_frame = encoder.finalize_self_contained().to_vec();

        let mut encoder = Lz4FrameEncoder::default();
        encoder.add_envelope(small_envelope);
        let compressed_frame = encoder.finalize_self_contained().to_vec();

        let mut encoder = Lz4FrameEncoder::default();
        let (_, frame) =
            encoder.finalize_non_self_contained(&make_incompressible_envelope(PAYLOAD_SIZE_LIMIT));
        let stored_frame = frame.to_vec();

        let stored_header = u64::from_le_bytes(stored_frame[..8].try_into().unwrap());
        assert_eq!(
            (stored_header >> 17) & 0x1ffff,
            0,
            "test setup: payload must be stored uncompressed"
        );

        for leftover in [0, 10] {
            let cases: Vec<(Box<dyn FrameDecoder>, &[u8])> = vec![
                (
                    Box::<UncompressedFrameDecoder>::default(),
                    &uncompressed_frame,
                ),
                (Box::<Lz4FrameDecoder>::default(), &compressed_frame),
                (Box::<Lz4FrameDecoder>::default(), &stored_frame),
            ];

            for (mut decoder, frame) in cases {
                let mut buffer = Vec::with_capacity(2 * MAX_FRAME_SIZE);
                buffer.extend_from_slice(frame);
                buffer.extend_from_slice(&frame[..leftover]);
                let capacity = buffer.capacity();

                decoder.consume(&mut buffer, Compression::None).unwrap();

                assert!(buffer.is_empty());
                assert_eq!(buffer.capacity(), capacity, "leftover: {}", leftover);
            }
        }
    }

    // Frames from the Apache Cassandra Java driver test suite (BytesToSegmentDecoderTest).
    #[test]
    fn decoders_accept_reference_frames() {
        let mut frame = vec![
            0x04, 0x00, 0x02, 0x01, 0xf9, 0xf2, 0, 0, 0, 1, 0x1f, 0xd6, 0x02, 0x2d,
        ];
        assert_eq!(
            UncompressedFrameDecoder::try_decode_frame(&mut frame).unwrap(),
            Some((true, vec![0, 0, 0, 1]))
        );
        assert!(frame.is_empty());

        let mut frame = vec![0xff, 0xff, 0x03, 0x25, 0x40, 0x47];
        frame.resize(UNCOMPRESSED_FRAME_HEADER_LENGTH + 0x1ffff, 1);
        frame.extend_from_slice(&[0xa0, 0x5c, 0x2f, 0x13]);
        assert_eq!(
            UncompressedFrameDecoder::try_decode_frame(&mut frame).unwrap(),
            Some((true, vec![1; 0x1ffff]))
        );

        let mut frame = vec![
            0x12, 0x00, 0x20, 0x00, 0x04, 0x91, 0xc9, 0x4f, // header
            0xf0, 0x01, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, // payload
            0x2b, 0xd6, 0x7f, 0x90, // trailer
        ];
        assert_eq!(
            Lz4FrameDecoder::try_decode_frame(&mut frame).unwrap(),
            Some((true, vec![0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1]))
        );

        let mut wrong_header = vec![
            0x04, 0x00, 0x02, 0x02, 0xf9, 0xf2, 0, 0, 0, 1, 0x1f, 0xd6, 0x02, 0x2d,
        ];
        assert!(UncompressedFrameDecoder::try_decode_frame(&mut wrong_header).is_err());

        let mut wrong_trailer = vec![
            0x04, 0x00, 0x02, 0x01, 0xf9, 0xf2, 0, 0, 0, 1, 0x1f, 0xd6, 0x02, 0x2e,
        ];
        assert!(UncompressedFrameDecoder::try_decode_frame(&mut wrong_trailer).is_err());
    }

    #[test]
    fn lz4_decoder_round_trips_frames_fed_in_chunks() {
        let small_envelopes: Vec<Vec<u8>> = (0..3)
            .map(|i| make_envelope(i, 0xa0 + i as u8, 1000))
            .collect();

        let mut encoder = Lz4FrameEncoder::default();
        for envelope in &small_envelopes {
            assert!(encoder.can_fit(envelope.len()));
            encoder.add_envelope(envelope.clone());
        }
        let mut wire = encoder.finalize_self_contained().to_vec();

        let header = u64::from_le_bytes(wire[..8].try_into().unwrap());
        assert!(
            header & 0x1ffff < (header >> 17) & 0x1ffff,
            "test setup: payload must be compressed"
        );

        let large_envelope = make_envelope(3, 0xcc, 2 * PAYLOAD_SIZE_LIMIT);
        wire.extend_from_slice(&encode_as_non_self_contained_with(
            &mut Lz4FrameEncoder::default(),
            &large_envelope,
        ));

        let mut decoder = Lz4FrameDecoder::default();
        let mut envelopes = vec![];
        for chunk in wire.chunks(1000) {
            let mut data = chunk.to_vec();
            envelopes.extend(decoder.consume(&mut data, Compression::None).unwrap());
            assert!(data.is_empty());
        }

        assert_eq!(envelopes.len(), 4);
        for (i, envelope) in envelopes.iter().take(3).enumerate() {
            assert_eq!(envelope.stream_id, i as i16);
            assert_eq!(envelope.body, vec![0xa0 + i as u8; 1000]);
        }
        assert_eq!(envelopes[3].stream_id, 3);
        assert_eq!(envelopes[3].body, vec![0xcc; 2 * PAYLOAD_SIZE_LIMIT]);
    }

    fn self_contained_frames() -> (Vec<u8>, Vec<u8>) {
        let envelope = make_envelope(1, 0xaa, 100);

        let mut encoder = UncompressedFrameEncoder::default();
        encoder.add_envelope(envelope.clone());
        let uncompressed = encoder.finalize_self_contained().to_vec();

        let mut encoder = Lz4FrameEncoder::default();
        encoder.add_envelope(envelope);
        let compressed = encoder.finalize_self_contained().to_vec();

        (uncompressed, compressed)
    }

    fn assert_corruption_detected(
        new_decoder: impl Fn() -> Box<dyn FrameDecoder>,
        frame: &[u8],
        positions: std::ops::Range<usize>,
        expected_error: &str,
    ) {
        for position in positions {
            for bit in 0..8 {
                let mut wire = frame.to_vec();
                wire[position] ^= 1 << bit;

                let error = new_decoder()
                    .consume(&mut wire, Compression::None)
                    .unwrap_err();
                assert!(
                    error.to_string().contains(expected_error),
                    "byte {} bit {}: {}",
                    position,
                    bit,
                    error
                );
            }
        }
    }

    #[test]
    fn decoders_reject_corrupted_header_crc() {
        let (uncompressed, compressed) = self_contained_frames();

        assert_corruption_detected(
            || Box::<UncompressedFrameDecoder>::default(),
            &uncompressed,
            0..UNCOMPRESSED_FRAME_HEADER_LENGTH,
            "Header CRC mismatch",
        );
        assert_corruption_detected(
            || Box::<Lz4FrameDecoder>::default(),
            &compressed,
            0..COMPRESSED_FRAME_HEADER_LENGTH,
            "Header CRC mismatch",
        );
    }

    #[test]
    fn decoders_reject_corrupted_payload_crc() {
        let (uncompressed, compressed) = self_contained_frames();

        assert_corruption_detected(
            || Box::<UncompressedFrameDecoder>::default(),
            &uncompressed,
            UNCOMPRESSED_FRAME_HEADER_LENGTH..uncompressed.len(),
            "Payload CRC mismatch",
        );
        assert_corruption_detected(
            || Box::<Lz4FrameDecoder>::default(),
            &compressed,
            COMPRESSED_FRAME_HEADER_LENGTH..compressed.len(),
            "Payload CRC mismatch",
        );
    }

    #[test]
    fn decoders_reject_truncated_envelope_in_self_contained_frame() {
        let envelope = make_envelope(1, 0xAA, 100);

        for truncated_len in [5, 50] {
            let mut payload = envelope.clone();
            payload.extend_from_slice(&envelope[..truncated_len]);

            let mut encoder = UncompressedFrameEncoder::default();
            encoder.add_envelope(payload.clone());
            let mut wire = encoder.finalize_self_contained().to_vec();

            let result = UncompressedFrameDecoder::default().consume(&mut wire, Compression::None);
            assert!(
                result.is_err(),
                "truncated to {}: {:?}",
                truncated_len,
                result
            );

            let mut encoder = Lz4FrameEncoder::default();
            encoder.add_envelope(payload);
            let mut wire = encoder.finalize_self_contained().to_vec();

            let result = Lz4FrameDecoder::default().consume(&mut wire, Compression::None);
            assert!(
                result.is_err(),
                "truncated to {}: {:?}",
                truncated_len,
                result
            );
        }
    }

    #[test]
    fn decoders_reject_invalid_envelope_body_length() {
        for body_len in [-1, -10, MAX_ENVELOPE_BODY_SIZE as i32 + 1] {
            let mut envelope_header = vec![u8::from(Version::V5), 0, 0, 1, u8::from(Opcode::Query)];
            envelope_header.extend_from_slice(&body_len.to_be_bytes());

            let mut encoder = UncompressedFrameEncoder::default();
            let (_, frame) = encoder.finalize_non_self_contained(&envelope_header);
            let mut wire = frame.to_vec();

            let result = UncompressedFrameDecoder::default().consume(&mut wire, Compression::None);
            assert!(result.is_err(), "body length {}: {:?}", body_len, result);

            let mut wire = envelope_header.clone();
            let result = LegacyFrameDecoder::default().consume(&mut wire, Compression::None);
            assert!(result.is_err(), "body length {}: {:?}", body_len, result);
        }
    }
}
