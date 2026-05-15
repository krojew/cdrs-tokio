use crate::compression::{Compression, CompressionError};
use crate::crc::{crc24, crc32};
use crate::error::{Error, Result};
use crate::frame::{
    Envelope, ParseEnvelopeError, COMPRESSED_FRAME_HEADER_LENGTH, ENVELOPE_HEADER_LEN,
    FRAME_TRAILER_LENGTH, MAX_FRAME_SIZE, PAYLOAD_SIZE_LIMIT, UNCOMPRESSED_FRAME_HEADER_LENGTH,
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
    let (_, envelopes) = extract_envelopes(buffer, Compression::None)?;
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
            *buffer = buffer.split_off(frame_end);

            return Ok(Some((self_contained, payload)));
        }

        decompress(
            &buffer[COMPRESSED_FRAME_HEADER_LENGTH..compressed_payload_end],
            uncompressed_length,
        )
        .map_err(|error| CompressionError::Lz4(io::Error::other(error)).into())
        .map(|payload| {
            *buffer = buffer.split_off(frame_end);
            Some((self_contained, payload))
        })
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
        *buffer = buffer.split_off(frame_end);

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
            // body was still partial, lose the partial data on `clear()`, and
            // mis-frame the next envelope.
            let total_envelope_len = ENVELOPE_HEADER_LEN + expected_payload_len;
            if self.payload_buffer.len() < total_envelope_len {
                return Ok(vec![]);
            }

            let envelopes = try_decode_envelopes_without_spare_data(&self.payload_buffer)?;

            // Reset state for the next envelope sequence. Failing to clear
            // expected_payload_len here meant the previous envelope's size
            // was used to gate the next one, causing wrong-sized reads or
            // silent data loss when sizes differed.
            self.payload_buffer.clear();
            self.expected_payload_len = None;
            return Ok(envelopes);
        }

        if let Some(expected_payload_len) = self.extract_expected_payload_len() {
            self.expected_payload_len = Some(expected_payload_len);
            self.extract_non_self_contained_envelopes()
        } else {
            Ok(vec![])
        }
    }

    fn extract_expected_payload_len(&self) -> Option<usize> {
        if self.payload_buffer.len() < ENVELOPE_HEADER_LEN {
            return None;
        }

        Some(i32::from_be_bytes(self.payload_buffer[5..9].try_into().unwrap()) as usize)
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
    use crate::frame::frame_encoder::{FrameEncoder, UncompressedFrameEncoder};
    use crate::frame::{Direction, Envelope, Flags, Opcode, Version};

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
        }
        .encode_with(Compression::None)
        .unwrap()
    }

    // Encode one envelope (which is too large to fit in a single frame) as a
    // sequence of non-self-contained frames. Each frame has its own header and
    // CRC trailer so we can simply concatenate them on the wire.
    fn encode_as_non_self_contained(envelope: &[u8]) -> Vec<u8> {
        let mut encoder = UncompressedFrameEncoder::default();
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
}
