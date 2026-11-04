use std::convert::TryFrom;
use std::io;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;

use cassandra_protocol::compression::Compression;
use cassandra_protocol::error;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::{
    Envelope, Flags, Opcode, ParseEnvelopeError, Version, LENGTH_LEN, MAX_ENVELOPE_BODY_SIZE,
    STREAM_LEN,
};
use cassandra_protocol::types::try_i32_from_bytes;

const OPCODE_POS: usize = Version::BYTE_LENGTH + Flags::BYTE_LENGTH + STREAM_LEN;
const ENVELOPE_HEADER_LEN: usize = OPCODE_POS + Opcode::BYTE_LENGTH + LENGTH_LEN;

async fn parse_raw_envelope<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
) -> error::Result<Envelope> {
    let mut envelope = vec![0; ENVELOPE_HEADER_LEN];
    cursor.read_exact(&mut envelope).await?;

    // reject garbage before waiting for a body which might never arrive
    Version::try_from(envelope[0])?;
    Opcode::try_from(envelope[OPCODE_POS])?;

    // The wire format encodes the body length as a signed 32-bit int. Without
    // validation a negative value would wrap around to a multi-gigabyte usize,
    // and even a legitimate large positive value would happily allocate up to
    // 2 GiB before reading any body bytes. Reject both before allocating.
    let length_signed = try_i32_from_bytes(&envelope[ENVELOPE_HEADER_LEN - LENGTH_LEN..])?;
    if length_signed < 0 {
        return Err(error::Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("negative envelope body length {length_signed}"),
        )));
    }
    let length = length_signed as usize;
    if length > MAX_ENVELOPE_BODY_SIZE {
        return Err(error::Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("envelope body length {length} exceeds maximum {MAX_ENVELOPE_BODY_SIZE}"),
        )));
    }

    envelope.resize(ENVELOPE_HEADER_LEN + length, 0);
    cursor
        .read_exact(&mut envelope[ENVELOPE_HEADER_LEN..])
        .await?;

    Envelope::from_buffer(&envelope, compressor)
        .map(|parsed| parsed.envelope)
        .map_err(|error| match error {
            ParseEnvelopeError::DecompressionError(error) => error.into(),
            error => error::Error::General(error.to_string()),
        })
}

pub async fn parse_envelope<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
    addr: SocketAddr,
) -> error::Result<Envelope> {
    let envelope = parse_raw_envelope(cursor, compressor).await?;
    convert_envelope_into_result(envelope, addr)
}

pub(crate) fn convert_envelope_into_result(
    envelope: Envelope,
    addr: SocketAddr,
) -> error::Result<Envelope> {
    match envelope.opcode {
        Opcode::Error => envelope.response_body().and_then(|err| match err {
            ResponseBody::Error(err) => Err(error::Error::Server { body: err, addr }),
            // ResponseBody::try_from is expected to always return Error for
            // Opcode::Error, but if a future protocol change ever broke that
            // invariant we don't want to crash the reader task with a panic
            // - surface it as a normal error instead.
            other => Err(error::Error::General(format!(
                "Expected ResponseBody::Error for Opcode::Error envelope, got {other:?}"
            ))),
        }),
        _ => Ok(envelope),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_protocol::frame::{Direction, Version};

    // Build a minimal valid envelope header byte sequence with the supplied
    // 4-byte length field. The header layout is:
    //   1 byte version | 1 byte flags | 2 bytes stream | 1 byte opcode | 4 bytes length
    fn header_with_length(length_bytes: [u8; 4]) -> Vec<u8> {
        let mut buf = vec![
            u8::from(Version::V4), // version
            0,                     // flags
            0,
            0,                       // stream id
            u8::from(Opcode::Ready), // opcode (any valid one will do)
        ];
        buf.extend_from_slice(&length_bytes);
        buf
    }

    #[tokio::test]
    async fn parse_envelope_rejects_negative_body_length() {
        // a length field of 0xFFFFFFFF reads as -1 in i32. Without validation
        // this would be cast to usize::MAX-ish and immediately OOM the process.
        let mut payload = header_with_length([0xff, 0xff, 0xff, 0xff]);
        let mut cursor = std::io::Cursor::new(&mut payload);
        let mut bytes = vec![];
        tokio::io::AsyncReadExt::read_to_end(&mut cursor, &mut bytes)
            .await
            .unwrap();

        let mut reader = bytes.as_slice();
        assert!(parse_raw_envelope(&mut reader, Compression::None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn parse_envelope_rejects_oversized_body_length() {
        // i32::MAX (~2 GiB) is well past any plausible Cassandra envelope.
        // We expect a clean error rather than allocation of a giant buffer
        // up-front before even reading the body.
        let payload = header_with_length(i32::MAX.to_be_bytes());

        let mut reader = payload.as_slice();
        assert!(parse_raw_envelope(&mut reader, Compression::None)
            .await
            .is_err());
    }

    // Per the Cassandra protocol spec, the TRACING flag in a REQUEST asks the
    // server to enable tracing and carries no payload metadata. The
    // tracing_id UUID only appears in RESPONSES. parse_raw_envelope used to
    // attempt to read 16 bytes of tracing UUID whenever the TRACING flag was
    // set regardless of direction, which is inconsistent with the canonical
    // Envelope::from_buffer parser and would corrupt the body on a request.
    #[tokio::test]
    async fn parse_envelope_does_not_read_tracing_id_for_request_direction() {
        // Build a request envelope (direction bit clear in byte 0) with
        // TRACING set and exactly 16 bytes of body. After parsing, the body
        // must come back intact - no bytes consumed as a tracing UUID.
        let body: Vec<u8> = (0..16u8).collect();
        let mut wire = vec![
            // version 4, direction = Request (0x80 bit clear)
            u8::from(Version::V4),
            // TRACING flag (0x02)
            Flags::TRACING.bits(),
            // stream id
            0,
            0,
            // opcode - any valid request opcode (Query)
            u8::from(Opcode::Query),
        ];
        wire.extend_from_slice(&(body.len() as i32).to_be_bytes());
        wire.extend_from_slice(&body);

        let mut reader = wire.as_slice();
        let envelope = parse_raw_envelope(&mut reader, Compression::None)
            .await
            .expect("a request envelope with TRACING flag must still parse");

        assert_eq!(envelope.direction, Direction::Request);
        assert!(
            envelope.tracing_id.is_none(),
            "request envelopes must not carry a tracing UUID"
        );
        assert_eq!(
            envelope.body, body,
            "request body should be preserved verbatim, got {:?}",
            envelope.body
        );
    }

    #[tokio::test]
    async fn parse_envelope_rejects_unknown_version_before_reading_body() {
        let mut wire = header_with_length(100i32.to_be_bytes());
        wire[0] = 0x48;

        let mut reader = wire.as_slice();
        let result = parse_raw_envelope(&mut reader, Compression::None).await;
        assert!(
            matches!(result, Err(error::Error::General(_))),
            "{:?}",
            result
        );
    }

    #[tokio::test]
    async fn parse_envelope_separates_custom_payload_from_body() {
        let custom_payload = [0, 1, 0, 1, 107, 0, 0, 0, 1, 42]; // {"k": [42]}

        let mut body = vec![0, 1, 0, 2, 104, 105]; // warnings ["hi"]
        body.extend_from_slice(&custom_payload);
        body.extend_from_slice(&[0, 0, 0, 1]);

        let mut wire = vec![
            u8::from(Version::V4) | u8::from(Direction::Response),
            (Flags::WARNING | Flags::CUSTOM_PAYLOAD).bits(),
            0,
            0,
            u8::from(Opcode::Result),
        ];
        wire.extend_from_slice(&(body.len() as i32).to_be_bytes());
        wire.extend_from_slice(&body);

        let mut reader = wire.as_slice();
        let envelope = parse_raw_envelope(&mut reader, Compression::None)
            .await
            .unwrap();

        assert_eq!(envelope.warnings, vec!["hi".to_string()]);
        assert_eq!(envelope.body, vec![0, 0, 0, 1]);
        assert_eq!(envelope.encode_with(Compression::None).unwrap(), wire);
        assert!(reader.is_empty());
    }
}
