use std::convert::TryFrom;
use std::io;
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;

use cassandra_protocol::compression::Compression;
use cassandra_protocol::error;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::{
    Direction, Envelope, Flags, Opcode, Version, LENGTH_LEN, STREAM_LEN,
};
use cassandra_protocol::types::data_serialization_types::decode_timeuuid;
use cassandra_protocol::types::{
    from_cursor_string_list, try_i16_from_bytes, try_i32_from_bytes, UUID_LEN,
};

// Cassandra's documented hard cap for an envelope body. Pre-V5 (unframed)
// connections can in principle send anything up to 256 MiB. Using this as the
// upper bound prevents a hostile or malfunctioning server from making us
// allocate gigabytes from a single 4-byte length field.
const MAX_ENVELOPE_BODY_SIZE: usize = 256 * 1024 * 1024;

async fn parse_raw_envelope<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
) -> error::Result<Envelope> {
    let mut version_bytes = [0; Version::BYTE_LENGTH];
    let mut flag_bytes = [0; Flags::BYTE_LENGTH];
    let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes = [0; LENGTH_LEN];

    // NOTE: order of reads matters
    cursor.read_exact(&mut version_bytes).await?;
    cursor.read_exact(&mut flag_bytes).await?;
    cursor.read_exact(&mut stream_bytes).await?;
    cursor.read_exact(&mut opcode_bytes).await?;
    cursor.read_exact(&mut length_bytes).await?;

    let version = Version::try_from(version_bytes[0])?;
    let direction = Direction::from(version_bytes[0]);
    let flags = Flags::from_bits_truncate(flag_bytes[0]);
    let stream_id = try_i16_from_bytes(&stream_bytes)?;
    let opcode = Opcode::try_from(opcode_bytes[0])?;

    // The wire format encodes the body length as a signed 32-bit int. Without
    // validation a negative value would wrap around to a multi-gigabyte usize,
    // and even a legitimate large positive value would happily allocate up to
    // 2 GiB before reading any body bytes. Reject both before allocating.
    let length_signed = try_i32_from_bytes(&length_bytes)?;
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

    let mut body_bytes = vec![0; length];

    cursor.read_exact(&mut body_bytes).await?;

    let full_body = if flags.contains(Flags::COMPRESSION) {
        compressor.decode(body_bytes)?
    } else {
        Compression::None.decode(body_bytes)?
    };

    let body_len = full_body.len();

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    // The TRACING flag has different semantics in each direction: in a request
    // it is the client asking the server to enable tracing; in a response it
    // signals that the body starts with a 16-byte tracing UUID. Reading that
    // UUID on the wrong direction (which the previous code did) would silently
    // consume the first 16 bytes of a request body. Match the canonical
    // Envelope::from_buffer parser by gating on direction too.
    let tracing_id = if flags.contains(Flags::TRACING) && direction == Direction::Response {
        let mut tracing_bytes = [0; UUID_LEN];
        std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes)?;

        decode_timeuuid(&tracing_bytes).ok()
    } else {
        None
    };

    let warnings = if flags.contains(Flags::WARNING) {
        from_cursor_string_list(&mut body_cursor)?
    } else {
        vec![]
    };

    let mut body = Vec::with_capacity(body_len - body_cursor.position() as usize);

    std::io::Read::read_to_end(&mut body_cursor, &mut body)?;

    let envelope = Envelope {
        version,
        direction,
        flags,
        opcode,
        stream_id,
        body,
        tracing_id,
        warnings,
    };

    Ok(envelope)
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
    use cassandra_protocol::frame::Version;

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
}
