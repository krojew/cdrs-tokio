use std::convert::TryFrom;
use std::io::Cursor;
use tokio::io::AsyncReadExt;

use cassandra_protocol::compression::Compression;
use cassandra_protocol::error;
use cassandra_protocol::frame::frame_response::ResponseBody;
use cassandra_protocol::frame::{
    Direction, Flags, Frame, FromCursor, Opcode, Version, LENGTH_LEN, STREAM_LEN,
};
use cassandra_protocol::types::data_serialization_types::decode_timeuuid;
use cassandra_protocol::types::{try_i16_from_bytes, try_i32_from_bytes, CStringList, UUID_LEN};

async fn parse_raw_frame<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
) -> error::Result<Frame> {
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
    let stream = try_i16_from_bytes(&stream_bytes)?;
    let opcode = Opcode::try_from(opcode_bytes[0])?;
    let length = try_i32_from_bytes(&length_bytes)? as usize;

    let mut body_bytes = vec![0; length];

    cursor.read_exact(&mut body_bytes).await?;

    let full_body = if flags.contains(Flags::COMPRESSION) {
        compressor.decode(body_bytes)?
    } else {
        Compression::None.decode(body_bytes)?
    };

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    let tracing_id = if flags.contains(Flags::TRACING) {
        let mut tracing_bytes = vec![0; UUID_LEN];
        std::io::Read::read_exact(&mut body_cursor, &mut tracing_bytes)?;

        decode_timeuuid(tracing_bytes.as_slice()).ok()
    } else {
        None
    };

    let warnings = if flags.contains(Flags::WARNING) {
        CStringList::from_cursor(&mut body_cursor)?.into_plain()
    } else {
        vec![]
    };

    let mut body = vec![];

    std::io::Read::read_to_end(&mut body_cursor, &mut body)?;

    let frame = Frame {
        version,
        direction,
        flags,
        opcode,
        stream,
        body,
        tracing_id,
        warnings,
    };

    Ok(frame)
}

pub async fn parse_frame<T: AsyncReadExt + Unpin>(
    cursor: &mut T,
    compressor: Compression,
) -> error::Result<Frame> {
    convert_frame_into_result(parse_raw_frame(cursor, compressor).await?)
}

fn convert_frame_into_result(frame: Frame) -> error::Result<Frame> {
    match frame.opcode {
        Opcode::Error => frame.body_response().and_then(|err| match err {
            ResponseBody::Error(err) => Err(error::Error::Server(err)),
            _ => unreachable!(),
        }),
        _ => Ok(frame),
    }
}
