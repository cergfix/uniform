use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::constants::*;

/// Read a MySQL packet from the stream.
/// Returns (sequence_id, payload).
pub async fn read_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), String> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|e| format!("read header: {}", e))?;

    let payload_len =
        (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq_id = header[3];

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| format!("read payload: {}", e))?;
    }

    Ok((seq_id, payload))
}

/// Write a MySQL packet to the stream.
pub async fn write_packet(
    stream: &mut TcpStream,
    seq_id: u8,
    payload: &[u8],
) -> Result<(), String> {
    let len = payload.len();
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq_id,
    ];
    stream
        .write_all(&header)
        .await
        .map_err(|e| format!("write header: {}", e))?;
    stream
        .write_all(payload)
        .await
        .map_err(|e| format!("write payload: {}", e))?;
    Ok(())
}

/// Append a MySQL packet (4-byte header + payload) to an in-memory buffer.
pub fn append_packet(buf: &mut Vec<u8>, seq_id: u8, payload: &[u8]) {
    let len = payload.len();
    buf.push((len & 0xff) as u8);
    buf.push(((len >> 8) & 0xff) as u8);
    buf.push(((len >> 16) & 0xff) as u8);
    buf.push(seq_id);
    buf.extend_from_slice(payload);
}

/// Build the initial handshake packet (sent by server to client).
pub fn build_handshake(connection_id: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Protocol version
    buf.push(10);

    // Server version (null-terminated)
    buf.extend_from_slice(SERVER_VERSION.as_bytes());
    buf.push(0);

    // Connection ID (4 bytes, little-endian)
    buf.extend_from_slice(&connection_id.to_le_bytes());

    // Auth data part 1 (8 bytes) — dummy scramble
    buf.extend_from_slice(&[0x3a, 0x23, 0x3d, 0x4e, 0x6a, 0x7b, 0x25, 0x56]);

    // Filler
    buf.push(0);

    // Capability flags (lower 2 bytes)
    let capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    buf.push((capabilities & 0xff) as u8);
    buf.push(((capabilities >> 8) & 0xff) as u8);

    // Character set (utf8mb4 = 45)
    buf.push(45);

    // Status flags
    buf.push((SERVER_STATUS_AUTOCOMMIT & 0xff) as u8);
    buf.push(((SERVER_STATUS_AUTOCOMMIT >> 8) & 0xff) as u8);

    // Capability flags (upper 2 bytes)
    buf.push(((capabilities >> 16) & 0xff) as u8);
    buf.push(((capabilities >> 24) & 0xff) as u8);

    // Auth data length
    buf.push(21); // length of auth plugin data

    // Reserved (10 bytes of 0)
    buf.extend_from_slice(&[0; 10]);

    // Auth data part 2 (13 bytes, including null terminator)
    buf.extend_from_slice(&[
        0x5a, 0x6b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x12, 0x23, 0x34, 0x45, 0x00,
    ]);

    // Auth plugin name (null-terminated)
    buf.extend_from_slice(MYSQL_NATIVE_PASSWORD.as_bytes());
    buf.push(0);

    buf
}

/// Build an OK packet.
pub fn build_ok_packet(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(OK_HEADER);
    write_length_encoded_int(&mut buf, affected_rows);
    write_length_encoded_int(&mut buf, last_insert_id);
    // Status flags
    buf.push((SERVER_STATUS_AUTOCOMMIT & 0xff) as u8);
    buf.push(((SERVER_STATUS_AUTOCOMMIT >> 8) & 0xff) as u8);
    // Warnings
    buf.push(0);
    buf.push(0);
    buf
}

/// Build an ERR packet.
pub fn build_err_packet(error_code: u16, message: &str) -> Vec<u8> {
    let mut buf = vec![
        ERR_HEADER,
        (error_code & 0xff) as u8,
        ((error_code >> 8) & 0xff) as u8,
        b'#',
    ];
    buf.extend_from_slice(b"HY000");
    buf.extend_from_slice(message.as_bytes());
    buf
}

/// Build an EOF packet.
pub fn build_eof_packet() -> Vec<u8> {
    vec![
        EOF_HEADER,
        0,
        0,
        (SERVER_STATUS_AUTOCOMMIT & 0xff) as u8,
        ((SERVER_STATUS_AUTOCOMMIT >> 8) & 0xff) as u8,
    ]
}

/// Write a length-encoded integer.
pub fn write_length_encoded_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.push(val as u8);
    } else if val < (1 << 16) {
        buf.push(0xfc);
        buf.push((val & 0xff) as u8);
        buf.push(((val >> 8) & 0xff) as u8);
    } else if val < (1 << 24) {
        buf.push(0xfd);
        buf.push((val & 0xff) as u8);
        buf.push(((val >> 8) & 0xff) as u8);
        buf.push(((val >> 16) & 0xff) as u8);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&val.to_le_bytes());
    }
}

/// Write a length-encoded string.
pub fn write_length_encoded_string(buf: &mut Vec<u8>, s: &str) {
    write_length_encoded_int(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}
