use tokio::net::TcpStream;

use crate::server::mysql::protocol;
use crate::types::row::OwnedRow;

/// Write a MySQL result set to the stream.
pub async fn write_result_set(
    stream: &mut TcpStream,
    mut seq_id: u8,
    rows: &[OwnedRow],
) -> Result<(), String> {
    if rows.is_empty() {
        // Send OK with zero rows
        let ok = protocol::build_ok_packet(0, 0);
        protocol::write_packet(stream, seq_id, &ok).await?;
        return Ok(());
    }

    // Collect all column names from all rows
    let mut column_names: Vec<String> = Vec::new();
    for row in rows {
        for key in row.columns.keys() {
            if !column_names.contains(key) {
                column_names.push(key.clone());
            }
        }
    }

    // 1. Column count packet
    let mut buf = Vec::new();
    protocol::write_length_encoded_int(&mut buf, column_names.len() as u64);
    protocol::write_packet(stream, seq_id, &buf).await?;
    seq_id = seq_id.wrapping_add(1);

    // 2. Column definition packets
    for col_name in &column_names {
        let col_def = build_column_definition(col_name);
        protocol::write_packet(stream, seq_id, &col_def).await?;
        seq_id = seq_id.wrapping_add(1);
    }

    // 3. EOF packet (after column definitions)
    let eof = protocol::build_eof_packet();
    protocol::write_packet(stream, seq_id, &eof).await?;
    seq_id = seq_id.wrapping_add(1);

    // 4. Row data packets
    for row in rows {
        let row_data = build_row_data(row, &column_names);
        protocol::write_packet(stream, seq_id, &row_data).await?;
        seq_id = seq_id.wrapping_add(1);
    }

    // 5. EOF packet (after rows)
    let eof = protocol::build_eof_packet();
    protocol::write_packet(stream, seq_id, &eof).await?;

    Ok(())
}

/// Build a column definition packet.
fn build_column_definition(name: &str) -> Vec<u8> {
    let mut buf = Vec::new();

    // catalog
    protocol::write_length_encoded_string(&mut buf, "def");
    // schema
    protocol::write_length_encoded_string(&mut buf, "");
    // table
    protocol::write_length_encoded_string(&mut buf, "");
    // org_table
    protocol::write_length_encoded_string(&mut buf, "");
    // name
    protocol::write_length_encoded_string(&mut buf, name);
    // org_name
    protocol::write_length_encoded_string(&mut buf, name);

    // Fixed-length fields (12 bytes)
    buf.push(0x0c); // length of fixed fields

    // Character set (utf8mb4 = 45, 0x002d)
    buf.push(0x2d);
    buf.push(0x00);

    // Column length (4 bytes)
    buf.extend_from_slice(&[0xff, 0xff, 0x00, 0x00]);

    // Column type (VARCHAR = 0xfd)
    buf.push(0xfd);

    // Flags (2 bytes)
    buf.push(0x00);
    buf.push(0x00);

    // Decimals
    buf.push(0x00);

    // Filler (2 bytes)
    buf.push(0x00);
    buf.push(0x00);

    buf
}

/// Build a row data packet (text protocol).
fn build_row_data(row: &OwnedRow, column_names: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for col_name in column_names {
        match row.columns.get(col_name) {
            Some(val) => {
                let s = val.to_string_repr();
                protocol::write_length_encoded_string(&mut buf, &s);
            }
            None => {
                // NULL
                buf.push(0xfb);
            }
        }
    }
    buf
}
