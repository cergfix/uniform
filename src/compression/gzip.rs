use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression;
use std::io::Read;

/// Gzip compress data and return base64-encoded result.
pub fn gzip_encode(data: &[u8]) -> Result<String, String> {
    let mut encoder = GzEncoder::new(data, Compression::default());
    let mut compressed = Vec::new();
    encoder
        .read_to_end(&mut compressed)
        .map_err(|e| format!("gzip compress: {}", e))?;
    Ok(BASE64.encode(&compressed))
}

/// Decode base64 and gunzip decompress.
pub fn gunzip_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let compressed = BASE64
        .decode(encoded)
        .map_err(|e| format!("base64 decode: {}", e))?;
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| format!("gunzip decompress: {}", e))?;
    Ok(decompressed)
}

/// Gzip compress a string field value.
pub fn gzip_field(value: &str) -> Result<String, String> {
    gzip_encode(value.as_bytes())
}

/// Gunzip decompress a base64-encoded field value.
pub fn gunzip_field(value: &str) -> Result<String, String> {
    let bytes = gunzip_decode(value)?;
    String::from_utf8(bytes).map_err(|e| format!("utf8 decode: {}", e))
}
