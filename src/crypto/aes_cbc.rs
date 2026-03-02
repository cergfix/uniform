use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

/// Encrypt data using AES-256-CBC with PKCS7 padding.
/// Key is hashed to 32 bytes if needed. IV is derived from key.
pub fn encrypt(plaintext: &[u8], key: &str) -> Result<String, String> {
    let key_bytes = derive_key(key);
    let iv = derive_iv(&key_bytes);

    let cipher = Aes256CbcEnc::new(&key_bytes.into(), &iv.into());

    // Allocate buffer: plaintext + up to one block of padding
    let mut buf = vec![0u8; plaintext.len() + 16];
    buf[..plaintext.len()].copy_from_slice(plaintext);

    let encrypted = cipher
        .encrypt_padded_mut::<Pkcs7>(&mut buf, plaintext.len())
        .map_err(|e| format!("encrypt: {}", e))?;

    Ok(BASE64.encode(encrypted))
}

/// Decrypt base64-encoded AES-256-CBC data.
pub fn decrypt(ciphertext_b64: &str, key: &str) -> Result<Vec<u8>, String> {
    let key_bytes = derive_key(key);
    let iv = derive_iv(&key_bytes);

    let ciphertext = BASE64
        .decode(ciphertext_b64)
        .map_err(|e| format!("base64 decode: {}", e))?;

    let cipher = Aes256CbcDec::new(&key_bytes.into(), &iv.into());

    let mut buf = ciphertext.clone();
    let decrypted = cipher
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|e| format!("decrypt: {}", e))?;

    Ok(decrypted.to_vec())
}

/// Derive a 32-byte key from the input string.
fn derive_key(key: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let result = hasher.finalize();
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&result);
    key_bytes
}

/// Derive a 16-byte IV from the key (first 16 bytes).
fn derive_iv(key: &[u8; 32]) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&key[..16]);
    iv
}
