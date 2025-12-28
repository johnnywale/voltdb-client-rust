//! Shared protocol handling for sync and async connections.
//!
//! This module contains common code for VoltDB wire protocol handling,
//! including authentication handshake and response parsing.

use std::io::Read;
use std::net::Ipv4Addr;
use std::str::from_utf8;

use bytebuffer::ByteBuffer;
use sha2::{Digest, Sha256};

use crate::encode::VoltError;
use crate::node::ConnInfo;

/// Protocol version byte
pub const PROTOCOL_VERSION: u8 = 1;

/// Ping handle constant - used for keep-alive messages
pub const PING_HANDLE: i64 = 1 << (63 - 1);

/// Build authentication message for VoltDB connection.
///
/// Returns the serialized authentication message as bytes.
pub fn build_auth_message(user: Option<&str>, pass: Option<&str>) -> Result<Vec<u8>, VoltError> {
    let mut buffer = ByteBuffer::new();
    let version = [PROTOCOL_VERSION; 1];

    // Message length placeholder (will be filled later)
    buffer.write_u32(0);
    // Protocol version
    buffer.write_bytes(&version);
    buffer.write_bytes(&version);
    // Database name
    buffer.write_string("database");

    // Username
    match user {
        None => buffer.write_string(""),
        Some(u) => buffer.write_string(u),
    }

    // Password hash (SHA-256)
    let password_bytes = pass.map(|p| p.as_bytes()).unwrap_or(&[]);
    let mut hasher: Sha256 = Sha256::new();
    Digest::update(&mut hasher, password_bytes);
    buffer.write_bytes(&hasher.finalize());

    // Update message length
    buffer.set_wpos(0);
    buffer.write_u32((buffer.len() - 4) as u32);

    Ok(buffer.into_vec())
}

/// Parse authentication response from server.
///
/// Returns connection info on success, or VoltError::AuthFailed on failure.
pub fn parse_auth_response(data: &[u8]) -> Result<ConnInfo, VoltError> {
    let mut res = ByteBuffer::from_bytes(data);

    let _version = res.read_u8()?;
    let auth = res.read_u8()?;

    if auth != 0 {
        return Err(VoltError::AuthFailed);
    }

    let host_id = res.read_i32()?;
    let connection = res.read_i64()?;
    let _ = res.read_i64()?; // timestamp
    let leader = res.read_i32()?;
    let bs = (leader as u32).to_be_bytes();
    let leader_addr = Ipv4Addr::from(bs);

    let length = res.read_i32()?;
    let mut build = vec![0; length as usize];
    res.read_exact(&mut build)?;
    let b = from_utf8(&build)?;

    Ok(ConnInfo {
        host_id,
        connection,
        leader_addr,
        build: String::from(b),
    })
}

/// Read a length-prefixed message from a stream.
///
/// Returns the message payload (without the length prefix).
pub fn read_message<R: Read>(reader: &mut R) -> Result<Vec<u8>, VoltError> {
    use byteorder::{BigEndian, ReadBytesExt};

    let len = reader.read_u32::<BigEndian>()?;
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut data = vec![0u8; len as usize];
    reader.read_exact(&mut data)?;
    Ok(data)
}

/// Parse the handle from a response message.
///
/// Assumes the first byte is status and the next 8 bytes are the handle.
pub fn parse_response_handle(data: &[u8]) -> Result<i64, VoltError> {
    if data.len() < 9 {
        return Err(VoltError::Other("Response too short".to_string()));
    }

    let mut buffer = ByteBuffer::from_bytes(&data[1..9]);
    Ok(buffer.read_i64()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_auth_message_no_credentials() {
        let msg = build_auth_message(None, None).unwrap();
        assert!(!msg.is_empty());
        // First 4 bytes are length
        let len = u32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(len as usize, msg.len() - 4);
    }

    #[test]
    fn test_build_auth_message_with_credentials() {
        let msg = build_auth_message(Some("admin"), Some("password")).unwrap();
        assert!(!msg.is_empty());
        // Should be larger than no-credentials message due to username
        let no_cred_msg = build_auth_message(None, None).unwrap();
        assert!(msg.len() > no_cred_msg.len());
    }

    #[test]
    fn test_build_auth_message_user_only() {
        let msg = build_auth_message(Some("testuser"), None).unwrap();
        assert!(!msg.is_empty());
        let len = u32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(len as usize, msg.len() - 4);
    }

    #[test]
    fn test_build_auth_message_contains_protocol_version() {
        let msg = build_auth_message(None, None).unwrap();
        // Bytes 4 and 5 should be protocol version (1)
        assert_eq!(msg[4], PROTOCOL_VERSION);
        assert_eq!(msg[5], PROTOCOL_VERSION);
    }

    #[test]
    fn test_build_auth_message_contains_database() {
        let msg = build_auth_message(None, None).unwrap();
        // "database" string should be in the message
        let msg_str = String::from_utf8_lossy(&msg);
        assert!(msg_str.contains("database"));
    }

    #[test]
    fn test_parse_response_handle_valid() {
        // Status byte + 8 bytes handle
        let data = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 42];
        let handle = parse_response_handle(&data).unwrap();
        assert_eq!(handle, 42);
    }

    #[test]
    fn test_parse_response_handle_negative() {
        // Status byte + handle with high bit set
        let data = vec![0u8, 255, 255, 255, 255, 255, 255, 255, 255];
        let handle = parse_response_handle(&data).unwrap();
        assert_eq!(handle, -1);
    }

    #[test]
    fn test_parse_response_handle_too_short() {
        let data = vec![0u8, 1, 2, 3]; // Only 4 bytes, need 9
        let result = parse_response_handle(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_auth_response_invalid_auth() {
        // Version byte + auth status (non-zero = failure)
        let mut data = vec![1u8, 1]; // version=1, auth=1 (failed)
        // Pad with enough data to avoid read errors
        data.extend_from_slice(&[0u8; 50]);
        let result = parse_auth_response(&data);
        assert!(matches!(result, Err(VoltError::AuthFailed)));
    }

    #[test]
    fn test_ping_handle_constant() {
        // PING_HANDLE should be a large positive value (1 << 62)
        assert!(PING_HANDLE > 0);
        // 1 << 63 - 1 is parsed as 1 << (63-1) = 1 << 62 due to operator precedence
        assert_eq!(PING_HANDLE, 1i64 << 62);
    }
}
