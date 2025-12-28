//! Hierarchical error types for VoltDB client.
//!
//! This module provides structured error types organized by category:
//! - `ConnectionError` - Network and connection-related errors
//! - `ProtocolError` - Wire protocol parsing and encoding errors
//! - `QueryError` - Query execution and result handling errors
//!
//! The main `VoltError` type wraps these subcategories for backward compatibility.

use std::fmt;
use std::io;
use std::str::Utf8Error;
use std::sync::PoisonError;

use crate::response::VoltResponseInfo;

/// Connection-related errors.
#[derive(Debug)]
pub enum ConnectionError {
    /// I/O error during network operations
    Io(io::Error),
    /// Connection is not available or has been closed
    NotAvailable,
    /// Connection timeout occurred
    Timeout,
    /// Authentication failed
    AuthFailed,
    /// Invalid connection configuration
    InvalidConfig,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "I/O error: {}", e),
            ConnectionError::NotAvailable => write!(f, "Connection not available"),
            ConnectionError::Timeout => write!(f, "Connection timeout"),
            ConnectionError::AuthFailed => write!(f, "Authentication failed"),
            ConnectionError::InvalidConfig => write!(f, "Invalid configuration"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

/// Protocol-related errors (wire format, parsing).
#[derive(Debug)]
pub enum ProtocolError {
    /// Invalid column type encountered
    InvalidColumnType(i8),
    /// Negative number of tables in response
    NegativeNumTables(i16),
    /// Bad return status on table
    BadReturnStatusOnTable(i8),
    /// UTF-8 encoding/decoding error
    Utf8(Utf8Error),
    /// Generic protocol error
    Other(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::InvalidColumnType(t) => write!(f, "Invalid column type: {}", t),
            ProtocolError::NegativeNumTables(n) => write!(f, "Negative number of tables: {}", n),
            ProtocolError::BadReturnStatusOnTable(s) => {
                write!(f, "Bad return status on table: {}", s)
            }
            ProtocolError::Utf8(e) => write!(f, "UTF-8 error: {}", e),
            ProtocolError::Other(s) => write!(f, "Protocol error: {}", s),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Utf8(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Utf8Error> for ProtocolError {
    fn from(err: Utf8Error) -> Self {
        ProtocolError::Utf8(err)
    }
}

/// Query execution and result handling errors.
#[derive(Debug)]
pub enum QueryError {
    /// Server returned an error response
    ExecuteFail(VoltResponseInfo),
    /// Requested column/value not found
    NoValue(String),
    /// Unexpected NULL value in non-nullable column
    UnexpectedNull(String),
    /// Channel receive error
    RecvError(std::sync::mpsc::RecvError),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::ExecuteFail(info) => write!(f, "Query execution failed: {:?}", info),
            QueryError::NoValue(col) => write!(f, "No value found for: {}", col),
            QueryError::UnexpectedNull(col) => {
                write!(
                    f,
                    "Unexpected NULL in column '{}'. Use Option<T> for nullable columns.",
                    col
                )
            }
            QueryError::RecvError(e) => write!(f, "Receive error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QueryError::RecvError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::sync::mpsc::RecvError> for QueryError {
    fn from(err: std::sync::mpsc::RecvError) -> Self {
        QueryError::RecvError(err)
    }
}

/// Concurrency-related errors.
#[derive(Debug)]
pub struct ConcurrencyError(pub String);

impl fmt::Display for ConcurrencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Concurrency error: {}", self.0)
    }
}

impl std::error::Error for ConcurrencyError {}

impl<T> From<PoisonError<T>> for ConcurrencyError {
    fn from(err: PoisonError<T>) -> Self {
        ConcurrencyError(err.to_string())
    }
}

/// Helper trait to convert subcategory errors to VoltError.
pub trait IntoVoltError {
    fn into_volt_error(self) -> crate::encode::VoltError;
}

impl IntoVoltError for ConnectionError {
    fn into_volt_error(self) -> crate::encode::VoltError {
        match self {
            ConnectionError::Io(e) => crate::encode::VoltError::Io(e),
            ConnectionError::NotAvailable => crate::encode::VoltError::ConnectionNotAvailable,
            ConnectionError::Timeout => crate::encode::VoltError::Timeout,
            ConnectionError::AuthFailed => crate::encode::VoltError::AuthFailed,
            ConnectionError::InvalidConfig => crate::encode::VoltError::InvalidConfig,
        }
    }
}

impl IntoVoltError for ProtocolError {
    fn into_volt_error(self) -> crate::encode::VoltError {
        match self {
            ProtocolError::InvalidColumnType(t) => crate::encode::VoltError::InvalidColumnType(t),
            ProtocolError::NegativeNumTables(n) => crate::encode::VoltError::NegativeNumTables(n),
            ProtocolError::BadReturnStatusOnTable(s) => {
                crate::encode::VoltError::BadReturnStatusOnTable(s)
            }
            ProtocolError::Utf8(e) => crate::encode::VoltError::Utf8Error(e),
            ProtocolError::Other(s) => crate::encode::VoltError::Other(s),
        }
    }
}

impl IntoVoltError for QueryError {
    fn into_volt_error(self) -> crate::encode::VoltError {
        match self {
            QueryError::ExecuteFail(info) => crate::encode::VoltError::ExecuteFail(info),
            QueryError::NoValue(s) => crate::encode::VoltError::NoValue(s),
            QueryError::UnexpectedNull(s) => crate::encode::VoltError::UnexpectedNull(s),
            QueryError::RecvError(e) => crate::encode::VoltError::RecvError(e),
        }
    }
}

impl IntoVoltError for ConcurrencyError {
    fn into_volt_error(self) -> crate::encode::VoltError {
        crate::encode::VoltError::PoisonError(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    // ConnectionError tests
    #[test]
    fn test_connection_error_display() {
        let err = ConnectionError::NotAvailable;
        assert_eq!(format!("{}", err), "Connection not available");
    }

    #[test]
    fn test_connection_error_timeout() {
        let err = ConnectionError::Timeout;
        assert_eq!(format!("{}", err), "Connection timeout");
    }

    #[test]
    fn test_connection_error_auth_failed() {
        let err = ConnectionError::AuthFailed;
        assert_eq!(format!("{}", err), "Authentication failed");
    }

    #[test]
    fn test_connection_error_invalid_config() {
        let err = ConnectionError::InvalidConfig;
        assert_eq!(format!("{}", err), "Invalid configuration");
    }

    #[test]
    fn test_connection_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        let conn_err: ConnectionError = io_err.into();
        assert!(matches!(conn_err, ConnectionError::Io(_)));
        assert!(format!("{}", conn_err).contains("I/O error"));
    }

    // ProtocolError tests
    #[test]
    fn test_protocol_error_display() {
        let err = ProtocolError::InvalidColumnType(99);
        assert_eq!(format!("{}", err), "Invalid column type: 99");
    }

    #[test]
    fn test_protocol_error_negative_tables() {
        let err = ProtocolError::NegativeNumTables(-5);
        assert!(format!("{}", err).contains("-5"));
    }

    #[test]
    fn test_protocol_error_bad_status() {
        let err = ProtocolError::BadReturnStatusOnTable(-1);
        assert!(format!("{}", err).contains("-1"));
    }

    #[test]
    fn test_protocol_error_other() {
        let err = ProtocolError::Other("custom error".to_string());
        assert!(format!("{}", err).contains("custom error"));
    }

    // QueryError tests
    #[test]
    fn test_query_error_display() {
        let err = QueryError::UnexpectedNull("my_column".to_string());
        assert!(format!("{}", err).contains("my_column"));
    }

    #[test]
    fn test_query_error_no_value() {
        let err = QueryError::NoValue("missing_col".to_string());
        assert!(format!("{}", err).contains("missing_col"));
    }

    #[test]
    fn test_query_error_unexpected_null_message() {
        let err = QueryError::UnexpectedNull("test_col".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("test_col"));
        assert!(msg.contains("Option<T>"));
    }

    // ConcurrencyError tests
    #[test]
    fn test_concurrency_error_display() {
        let err = ConcurrencyError("lock poisoned".to_string());
        assert!(format!("{}", err).contains("lock poisoned"));
    }

    // IntoVoltError trait tests
    #[test]
    fn test_connection_error_into_volt_error() {
        let conn_err = ConnectionError::NotAvailable;
        let volt_err = conn_err.into_volt_error();
        assert!(matches!(
            volt_err,
            crate::encode::VoltError::ConnectionNotAvailable
        ));
    }

    #[test]
    fn test_connection_timeout_into_volt_error() {
        let conn_err = ConnectionError::Timeout;
        let volt_err = conn_err.into_volt_error();
        assert!(matches!(volt_err, crate::encode::VoltError::Timeout));
    }

    #[test]
    fn test_protocol_error_into_volt_error() {
        let proto_err = ProtocolError::InvalidColumnType(42);
        let volt_err = proto_err.into_volt_error();
        match volt_err {
            crate::encode::VoltError::InvalidColumnType(t) => assert_eq!(t, 42),
            _ => panic!("Expected InvalidColumnType"),
        }
    }

    #[test]
    fn test_query_error_into_volt_error() {
        let query_err = QueryError::NoValue("col".to_string());
        let volt_err = query_err.into_volt_error();
        match volt_err {
            crate::encode::VoltError::NoValue(s) => assert_eq!(s, "col"),
            _ => panic!("Expected NoValue"),
        }
    }

    #[test]
    fn test_concurrency_error_into_volt_error() {
        let conc_err = ConcurrencyError("poison".to_string());
        let volt_err = conc_err.into_volt_error();
        match volt_err {
            crate::encode::VoltError::PoisonError(s) => assert_eq!(s, "poison"),
            _ => panic!("Expected PoisonError"),
        }
    }

    // std::error::Error trait tests
    #[test]
    fn test_connection_error_source() {
        let io_err = io::Error::new(io::ErrorKind::Other, "test");
        let conn_err = ConnectionError::Io(io_err);
        assert!(conn_err.source().is_some());

        let conn_err = ConnectionError::NotAvailable;
        assert!(conn_err.source().is_none());
    }

    #[test]
    fn test_protocol_error_source() {
        let proto_err = ProtocolError::InvalidColumnType(1);
        assert!(proto_err.source().is_none());
    }

    #[test]
    fn test_query_error_source() {
        let query_err = QueryError::NoValue("x".to_string());
        assert!(query_err.source().is_none());
    }
}
