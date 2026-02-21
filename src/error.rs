//! Canonical error type for the VoltDB client.

use std::sync::PoisonError;

use crate::response::VoltResponseInfo;

/// The single error type for all VoltDB client operations.
#[derive(Debug, thiserror::Error)]
pub enum VoltError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Recv error: {0}")]
    RecvError(#[from] std::sync::mpsc::RecvError),

    #[error("volt execute failed: {0:?}")]
    ExecuteFail(VoltResponseInfo),

    #[error("InvalidColumnType {0}")]
    InvalidColumnType(i8),

    #[error("MessageTooLarge {0}")]
    MessageTooLarge(usize),

    #[error("Error {0}")]
    NoValue(String),

    #[error("Error {0}")]
    Other(String),

    #[error("Error {0}")]
    NegativeNumTables(i16),

    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Error {0}")]
    PoisonError(String),

    #[error("Error {0}")]
    BadReturnStatusOnTable(i8),

    #[error("Auth failed")]
    AuthFailed,

    #[error("Connection lost")]
    ConnectionNotAvailable,

    #[error("Invalid Config")]
    InvalidConfig,

    #[error("Operation timeout")]
    Timeout,

    #[error("Connection Closed")]
    ConnectionClosed,

    /// Returned when a non-Option type encounters a NULL value.
    /// Use `Option<T>` if the column may contain NULL values.
    #[error("Unexpected NULL value in column '{0}'. Use Option<T> for nullable columns.")]
    UnexpectedNull(String),

    #[error("Pool is shutting down")]
    PoolShutdown,

    #[error("Circuit breaker is open")]
    CircuitOpen,

    #[error("Pool exhausted, no healthy connections")]
    PoolExhausted,
}

impl<T> From<PoisonError<T>> for VoltError {
    fn from(p: PoisonError<T>) -> VoltError {
        VoltError::PoisonError(p.to_string().to_owned())
    }
}

impl VoltError {
    /// Returns true if this error indicates a fatal connection problem
    /// that requires the connection to be replaced/healed.
    pub fn is_connection_fatal(&self) -> bool {
        match self {
            VoltError::Io(io_err) => matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            ),
            VoltError::ConnectionNotAvailable => true,
            VoltError::ConnectionClosed => true,
            VoltError::Timeout => true,
            VoltError::RecvError(_) => true,
            VoltError::PoolShutdown => true,
            _ => false,
        }
    }

    pub fn message_too_large(size: usize) -> Self {
        VoltError::MessageTooLarge(size)
    }

    pub fn connection_closed() -> Self {
        VoltError::ConnectionClosed
    }

    pub fn timeout() -> Self {
        VoltError::Timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_volt_error_display() {
        let err = VoltError::AuthFailed;
        assert_eq!(format!("{}", err), "Auth failed");

        let err = VoltError::ConnectionNotAvailable;
        assert_eq!(format!("{}", err), "Connection lost");

        let err = VoltError::Timeout;
        assert_eq!(format!("{}", err), "Operation timeout");
    }

    #[test]
    fn test_volt_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        let volt_err: VoltError = io_err.into();
        assert!(matches!(volt_err, VoltError::Io(_)));
    }

    #[test]
    fn test_volt_error_unexpected_null() {
        let err = VoltError::UnexpectedNull("my_column".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("my_column"));
        assert!(msg.contains("Option<T>"));
    }

    #[test]
    fn test_pool_shutdown_display() {
        assert_eq!(
            format!("{}", VoltError::PoolShutdown),
            "Pool is shutting down"
        );
    }

    #[test]
    fn test_circuit_open_display() {
        assert_eq!(
            format!("{}", VoltError::CircuitOpen),
            "Circuit breaker is open"
        );
    }

    #[test]
    fn test_pool_exhausted_display() {
        assert_eq!(
            format!("{}", VoltError::PoolExhausted),
            "Pool exhausted, no healthy connections"
        );
    }

    #[test]
    fn test_is_connection_fatal_pool_shutdown() {
        let err = VoltError::PoolShutdown;
        assert!(err.is_connection_fatal());
    }

    #[test]
    fn test_is_connection_fatal_io_connection_reset() {
        let err = VoltError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
        assert!(err.is_connection_fatal());
    }

    #[test]
    fn test_is_connection_fatal_no_value_not_fatal() {
        let err = VoltError::NoValue("test".to_string());
        assert!(!err.is_connection_fatal());
    }

    #[test]
    fn test_convenience_constructors() {
        assert!(matches!(
            VoltError::message_too_large(100),
            VoltError::MessageTooLarge(100)
        ));
        assert!(matches!(
            VoltError::connection_closed(),
            VoltError::ConnectionClosed
        ));
        assert!(matches!(VoltError::timeout(), VoltError::Timeout));
    }
}
