//! Shared pool components for sync and async connection pools.
//!
//! This module contains types and traits that are shared between
//! `Pool` (sync) and `AsyncPool` (async) implementations.

use std::fmt;
use std::time::{Duration, Instant};

use crate::encode::VoltError;

// ============================================================================
// Pool-specific errors
// ============================================================================

/// Pool-specific error conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    /// Pool is shutting down, no new connections allowed
    PoolShutdown,
    /// Circuit breaker is open for the requested connection
    CircuitOpen,
    /// All connections are busy or unhealthy
    PoolExhausted,
    /// Timed out waiting for a connection
    Timeout,
    /// Internal lock was poisoned
    LockPoisoned,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::PoolShutdown => write!(f, "Pool is shutting down"),
            PoolError::CircuitOpen => write!(f, "Circuit breaker is open"),
            PoolError::PoolExhausted => write!(f, "Pool exhausted, no healthy connections"),
            PoolError::Timeout => write!(f, "Timed out waiting for connection"),
            PoolError::LockPoisoned => write!(f, "Internal lock poisoned"),
        }
    }
}

impl std::error::Error for PoolError {}

impl From<PoolError> for VoltError {
    fn from(e: PoolError) -> Self {
        match e {
            PoolError::PoolShutdown => VoltError::ConnectionNotAvailable,
            PoolError::CircuitOpen => VoltError::ConnectionNotAvailable,
            PoolError::PoolExhausted => VoltError::ConnectionNotAvailable,
            PoolError::Timeout => VoltError::Timeout,
            PoolError::LockPoisoned => VoltError::PoisonError("Pool lock poisoned".to_string()),
        }
    }
}

// ============================================================================
// Connection State Machine
// ============================================================================

/// Connection health state.
#[derive(Debug, Clone)]
pub enum ConnState {
    /// Connection is working normally
    Healthy,
    /// Connection has failed, tracking when it became unhealthy
    Unhealthy { since: Instant },
    /// Connection is being replaced (reconnection in progress)
    Reconnecting,
}

impl ConnState {
    /// Check if the connection is in a healthy state.
    pub fn is_healthy(&self) -> bool {
        matches!(self, ConnState::Healthy)
    }

    /// Check if the connection is currently reconnecting.
    pub fn is_reconnecting(&self) -> bool {
        matches!(self, ConnState::Reconnecting)
    }
}

// ============================================================================
// Circuit Breaker
// ============================================================================

/// Per-connection circuit breaker state.
#[derive(Debug, Clone)]
pub enum Circuit {
    /// Normal operation - requests flow through
    Closed,
    /// Circuit is open - fail fast until `until` time
    Open { until: Instant },
    /// Allow one probe request to test recovery
    HalfOpen,
}

impl Circuit {
    /// Check if request should be allowed through.
    pub fn should_allow(&self) -> bool {
        match self {
            Circuit::Closed => true,
            Circuit::Open { until } => Instant::now() >= *until,
            Circuit::HalfOpen => true,
        }
    }

    /// Transition to Open state.
    pub fn open(&mut self, duration: Duration) {
        *self = Circuit::Open {
            until: Instant::now() + duration,
        };
    }

    /// Transition to HalfOpen state (for probing).
    #[allow(dead_code)]
    pub fn half_open(&mut self) {
        *self = Circuit::HalfOpen;
    }

    /// Transition to Closed state (healthy).
    pub fn close(&mut self) {
        *self = Circuit::Closed;
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// What to do when all connections are busy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExhaustionPolicy {
    /// Return error immediately
    #[default]
    FailFast,
    /// Block up to the specified duration waiting for a connection
    Block { timeout: Duration },
}

/// How to handle startup connection failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationMode {
    /// Panic if any connection fails during pool creation
    #[default]
    FailFast,
    /// Mark failed connections as unhealthy, continue startup
    BestEffort,
}

// ============================================================================
// Pool Phase (Lifecycle)
// ============================================================================

/// Pool lifecycle phase for graceful shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolPhase {
    /// Normal operation
    Running,
    /// Fully shut down
    Shutdown,
}

// ============================================================================
// Pool Statistics
// ============================================================================

/// Pool statistics snapshot.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total number of slots in the pool
    pub size: usize,
    /// Number of healthy connections
    pub healthy: usize,
    /// Total number of requests made
    pub total_requests: usize,
    /// Whether the pool is shut down
    pub is_shutdown: bool,
}

// ============================================================================
// Logging Macros (for use by pool implementations)
// ============================================================================

/// Generate logging macros for a pool module.
///
/// Usage: `define_pool_logging_macros!(pool);` generates `pool_trace!`, `pool_debug!`, etc.
#[macro_export]
macro_rules! define_pool_logging_macros {
    ($prefix:ident) => {
        #[cfg(feature = "tracing")]
        macro_rules! $prefix_trace {
            ($($arg:tt)*) => { tracing::trace!($($arg)*) };
        }
        #[cfg(not(feature = "tracing"))]
        macro_rules! $prefix_trace {
            ($($arg:tt)*) => {};
        }

        #[cfg(feature = "tracing")]
        macro_rules! $prefix_debug {
            ($($arg:tt)*) => { tracing::debug!($($arg)*) };
        }
        #[cfg(not(feature = "tracing"))]
        macro_rules! $prefix_debug {
            ($($arg:tt)*) => {};
        }

        #[cfg(feature = "tracing")]
        macro_rules! $prefix_info {
            ($($arg:tt)*) => { tracing::info!($($arg)*) };
        }
        #[cfg(not(feature = "tracing"))]
        macro_rules! $prefix_info {
            ($($arg:tt)*) => {};
        }

        #[cfg(feature = "tracing")]
        macro_rules! $prefix_warn {
            ($($arg:tt)*) => { tracing::warn!($($arg)*) };
        }
        #[cfg(not(feature = "tracing"))]
        macro_rules! $prefix_warn {
            ($($arg:tt)*) => {};
        }

        #[cfg(feature = "tracing")]
        macro_rules! $prefix_error {
            ($($arg:tt)*) => { tracing::error!($($arg)*) };
        }
        #[cfg(not(feature = "tracing"))]
        macro_rules! $prefix_error {
            ($($arg:tt)*) => {};
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conn_state_is_healthy() {
        assert!(ConnState::Healthy.is_healthy());
        assert!(
            !ConnState::Unhealthy {
                since: Instant::now()
            }
            .is_healthy()
        );
        assert!(!ConnState::Reconnecting.is_healthy());
    }

    #[test]
    fn test_conn_state_is_reconnecting() {
        assert!(!ConnState::Healthy.is_reconnecting());
        assert!(
            !ConnState::Unhealthy {
                since: Instant::now()
            }
            .is_reconnecting()
        );
        assert!(ConnState::Reconnecting.is_reconnecting());
    }

    #[test]
    fn test_circuit_should_allow_closed() {
        let circuit = Circuit::Closed;
        assert!(circuit.should_allow());
    }

    #[test]
    fn test_circuit_should_allow_open_not_expired() {
        let circuit = Circuit::Open {
            until: Instant::now() + Duration::from_secs(60),
        };
        assert!(!circuit.should_allow());
    }

    #[test]
    fn test_circuit_should_allow_open_expired() {
        let circuit = Circuit::Open {
            until: Instant::now() - Duration::from_secs(1),
        };
        assert!(circuit.should_allow());
    }

    #[test]
    fn test_circuit_should_allow_half_open() {
        let circuit = Circuit::HalfOpen;
        assert!(circuit.should_allow());
    }

    #[test]
    fn test_circuit_transitions() {
        let mut circuit = Circuit::Closed;

        circuit.open(Duration::from_secs(30));
        assert!(matches!(circuit, Circuit::Open { .. }));

        circuit.half_open();
        assert!(matches!(circuit, Circuit::HalfOpen));

        circuit.close();
        assert!(matches!(circuit, Circuit::Closed));
    }

    #[test]
    fn test_exhaustion_policy_default() {
        let policy = ExhaustionPolicy::default();
        assert_eq!(policy, ExhaustionPolicy::FailFast);
    }

    #[test]
    fn test_validation_mode_default() {
        let mode = ValidationMode::default();
        assert_eq!(mode, ValidationMode::FailFast);
    }

    #[test]
    fn test_pool_stats() {
        let stats = PoolStats {
            size: 10,
            healthy: 8,
            total_requests: 100,
            is_shutdown: false,
        };

        assert_eq!(stats.size, 10);
        assert_eq!(stats.healthy, 8);
        assert_eq!(stats.total_requests, 100);
        assert!(!stats.is_shutdown);
    }

    #[test]
    fn test_pool_error_display() {
        assert_eq!(
            format!("{}", PoolError::PoolShutdown),
            "Pool is shutting down"
        );
        assert_eq!(
            format!("{}", PoolError::CircuitOpen),
            "Circuit breaker is open"
        );
        assert_eq!(
            format!("{}", PoolError::PoolExhausted),
            "Pool exhausted, no healthy connections"
        );
        assert_eq!(
            format!("{}", PoolError::Timeout),
            "Timed out waiting for connection"
        );
    }

    #[test]
    fn test_pool_phase() {
        assert_eq!(PoolPhase::Running, PoolPhase::Running);
        assert_ne!(PoolPhase::Running, PoolPhase::Shutdown);
    }

    #[test]
    fn test_pool_error_into_volt_error() {
        let err: VoltError = PoolError::PoolShutdown.into();
        assert!(matches!(err, VoltError::ConnectionNotAvailable));

        let err: VoltError = PoolError::Timeout.into();
        assert!(matches!(err, VoltError::Timeout));
    }
}
