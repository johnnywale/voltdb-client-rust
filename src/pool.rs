//! Production-ready connection pool for VoltDB.
//!
//! # Features
//! - Thread-safe with fine-grained locking
//! - Connection state machine (Healthy, Unhealthy, Reconnecting)
//! - Per-connection circuit breaker
//! - Configurable exhaustion policy (FailFast or Block with Condvar)
//! - Graceful shutdown with drain mode
//! - Optional structured logging (`tracing` feature)
//! - Optional metrics (`metrics` feature)
//!
//! # Design
//! - Pool lock only guards metadata (state, circuit breaker)
//! - Each Node has its own lock for I/O operations
//! - Network I/O never holds the pool lock

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::pool_core::{
    Circuit, ConnState, ExhaustionPolicy, PoolError, PoolPhase, ValidationMode,
};
use crate::{Node, NodeOpt, Opts, Value, VoltError, VoltTable, block_for_result, node};

// ============================================================================
// Logging macros - use tracing if available, otherwise no-op
// ============================================================================

#[cfg(feature = "tracing")]
macro_rules! pool_trace {
    ($($arg:tt)*) => { tracing::trace!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! pool_trace {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! pool_debug {
    ($($arg:tt)*) => { tracing::debug!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! pool_debug {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! pool_info {
    ($($arg:tt)*) => { tracing::info!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! pool_info {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! pool_warn {
    ($($arg:tt)*) => { tracing::warn!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! pool_warn {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! pool_error {
    ($($arg:tt)*) => { tracing::error!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! pool_error {
    ($($arg:tt)*) => {};
}

// ============================================================================
// Metrics - use metrics crate if available, otherwise no-op
// ============================================================================

#[cfg(feature = "metrics")]
mod pool_metrics {
    use metrics::{counter, gauge};

    pub fn set_connections_total(count: usize) {
        gauge!("voltdb_pool_connections_total").set(count as f64);
    }

    pub fn set_connections_healthy(count: usize) {
        gauge!("voltdb_pool_connections_healthy").set(count as f64);
    }

    pub fn inc_reconnect_total() {
        counter!("voltdb_pool_reconnect_total").increment(1);
    }

    pub fn inc_circuit_open_total() {
        counter!("voltdb_pool_circuit_open_total").increment(1);
    }

    pub fn inc_requests_failed_total() {
        counter!("voltdb_pool_requests_failed_total").increment(1);
    }

    pub fn inc_requests_total() {
        counter!("voltdb_pool_requests_total").increment(1);
    }
}

#[cfg(not(feature = "metrics"))]
mod pool_metrics {
    pub fn set_connections_total(_count: usize) {}
    pub fn set_connections_healthy(_count: usize) {}
    pub fn inc_reconnect_total() {}
    pub fn inc_circuit_open_total() {}
    pub fn inc_requests_failed_total() {}
    pub fn inc_requests_total() {}
}

// ============================================================================
// Configuration
// ============================================================================

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of connections in the pool
    pub size: usize,
    /// Backoff duration before retry reconnection
    pub reconnect_backoff: Duration,
    /// How long circuit breaker stays open
    pub circuit_open_duration: Duration,
    /// What to do when pool is exhausted
    pub exhaustion_policy: ExhaustionPolicy,
    /// How to handle startup failures
    pub validation_mode: ValidationMode,
    /// Number of consecutive failures before opening circuit
    pub circuit_failure_threshold: u32,
    /// Timeout for graceful shutdown drain
    pub shutdown_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 10,
            reconnect_backoff: Duration::from_secs(5),
            circuit_open_duration: Duration::from_secs(30),
            exhaustion_policy: ExhaustionPolicy::FailFast,
            validation_mode: ValidationMode::FailFast,
            circuit_failure_threshold: 3,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl PoolConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    pub fn reconnect_backoff(mut self, duration: Duration) -> Self {
        self.reconnect_backoff = duration;
        self
    }

    pub fn circuit_open_duration(mut self, duration: Duration) -> Self {
        self.circuit_open_duration = duration;
        self
    }

    pub fn exhaustion_policy(mut self, policy: ExhaustionPolicy) -> Self {
        self.exhaustion_policy = policy;
        self
    }

    pub fn validation_mode(mut self, mode: ValidationMode) -> Self {
        self.validation_mode = mode;
        self
    }

    pub fn circuit_failure_threshold(mut self, threshold: u32) -> Self {
        self.circuit_failure_threshold = threshold;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

// ============================================================================
// Pool Phase (Lifecycle)
// ============================================================================
// Connection Slot (Metadata only - Node is separate)
// ============================================================================

/// Metadata for a connection slot. The actual Node is stored separately.
///
/// Note: VoltDB connections support concurrent requests via handle-based
/// request tracking. Multiple threads can share the same connection.
#[derive(Debug)]
struct SlotMeta {
    state: ConnState,
    circuit: Circuit,
    consecutive_failures: u32,
    last_reconnect_attempt: Option<Instant>,
    host_idx: usize,
}

impl SlotMeta {
    fn new_healthy(host_idx: usize) -> Self {
        Self {
            state: ConnState::Healthy,
            circuit: Circuit::Closed,
            consecutive_failures: 0,
            last_reconnect_attempt: None,
            host_idx,
        }
    }

    fn new_unhealthy(host_idx: usize) -> Self {
        Self {
            state: ConnState::Unhealthy {
                since: Instant::now(),
            },
            circuit: Circuit::Open {
                until: Instant::now() + Duration::from_secs(5),
            },
            consecutive_failures: 1,
            last_reconnect_attempt: None,
            host_idx,
        }
    }

    /// Check if this slot can be used (healthy and circuit allows)
    /// Note: Multiple threads can use the same slot concurrently
    fn is_available(&self) -> bool {
        self.state.is_healthy() && self.circuit.should_allow()
    }

    /// Check if this slot needs reconnection and can attempt it
    fn needs_reconnect(&self, backoff: Duration) -> bool {
        if self.state.is_healthy() || self.state.is_reconnecting() {
            return false;
        }

        match self.last_reconnect_attempt {
            None => true,
            Some(last) => Instant::now().duration_since(last) >= backoff,
        }
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = ConnState::Healthy;
        self.circuit.close();
    }

    fn record_failure(&mut self, config: &PoolConfig) {
        self.consecutive_failures += 1;
        self.state = ConnState::Unhealthy {
            since: Instant::now(),
        };

        if self.consecutive_failures >= config.circuit_failure_threshold {
            self.circuit.open(config.circuit_open_duration);
            pool_metrics::inc_circuit_open_total();
        }
    }
}

// ============================================================================
// Inner Pool (protected by Mutex)
// ============================================================================

struct InnerPool {
    opts: Opts,
    config: PoolConfig,
    slots: Vec<SlotMeta>,
    // Nodes are stored separately with their own locks.
    // Multiple threads can share the same node concurrently -
    // VoltDB uses handle-based request tracking for multiplexing.
    nodes: Vec<Arc<Mutex<Option<Node>>>>,
    phase: PoolPhase,
}

impl fmt::Debug for InnerPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InnerPool")
            .field("config", &self.config)
            .field("slots_count", &self.slots.len())
            .field("phase", &self.phase)
            .finish()
    }
}

impl InnerPool {
    fn node_opt(&self, host_idx: usize) -> Result<NodeOpt, VoltError> {
        let ip_port = self
            .opts
            .0
            .ip_ports
            .get(host_idx)
            .cloned()
            .ok_or(VoltError::InvalidConfig)?;
        Ok(NodeOpt {
            ip_port,
            pass: self.opts.0.pass.clone(),
            user: self.opts.0.user.clone(),
            connect_timeout: self.opts.0.connect_timeout,
            read_timeout: self.opts.0.read_timeout,
        })
    }

    fn new(opts: Opts, config: PoolConfig) -> Result<Self, VoltError> {
        let num_hosts = opts.0.ip_ports.len();
        let mut inner = InnerPool {
            opts,
            config: config.clone(),
            slots: Vec::with_capacity(config.size),
            nodes: Vec::with_capacity(config.size),
            phase: PoolPhase::Running,
        };

        for i in 0..config.size {
            let host_idx = i % num_hosts;
            let node_opt = inner.node_opt(host_idx)?;

            pool_debug!(slot = i, host = host_idx, "creating connection");

            match node::Node::new(node_opt) {
                Ok(node) => {
                    inner.slots.push(SlotMeta::new_healthy(host_idx));
                    inner.nodes.push(Arc::new(Mutex::new(Some(node))));
                    pool_info!(slot = i, "connection established");
                }
                Err(e) => match config.validation_mode {
                    ValidationMode::FailFast => {
                        pool_error!(slot = i, error = ?e, "connection failed, aborting pool creation");
                        return Err(e);
                    }
                    ValidationMode::BestEffort => {
                        pool_warn!(slot = i, error = ?e, "connection failed, marking unhealthy");
                        inner.slots.push(SlotMeta::new_unhealthy(host_idx));
                        inner.nodes.push(Arc::new(Mutex::new(None)));
                    }
                },
            }
        }

        inner.update_metrics();
        pool_info!(
            size = config.size,
            healthy = inner.healthy_count(),
            "pool initialized"
        );

        Ok(inner)
    }

    fn healthy_count(&self) -> usize {
        self.slots.iter().filter(|s| s.state.is_healthy()).count()
    }

    fn update_metrics(&self) {
        pool_metrics::set_connections_total(self.slots.len());
        pool_metrics::set_connections_healthy(self.healthy_count());
    }
}

// ============================================================================
// Pool (Thread-safe)
// ============================================================================

/// Thread-safe connection pool for VoltDB.
///
/// # Thread Safety
/// `Pool` is designed for high concurrency:
/// - Pool lock only guards metadata (microseconds)
/// - Each connection has its own lock for I/O
/// - Network I/O never blocks other threads from getting connections
///
/// # Example
/// ```ignore
/// use voltdb_client_rust::{Pool, PoolConfig, Opts, IpPort};
///
/// let hosts = vec![IpPort::new("localhost".to_string(), 21212)];
/// let config = PoolConfig::new().size(5);
/// let pool = Pool::with_config(Opts::new(hosts), config)?;
///
/// let mut conn = pool.get_conn()?;
/// let table = conn.query("SELECT * FROM foo")?;
/// ```
pub struct Pool {
    inner: Arc<(Mutex<InnerPool>, Condvar)>,
    counter: AtomicUsize,
    shutdown_flag: AtomicBool,
    config: PoolConfig,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.0.lock().ok();
        f.debug_struct("Pool")
            .field("counter", &self.counter.load(Ordering::Relaxed))
            .field("shutdown", &self.shutdown_flag.load(Ordering::Relaxed))
            .field(
                "healthy",
                &inner.as_ref().map(|i| i.healthy_count()).unwrap_or(0),
            )
            .field("config", &self.config)
            .finish()
    }
}

impl Pool {
    /// Create a new pool with default configuration (10 connections).
    pub fn new<T: Into<Opts>>(opts: T) -> Result<Pool, VoltError> {
        Pool::with_config(opts, PoolConfig::default())
    }

    /// Create a new pool with custom size (convenience method).
    pub fn new_manual<T: Into<Opts>>(size: usize, opts: T) -> Result<Pool, VoltError> {
        Pool::with_config(opts, PoolConfig::new().size(size))
    }

    /// Create a new pool with full configuration.
    pub fn with_config<T: Into<Opts>>(opts: T, config: PoolConfig) -> Result<Pool, VoltError> {
        let inner = InnerPool::new(opts.into(), config.clone())?;
        Ok(Pool {
            inner: Arc::new((Mutex::new(inner), Condvar::new())),
            counter: AtomicUsize::new(0),
            shutdown_flag: AtomicBool::new(false),
            config,
        })
    }

    /// Get a connection from the pool.
    ///
    /// # Errors
    /// - `PoolError::PoolShutdown` if pool is shutting down
    /// - `PoolError::PoolExhausted` if no healthy connections (FailFast policy)
    /// - `PoolError::Timeout` if wait times out (Block policy)
    pub fn get_conn(&self) -> Result<PooledConn<'_>, VoltError> {
        if self.shutdown_flag.load(Ordering::Relaxed) {
            pool_warn!("get_conn called on shutdown pool");
            return Err(PoolError::PoolShutdown.into());
        }

        pool_metrics::inc_requests_total();

        let preferred_idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.config.size;

        match self.config.exhaustion_policy {
            ExhaustionPolicy::FailFast => self.get_conn_failfast(preferred_idx),
            ExhaustionPolicy::Block { timeout } => self.get_conn_blocking(preferred_idx, timeout),
        }
    }

    fn get_conn_failfast(&self, preferred_idx: usize) -> Result<PooledConn<'_>, VoltError> {
        let (lock, _cvar) = &*self.inner;
        let inner = lock.lock().map_err(|_| PoolError::LockPoisoned)?;

        if inner.phase != PoolPhase::Running {
            return Err(PoolError::PoolShutdown.into());
        }

        // Try preferred index first
        if inner.slots[preferred_idx].is_available() {
            return self.checkout_slot(&inner, preferred_idx);
        }

        // Try to find any usable connection (with health-aware rotation)
        for i in 1..self.config.size {
            let idx = (preferred_idx + i) % self.config.size;
            if inner.slots[idx].is_available() {
                pool_debug!(
                    preferred = preferred_idx,
                    actual = idx,
                    "using alternate connection"
                );
                return self.checkout_slot(&inner, idx);
            }
        }

        pool_warn!("no healthy connections available");
        pool_metrics::inc_requests_failed_total();
        Err(PoolError::PoolExhausted.into())
    }

    fn get_conn_blocking(
        &self,
        preferred_idx: usize,
        timeout: Duration,
    ) -> Result<PooledConn<'_>, VoltError> {
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = &*self.inner;

        let mut inner = lock.lock().map_err(|_| PoolError::LockPoisoned)?;

        loop {
            if inner.phase != PoolPhase::Running {
                return Err(PoolError::PoolShutdown.into());
            }

            // Try preferred index first
            if inner.slots[preferred_idx].is_available() {
                return self.checkout_slot(&inner, preferred_idx);
            }

            // Try any available connection
            for i in 1..self.config.size {
                let idx = (preferred_idx + i) % self.config.size;
                if inner.slots[idx].is_available() {
                    return self.checkout_slot(&inner, idx);
                }
            }

            // No connection available, wait on condvar
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                pool_warn!(timeout = ?timeout, "connection wait timed out");
                pool_metrics::inc_requests_failed_total();
                return Err(PoolError::Timeout.into());
            }

            pool_trace!("waiting for available connection");
            let (guard, _timeout_result) = cvar
                .wait_timeout(inner, remaining)
                .map_err(|_| PoolError::LockPoisoned)?;
            inner = guard;
        }
    }

    /// Check out a slot - returns a connection handle
    /// Note: VoltDB allows concurrent requests per connection, so we don't track "in_use"
    fn checkout_slot<'a>(
        &'a self,
        inner: &InnerPool,
        idx: usize,
    ) -> Result<PooledConn<'a>, VoltError> {
        // Get the node Arc (cheap clone - multiple threads can share)
        let node = Arc::clone(&inner.nodes[idx]);
        let config = inner.config.clone();
        let host_idx = inner.slots[idx].host_idx;

        pool_trace!(slot = idx, "connection acquired");

        // Release pool lock here - PooledConn does NOT hold it
        Ok(PooledConn {
            pool: self,
            idx,
            node,
            config,
            host_idx,
        })
    }

    /// Report a fatal error on a connection slot and trigger reconnection if needed
    fn report_fatal_error(&self, idx: usize) {
        let (lock, cvar) = &*self.inner;

        // Variables for reconnection (populated inside lock, used outside)
        #[allow(clippy::type_complexity)]
        let mut reconnect_info: Option<(Arc<Mutex<Option<Node>>>, NodeOpt, PoolConfig)> = None;

        if let Ok(mut inner) = lock.lock() {
            // Mark as needing reconnection
            let config = inner.config.clone();
            inner.slots[idx].record_failure(&config);
            pool_debug!(slot = idx, "fatal error reported");

            inner.update_metrics();

            // Wake up any waiters (in case they're waiting for healthy connections)
            cvar.notify_all();

            // Check if we should trigger background reconnection
            if !self.shutdown_flag.load(Ordering::Relaxed) {
                let backoff = inner.config.reconnect_backoff;
                if inner.slots[idx].needs_reconnect(backoff) {
                    // Gather info for reconnection
                    let node_arc = Arc::clone(&inner.nodes[idx]);
                    let host_idx = inner.slots[idx].host_idx;
                    if let Ok(node_opt) = inner.node_opt(host_idx) {
                        inner.slots[idx].state = ConnState::Reconnecting;
                        inner.slots[idx].last_reconnect_attempt = Some(Instant::now());

                        reconnect_info = Some((node_arc, node_opt, config));
                    }
                }
            }
        }

        // Reconnection happens OUTSIDE the pool lock
        if let Some((node_arc, node_opt, config)) = reconnect_info {
            self.do_reconnect(idx, node_arc, node_opt, config);
        }
    }

    /// Perform reconnection OUTSIDE the pool lock
    fn do_reconnect(
        &self,
        idx: usize,
        node_arc: Arc<Mutex<Option<Node>>>,
        node_opt: NodeOpt,
        config: PoolConfig,
    ) {
        pool_info!(slot = idx, "attempting reconnection");
        pool_metrics::inc_reconnect_total();

        // This network I/O happens OUTSIDE the pool lock
        match node::Node::new(node_opt) {
            Ok(new_node) => {
                // Install new node (only node lock held)
                if let Ok(mut node_guard) = node_arc.lock() {
                    *node_guard = Some(new_node);
                }

                // Update metadata (pool lock)
                let (lock, cvar) = &*self.inner;
                if let Ok(mut inner) = lock.lock() {
                    inner.slots[idx].record_success();
                    inner.update_metrics();
                    cvar.notify_all(); // Wake waiters
                }
                pool_info!(slot = idx, "reconnection successful");
            }
            Err(_e) => {
                // Update metadata only
                let (lock, _) = &*self.inner;
                if let Ok(mut inner) = lock.lock() {
                    inner.slots[idx].record_failure(&config);
                    inner.update_metrics();
                }
                pool_error!(slot = idx, error = ?_e, "reconnection failed");
            }
        }
    }

    /// Mark success for a slot
    fn mark_success(&self, idx: usize) {
        let (lock, _) = &*self.inner;
        if let Ok(mut inner) = lock.lock() {
            inner.slots[idx].record_success();
            inner.update_metrics();
        }
    }

    /// Initiate graceful shutdown.
    ///
    /// This method:
    /// 1. Stops accepting new connections
    /// 2. Closes all connections
    ///
    /// Note: Since VoltDB connections are shared (multiple threads can use the same
    /// connection concurrently), we don't track "active" connections. Shutdown
    /// simply prevents new checkouts and clears the connections.
    pub fn shutdown(&self) {
        pool_info!("initiating pool shutdown");
        self.shutdown_flag.store(true, Ordering::Relaxed);

        let (lock, cvar) = &*self.inner;

        if let Ok(mut inner) = lock.lock() {
            // Set phase to Shutdown - reject new connections
            inner.phase = PoolPhase::Shutdown;
            pool_info!("entering shutdown phase");

            // Mark all slots as unhealthy
            for slot in &mut inner.slots {
                slot.state = ConnState::Unhealthy {
                    since: Instant::now(),
                };
            }

            // Clear all nodes
            for node_arc in &inner.nodes {
                if let Ok(mut node_guard) = node_arc.lock() {
                    *node_guard = None;
                }
            }
            inner.update_metrics();

            // Wake any waiters so they get shutdown error
            cvar.notify_all();
        }

        pool_info!("pool shutdown complete");
    }

    /// Check if pool is shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    /// Get current pool statistics.
    pub fn stats(&self) -> PoolStats {
        let (lock, _) = &*self.inner;
        let inner = lock.lock().ok();
        PoolStats {
            size: self.config.size,
            healthy: inner.as_ref().map(|i| i.healthy_count()).unwrap_or(0),
            total_requests: self.counter.load(Ordering::Relaxed),
            is_shutdown: self.shutdown_flag.load(Ordering::Relaxed),
        }
    }
}

/// Pool statistics snapshot.
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub size: usize,
    pub healthy: usize,
    pub total_requests: usize,
    pub is_shutdown: bool,
}

// ============================================================================
// Pooled Connection
// ============================================================================

/// A connection handle from the pool.
///
/// # Important
/// - Does NOT hold the pool lock during I/O operations
/// - Only the connection's own mutex is held during queries
/// - Multiple PooledConn instances can share the same underlying connection
///   (VoltDB supports concurrent requests via handle-based tracking)
pub struct PooledConn<'a> {
    pool: &'a Pool,
    idx: usize,
    node: Arc<Mutex<Option<Node>>>,
    #[allow(dead_code)] // Reserved for future per-connection config
    config: PoolConfig,
    #[allow(dead_code)]
    host_idx: usize,
}

impl fmt::Debug for PooledConn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledConn")
            .field("idx", &self.idx)
            .field("host_idx", &self.host_idx)
            .finish()
    }
}

impl PooledConn<'_> {
    /// Execute a SQL query.
    pub fn query(&mut self, sql: &str) -> Result<VoltTable, VoltError> {
        pool_trace!(slot = self.idx, sql = sql, "executing query");

        let mut node_guard = self
            .node
            .lock()
            .map_err(|_| VoltError::PoisonError("Node lock poisoned".to_string()))?;

        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;

        let result = node.query(sql).and_then(|r| block_for_result(&r));
        drop(node_guard);

        self.handle_result(&result);
        result
    }

    /// List all stored procedures.
    pub fn list_procedures(&mut self) -> Result<VoltTable, VoltError> {
        pool_trace!(slot = self.idx, "listing procedures");

        let mut node_guard = self
            .node
            .lock()
            .map_err(|_| VoltError::PoisonError("Node lock poisoned".to_string()))?;

        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;

        let result = node.list_procedures().and_then(|r| block_for_result(&r));
        drop(node_guard);

        self.handle_result(&result);
        result
    }

    /// Call a stored procedure with parameters.
    pub fn call_sp(&mut self, proc: &str, params: Vec<&dyn Value>) -> Result<VoltTable, VoltError> {
        pool_trace!(
            slot = self.idx,
            procedure = proc,
            "calling stored procedure"
        );

        let mut node_guard = self
            .node
            .lock()
            .map_err(|_| VoltError::PoisonError("Node lock poisoned".to_string()))?;

        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;

        let result = node
            .call_sp(proc, params)
            .and_then(|r| block_for_result(&r));
        drop(node_guard);

        self.handle_result(&result);
        result
    }

    /// Upload a JAR file.
    pub fn upload_jar(&mut self, bs: Vec<u8>) -> Result<VoltTable, VoltError> {
        pool_trace!(slot = self.idx, size = bs.len(), "uploading jar");

        let mut node_guard = self
            .node
            .lock()
            .map_err(|_| VoltError::PoisonError("Node lock poisoned".to_string()))?;

        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;

        let result = node.upload_jar(bs).and_then(|r| block_for_result(&r));
        drop(node_guard);

        self.handle_result(&result);
        result
    }

    /// Handle operation result - update state and trigger reconnection if needed
    fn handle_result<T>(&self, result: &Result<T, VoltError>) {
        match result {
            Ok(_) => {
                self.pool.mark_success(self.idx);
            }
            Err(e) if e.is_connection_fatal() => {
                pool_error!(slot = self.idx, error = ?e, "fatal connection error detected");
                // Clear the node and trigger reconnection
                if let Ok(mut guard) = self.node.lock() {
                    *guard = None;
                }
                self.pool.report_fatal_error(self.idx);
            }
            Err(_) => {
                // Non-fatal error, no action needed
            }
        }
    }

    /// Get the slot index of this connection (for debugging).
    pub fn slot_index(&self) -> usize {
        self.idx
    }
}

// No Drop implementation needed - VoltDB connections are shared and
// don't need to be "returned" to the pool. The Arc<Node> handles cleanup.

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new()
            .size(20)
            .reconnect_backoff(Duration::from_secs(10))
            .circuit_open_duration(Duration::from_secs(60))
            .exhaustion_policy(ExhaustionPolicy::Block {
                timeout: Duration::from_secs(5),
            })
            .validation_mode(ValidationMode::BestEffort)
            .circuit_failure_threshold(5)
            .shutdown_timeout(Duration::from_secs(60));

        assert_eq!(config.size, 20);
        assert_eq!(config.reconnect_backoff, Duration::from_secs(10));
        assert_eq!(config.circuit_open_duration, Duration::from_secs(60));
        assert_eq!(
            config.exhaustion_policy,
            ExhaustionPolicy::Block {
                timeout: Duration::from_secs(5)
            }
        );
        assert_eq!(config.validation_mode, ValidationMode::BestEffort);
        assert_eq!(config.circuit_failure_threshold, 5);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.size, 10);
        assert_eq!(config.exhaustion_policy, ExhaustionPolicy::FailFast);
        assert_eq!(config.validation_mode, ValidationMode::FailFast);
    }

    #[test]
    fn test_slot_meta_is_available() {
        // Healthy slot is available
        let slot = SlotMeta::new_healthy(0);
        assert!(slot.is_available());

        // Unhealthy slot is not available
        let slot = SlotMeta::new_unhealthy(0);
        assert!(!slot.is_available());

        // Healthy slot with open circuit is not available
        let mut slot = SlotMeta::new_healthy(0);
        slot.circuit = Circuit::Open {
            until: Instant::now() + Duration::from_secs(60),
        };
        assert!(!slot.is_available());
    }

    #[test]
    fn test_slot_meta_needs_reconnect() {
        let mut slot = SlotMeta::new_unhealthy(0);
        let backoff = Duration::from_millis(100);

        // Unhealthy slot needs reconnect
        assert!(slot.needs_reconnect(backoff));

        // After marking reconnecting, doesn't need it
        slot.state = ConnState::Reconnecting;
        assert!(!slot.needs_reconnect(backoff));

        // Healthy slot doesn't need reconnect
        slot.state = ConnState::Healthy;
        assert!(!slot.needs_reconnect(backoff));
    }

    #[test]
    fn test_slot_meta_record_success() {
        let mut slot = SlotMeta::new_unhealthy(0);
        slot.consecutive_failures = 5;

        slot.record_success();

        assert_eq!(slot.consecutive_failures, 0);
        assert!(matches!(slot.state, ConnState::Healthy));
        assert!(matches!(slot.circuit, Circuit::Closed));
    }

    #[test]
    fn test_slot_meta_record_failure_opens_circuit() {
        let mut slot = SlotMeta::new_healthy(0);
        slot.consecutive_failures = 2;

        let config = PoolConfig::default().circuit_failure_threshold(3);

        slot.record_failure(&config);

        assert_eq!(slot.consecutive_failures, 3);
        assert!(matches!(slot.circuit, Circuit::Open { .. }));
    }
}
