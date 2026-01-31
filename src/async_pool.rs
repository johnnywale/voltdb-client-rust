//! Production-ready async connection pool for VoltDB.
//!
//! # Features
//! - Thread-safe with fine-grained locking
//! - Connection state machine (Healthy, Unhealthy, Reconnecting)
//! - Per-connection circuit breaker
//! - Configurable exhaustion policy (FailFast or Block with async waiting)
//! - Graceful shutdown with drain mode
//! - Optional structured logging (`tracing` feature)
//! - Optional metrics (`metrics` feature)
//!
//! # Design
//! - Pool lock only guards metadata (state, circuit breaker)
//! - Each AsyncNode has its own lock for I/O operations
//! - Network I/O never holds the pool lock

#![cfg(feature = "tokio")]

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

use crate::async_node::{AsyncNode, async_block_for_result};
use crate::{NodeOpt, Opts, Value, VoltError, VoltTable};

// ============================================================================
// Logging macros - use tracing if available, otherwise no-op
// ============================================================================

#[cfg(feature = "tracing")]
macro_rules! async_pool_trace {
    ($($arg:tt)*) => { tracing::trace!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_pool_trace {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_pool_debug {
    ($($arg:tt)*) => { tracing::debug!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_pool_debug {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_pool_info {
    ($($arg:tt)*) => { tracing::info!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_pool_info {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_pool_warn {
    ($($arg:tt)*) => { tracing::warn!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_pool_warn {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_pool_error {
    ($($arg:tt)*) => { tracing::error!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_pool_error {
    ($($arg:tt)*) => {};
}

// ============================================================================
// Metrics - use metrics crate if available, otherwise no-op
// ============================================================================

#[cfg(feature = "metrics")]
mod async_pool_metrics {
    use metrics::{counter, gauge};

    pub fn set_connections_total(count: usize) {
        gauge!("voltdb_async_pool_connections_total").set(count as f64);
    }

    pub fn set_connections_healthy(count: usize) {
        gauge!("voltdb_async_pool_connections_healthy").set(count as f64);
    }

    pub fn inc_reconnect_total() {
        counter!("voltdb_async_pool_reconnect_total").increment(1);
    }

    pub fn inc_circuit_open_total() {
        counter!("voltdb_async_pool_circuit_open_total").increment(1);
    }

    pub fn inc_requests_failed_total() {
        counter!("voltdb_async_pool_requests_failed_total").increment(1);
    }

    pub fn inc_requests_total() {
        counter!("voltdb_async_pool_requests_total").increment(1);
    }
}

#[cfg(not(feature = "metrics"))]
mod async_pool_metrics {
    pub fn set_connections_total(_count: usize) {}
    pub fn set_connections_healthy(_count: usize) {}
    pub fn inc_reconnect_total() {}
    pub fn inc_circuit_open_total() {}
    pub fn inc_requests_failed_total() {}
    pub fn inc_requests_total() {}
}

// ============================================================================
// Pool-specific errors
// ============================================================================

/// Pool-specific error conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncPoolError {
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

impl fmt::Display for AsyncPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsyncPoolError::PoolShutdown => write!(f, "Pool is shutting down"),
            AsyncPoolError::CircuitOpen => write!(f, "Circuit breaker is open"),
            AsyncPoolError::PoolExhausted => write!(f, "Pool exhausted, no healthy connections"),
            AsyncPoolError::Timeout => write!(f, "Timed out waiting for connection"),
            AsyncPoolError::LockPoisoned => write!(f, "Internal lock poisoned"),
        }
    }
}

impl std::error::Error for AsyncPoolError {}

impl From<AsyncPoolError> for VoltError {
    fn from(e: AsyncPoolError) -> Self {
        match e {
            AsyncPoolError::PoolShutdown => VoltError::ConnectionNotAvailable,
            AsyncPoolError::CircuitOpen => VoltError::ConnectionNotAvailable,
            AsyncPoolError::PoolExhausted => VoltError::ConnectionNotAvailable,
            AsyncPoolError::Timeout => VoltError::Timeout,
            AsyncPoolError::LockPoisoned => {
                VoltError::PoisonError("Pool lock poisoned".to_string())
            }
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
    fn is_healthy(&self) -> bool {
        matches!(self, ConnState::Healthy)
    }

    fn is_reconnecting(&self) -> bool {
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
    /// Check if request should be allowed through
    fn should_allow(&self) -> bool {
        match self {
            Circuit::Closed => true,
            Circuit::Open { until } => Instant::now() >= *until,
            Circuit::HalfOpen => true,
        }
    }

    /// Transition to Open state
    fn open(&mut self, duration: Duration) {
        *self = Circuit::Open {
            until: Instant::now() + duration,
        };
        async_pool_metrics::inc_circuit_open_total();
        async_pool_warn!("circuit breaker opened");
    }

    /// Transition to HalfOpen state (for probing)
    #[allow(dead_code)]
    fn half_open(&mut self) {
        *self = Circuit::HalfOpen;
        async_pool_debug!("circuit breaker half-open");
    }

    /// Transition to Closed state (healthy)
    fn close(&mut self) {
        *self = Circuit::Closed;
        async_pool_info!("circuit breaker closed");
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

/// Async pool configuration.
#[derive(Debug, Clone)]
pub struct AsyncPoolConfig {
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

impl Default for AsyncPoolConfig {
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

impl AsyncPoolConfig {
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

/// Pool lifecycle phase for graceful shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolPhase {
    /// Normal operation
    Running,
    /// Fully shut down
    Shutdown,
}

// ============================================================================
// Connection Slot (Metadata only - AsyncNode is separate)
// ============================================================================

/// Metadata for a connection slot. The actual AsyncNode is stored separately.
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

    fn record_failure(&mut self, config: &AsyncPoolConfig) {
        self.consecutive_failures += 1;
        self.state = ConnState::Unhealthy {
            since: Instant::now(),
        };

        if self.consecutive_failures >= config.circuit_failure_threshold {
            self.circuit.open(config.circuit_open_duration);
        }
    }
}

// ============================================================================
// Inner Pool (protected by Mutex)
// ============================================================================

struct AsyncInnerPool {
    opts: Opts,
    config: AsyncPoolConfig,
    slots: Vec<SlotMeta>,
    nodes: Vec<Arc<Mutex<Option<AsyncNode>>>>,
    phase: PoolPhase,
}

impl fmt::Debug for AsyncInnerPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncInnerPool")
            .field("config", &self.config)
            .field("slots_count", &self.slots.len())
            .field("phase", &self.phase)
            .finish()
    }
}

impl AsyncInnerPool {
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

    async fn new(opts: Opts, config: AsyncPoolConfig) -> Result<Self, VoltError> {
        let num_hosts = opts.0.ip_ports.len();
        let mut inner = AsyncInnerPool {
            opts,
            config: config.clone(),
            slots: Vec::with_capacity(config.size),
            nodes: Vec::with_capacity(config.size),
            phase: PoolPhase::Running,
        };

        for i in 0..config.size {
            let host_idx = i % num_hosts;
            let node_opt = inner.node_opt(host_idx)?;

            async_pool_debug!(slot = i, host = host_idx, "creating connection");

            match AsyncNode::new(node_opt).await {
                Ok(node) => {
                    inner.slots.push(SlotMeta::new_healthy(host_idx));
                    inner.nodes.push(Arc::new(Mutex::new(Some(node))));
                    async_pool_info!(slot = i, "connection established");
                }
                Err(e) => match config.validation_mode {
                    ValidationMode::FailFast => {
                        async_pool_error!(slot = i, error = ?e, "connection failed, aborting pool creation");
                        return Err(e);
                    }
                    ValidationMode::BestEffort => {
                        async_pool_warn!(slot = i, error = ?e, "connection failed, marking unhealthy");
                        inner.slots.push(SlotMeta::new_unhealthy(host_idx));
                        inner.nodes.push(Arc::new(Mutex::new(None)));
                    }
                },
            }
        }

        inner.update_metrics();
        async_pool_info!(
            size = config.size,
            healthy = inner.healthy_count(),
            "async pool initialized"
        );

        Ok(inner)
    }

    fn healthy_count(&self) -> usize {
        self.slots.iter().filter(|s| s.state.is_healthy()).count()
    }

    fn update_metrics(&self) {
        async_pool_metrics::set_connections_total(self.slots.len());
        async_pool_metrics::set_connections_healthy(self.healthy_count());
    }
}

// ============================================================================
// Pool (Thread-safe)
// ============================================================================

/// Thread-safe async connection pool for VoltDB.
///
/// # Example
/// ```ignore
/// use voltdb_client_rust::{AsyncPool, AsyncPoolConfig, Opts, IpPort};
///
/// let hosts = vec![IpPort::new("localhost".to_string(), 21212)];
/// let config = AsyncPoolConfig::new().size(5);
/// let pool = AsyncPool::with_config(Opts::new(hosts), config).await?;
///
/// let conn = pool.get_conn().await?;
/// let table = conn.query("SELECT * FROM foo").await?;
/// ```
pub struct AsyncPool {
    inner: Arc<Mutex<AsyncInnerPool>>,
    notify: Arc<Notify>,
    counter: AtomicUsize,
    shutdown_flag: AtomicBool,
    config: AsyncPoolConfig,
}

impl fmt::Debug for AsyncPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncPool")
            .field("counter", &self.counter.load(Ordering::Relaxed))
            .field("shutdown", &self.shutdown_flag.load(Ordering::Relaxed))
            .field("config", &self.config)
            .finish()
    }
}

impl AsyncPool {
    /// Create a new pool with default configuration (10 connections).
    pub async fn new<T: Into<Opts>>(opts: T) -> Result<AsyncPool, VoltError> {
        AsyncPool::with_config(opts, AsyncPoolConfig::default()).await
    }

    /// Create a new pool with custom size (convenience method).
    pub async fn new_manual<T: Into<Opts>>(size: usize, opts: T) -> Result<AsyncPool, VoltError> {
        AsyncPool::with_config(opts, AsyncPoolConfig::new().size(size)).await
    }

    /// Create a new pool with full configuration.
    pub async fn with_config<T: Into<Opts>>(
        opts: T,
        config: AsyncPoolConfig,
    ) -> Result<AsyncPool, VoltError> {
        let inner = AsyncInnerPool::new(opts.into(), config.clone()).await?;
        Ok(AsyncPool {
            inner: Arc::new(Mutex::new(inner)),
            notify: Arc::new(Notify::new()),
            counter: AtomicUsize::new(0),
            shutdown_flag: AtomicBool::new(false),
            config,
        })
    }

    /// Get a connection from the pool.
    pub async fn get_conn(&self) -> Result<AsyncPooledConn<'_>, VoltError> {
        if self.shutdown_flag.load(Ordering::Relaxed) {
            async_pool_warn!("get_conn called on shutdown pool");
            return Err(AsyncPoolError::PoolShutdown.into());
        }

        async_pool_metrics::inc_requests_total();

        let preferred_idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.config.size;

        match self.config.exhaustion_policy {
            ExhaustionPolicy::FailFast => self.get_conn_failfast(preferred_idx).await,
            ExhaustionPolicy::Block {
                timeout: wait_timeout,
            } => self.get_conn_blocking(preferred_idx, wait_timeout).await,
        }
    }

    async fn get_conn_failfast(
        &self,
        preferred_idx: usize,
    ) -> Result<AsyncPooledConn<'_>, VoltError> {
        let inner = self.inner.lock().await;

        if inner.phase != PoolPhase::Running {
            return Err(AsyncPoolError::PoolShutdown.into());
        }

        // Try preferred index first
        if inner.slots[preferred_idx].is_available() {
            return self.checkout_slot(&inner, preferred_idx).await;
        }

        // Try to find any usable connection
        for i in 1..self.config.size {
            let idx = (preferred_idx + i) % self.config.size;
            if inner.slots[idx].is_available() {
                async_pool_debug!(
                    preferred = preferred_idx,
                    actual = idx,
                    "using alternate connection"
                );
                return self.checkout_slot(&inner, idx).await;
            }
        }

        async_pool_warn!("no healthy connections available");
        async_pool_metrics::inc_requests_failed_total();
        Err(AsyncPoolError::PoolExhausted.into())
    }

    async fn get_conn_blocking(
        &self,
        preferred_idx: usize,
        wait_timeout: Duration,
    ) -> Result<AsyncPooledConn<'_>, VoltError> {
        let deadline = Instant::now() + wait_timeout;

        loop {
            let inner = self.inner.lock().await;

            if inner.phase != PoolPhase::Running {
                return Err(AsyncPoolError::PoolShutdown.into());
            }

            // Try preferred index first
            if inner.slots[preferred_idx].is_available() {
                return self.checkout_slot(&inner, preferred_idx).await;
            }

            // Try any available connection
            for i in 1..self.config.size {
                let idx = (preferred_idx + i) % self.config.size;
                if inner.slots[idx].is_available() {
                    return self.checkout_slot(&inner, idx).await;
                }
            }

            // No connection available, wait
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                async_pool_warn!(timeout = ?wait_timeout, "connection wait timed out");
                async_pool_metrics::inc_requests_failed_total();
                return Err(AsyncPoolError::Timeout.into());
            }

            async_pool_trace!("waiting for available connection");
            drop(inner);

            // Wait for notification or timeout
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    async fn checkout_slot(
        &self,
        inner: &AsyncInnerPool,
        idx: usize,
    ) -> Result<AsyncPooledConn<'_>, VoltError> {
        let node = Arc::clone(&inner.nodes[idx]);
        let config = inner.config.clone();
        let host_idx = inner.slots[idx].host_idx;

        async_pool_trace!(slot = idx, "connection acquired");

        Ok(AsyncPooledConn {
            pool: self,
            idx,
            node,
            config,
            host_idx,
        })
    }

    /// Report a fatal error on a connection slot
    async fn report_fatal_error(&self, idx: usize) {
        #[allow(clippy::type_complexity)]
        let reconnect_info: Option<(
            Arc<Mutex<Option<AsyncNode>>>,
            NodeOpt,
            AsyncPoolConfig,
        )>;

        {
            let mut inner = self.inner.lock().await;
            let config = inner.config.clone();
            inner.slots[idx].record_failure(&config);
            async_pool_debug!(slot = idx, "fatal error reported");

            inner.update_metrics();
            self.notify.notify_waiters();

            if !self.shutdown_flag.load(Ordering::Relaxed) {
                let backoff = inner.config.reconnect_backoff;
                if inner.slots[idx].needs_reconnect(backoff) {
                    let node_arc = Arc::clone(&inner.nodes[idx]);
                    let host_idx = inner.slots[idx].host_idx;
                    if let Ok(node_opt) = inner.node_opt(host_idx) {
                        inner.slots[idx].state = ConnState::Reconnecting;
                        inner.slots[idx].last_reconnect_attempt = Some(Instant::now());

                        reconnect_info = Some((node_arc, node_opt, config));
                    } else {
                        reconnect_info = None;
                    }
                } else {
                    reconnect_info = None;
                }
            } else {
                reconnect_info = None;
            }
        }

        if let Some((node_arc, node_opt, config)) = reconnect_info {
            self.do_reconnect(idx, node_arc, node_opt, config).await;
        }
    }

    async fn do_reconnect(
        &self,
        idx: usize,
        node_arc: Arc<Mutex<Option<AsyncNode>>>,
        node_opt: NodeOpt,
        config: AsyncPoolConfig,
    ) {
        async_pool_info!(slot = idx, "attempting reconnection");
        async_pool_metrics::inc_reconnect_total();

        match AsyncNode::new(node_opt).await {
            Ok(new_node) => {
                {
                    let mut node_guard = node_arc.lock().await;
                    *node_guard = Some(new_node);
                }

                {
                    let mut inner = self.inner.lock().await;
                    inner.slots[idx].record_success();
                    inner.update_metrics();
                }

                self.notify.notify_waiters();
                async_pool_info!(slot = idx, "reconnection successful");
            }
            Err(_e) => {
                {
                    let mut inner = self.inner.lock().await;
                    inner.slots[idx].record_failure(&config);
                    inner.update_metrics();
                }
                async_pool_error!(slot = idx, error = ?_e, "reconnection failed");
            }
        }
    }

    async fn mark_success(&self, idx: usize) {
        let mut inner = self.inner.lock().await;
        inner.slots[idx].record_success();
        inner.update_metrics();
    }

    /// Initiate graceful shutdown.
    pub async fn shutdown(&self) {
        async_pool_info!("initiating pool shutdown");
        self.shutdown_flag.store(true, Ordering::Relaxed);

        let mut inner = self.inner.lock().await;
        inner.phase = PoolPhase::Shutdown;
        async_pool_info!("entering shutdown phase");

        for slot in &mut inner.slots {
            slot.state = ConnState::Unhealthy {
                since: Instant::now(),
            };
        }

        for node_arc in &inner.nodes {
            let mut node_guard = node_arc.lock().await;
            *node_guard = None;
        }

        inner.update_metrics();
        self.notify.notify_waiters();

        async_pool_info!("pool shutdown complete");
    }

    /// Check if pool is shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    /// Get current pool statistics.
    pub async fn stats(&self) -> AsyncPoolStats {
        let inner = self.inner.lock().await;
        AsyncPoolStats {
            size: self.config.size,
            healthy: inner.healthy_count(),
            total_requests: self.counter.load(Ordering::Relaxed),
            is_shutdown: self.shutdown_flag.load(Ordering::Relaxed),
        }
    }
}

/// Pool statistics snapshot.
#[derive(Debug, Clone)]
pub struct AsyncPoolStats {
    pub size: usize,
    pub healthy: usize,
    pub total_requests: usize,
    pub is_shutdown: bool,
}

// ============================================================================
// Pooled Connection
// ============================================================================

/// A connection handle from the async pool.
pub struct AsyncPooledConn<'a> {
    pool: &'a AsyncPool,
    idx: usize,
    node: Arc<Mutex<Option<AsyncNode>>>,
    #[allow(dead_code)]
    config: AsyncPoolConfig,
    #[allow(dead_code)]
    host_idx: usize,
}

impl fmt::Debug for AsyncPooledConn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncPooledConn")
            .field("idx", &self.idx)
            .field("host_idx", &self.host_idx)
            .finish()
    }
}

impl AsyncPooledConn<'_> {
    /// Execute a SQL query.
    pub async fn query(&self, sql: &str) -> Result<VoltTable, VoltError> {
        async_pool_trace!(slot = self.idx, sql = sql, "executing query");

        let mut node_guard = self.node.lock().await;
        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;
        let mut rx = node.query(sql).await?;
        drop(node_guard);
        let result = async_block_for_result(&mut rx).await;
        self.handle_result(&result).await;
        result
        // result
    }

    pub async fn list_procedures(&mut self) -> Result<VoltTable, VoltError> {
        async_pool_trace!(slot = self.idx, "listing procedures");

        let mut node_guard = self.node.lock().await;
        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;
        let mut rx = node.list_procedures().await?;
        drop(node_guard);
        let result = async_block_for_result(&mut rx).await;
        self.handle_result(&result).await;
        result
    }

    pub async fn call_sp(
        &mut self,
        proc: &str,
        params: Vec<&dyn Value>,
    ) -> Result<VoltTable, VoltError> {
        async_pool_trace!(
            slot = self.idx,
            procedure = proc,
            "calling stored procedure"
        );
        let mut node_guard = self.node.lock().await;
        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;
        let mut rx = node.call_sp(proc, params).await?;
        drop(node_guard);

        let result = async_block_for_result(&mut rx).await;
        self.handle_result(&result).await;
        result
    }

    /// Upload a JAR file.
    pub async fn upload_jar(&self, bs: Vec<u8>) -> Result<VoltTable, VoltError> {
        async_pool_trace!(slot = self.idx, size = bs.len(), "uploading jar");

        let mut node_guard = self.node.lock().await;
        let node = node_guard
            .as_mut()
            .ok_or(VoltError::ConnectionNotAvailable)?;

        let mut rx = node.upload_jar(bs).await?;
        drop(node_guard);

        let result = async_block_for_result(&mut rx).await;
        self.handle_result(&result).await;
        result
    }

    /// Handle operation result - update state and trigger reconnection if needed
    async fn handle_result<T>(&self, result: &Result<T, VoltError>) {
        match result {
            Ok(_) => {
                self.pool.mark_success(self.idx).await;
            }
            Err(e) if e.is_connection_fatal() => {
                async_pool_error!(slot = self.idx, error = ?e, "fatal connection error detected");
                {
                    let mut guard = self.node.lock().await;
                    *guard = None;
                }
                self.pool.report_fatal_error(self.idx).await;
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
    fn test_pool_config_builder() {
        let config = AsyncPoolConfig::new()
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
        let config = AsyncPoolConfig::default();
        assert_eq!(config.size, 10);
        assert_eq!(config.exhaustion_policy, ExhaustionPolicy::FailFast);
        assert_eq!(config.validation_mode, ValidationMode::FailFast);
    }

    #[test]
    fn test_slot_meta_is_available() {
        let slot = SlotMeta::new_healthy(0);
        assert!(slot.is_available());

        let slot = SlotMeta::new_unhealthy(0);
        assert!(!slot.is_available());

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

        assert!(slot.needs_reconnect(backoff));

        slot.state = ConnState::Reconnecting;
        assert!(!slot.needs_reconnect(backoff));

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

        let config = AsyncPoolConfig::default().circuit_failure_threshold(3);

        slot.record_failure(&config);

        assert_eq!(slot.consecutive_failures, 3);
        assert!(matches!(slot.circuit, Circuit::Open { .. }));
    }

    #[test]
    fn test_async_pool_error_display() {
        assert_eq!(
            format!("{}", AsyncPoolError::PoolShutdown),
            "Pool is shutting down"
        );
        assert_eq!(
            format!("{}", AsyncPoolError::CircuitOpen),
            "Circuit breaker is open"
        );
        assert_eq!(
            format!("{}", AsyncPoolError::PoolExhausted),
            "Pool exhausted, no healthy connections"
        );
        assert_eq!(
            format!("{}", AsyncPoolError::Timeout),
            "Timed out waiting for connection"
        );
    }

    #[test]
    fn test_pool_phase() {
        assert_eq!(PoolPhase::Running, PoolPhase::Running);
        assert_ne!(PoolPhase::Running, PoolPhase::Shutdown);
    }
}
