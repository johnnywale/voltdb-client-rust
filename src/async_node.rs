#![cfg(feature = "tokio")]
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::encode::{Value, VoltError};
use crate::node::{ConnInfo, NodeOpt};
use crate::procedure_invocation::new_procedure_invocation;
use crate::protocol::{PING_HANDLE, build_auth_message, parse_auth_response};
use crate::response::VoltResponseInfo;
use crate::table::{VoltTable, new_volt_table};
use crate::volt_param;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

// ============================================================================
// Logging macros - use tracing if available, otherwise no-op
// ============================================================================

#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! async_node_trace {
    ($($arg:tt)*) => { tracing::trace!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! async_node_trace {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_node_debug {
    ($($arg:tt)*) => { tracing::debug!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_node_debug {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_node_warn {
    ($($arg:tt)*) => { tracing::warn!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_node_warn {
    ($($arg:tt)*) => {};
}

#[cfg(feature = "tracing")]
macro_rules! async_node_error {
    ($($arg:tt)*) => { tracing::error!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! async_node_error {
    ($($arg:tt)*) => {};
}

/// Configuration constants
const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024; // 50MB message size limit
const WRITE_BUFFER_SIZE: usize = 1024; // Write queue capacity
const BATCH_WRITE_THRESHOLD: usize = 8192; // Batch write threshold
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30); // Default timeout
#[allow(dead_code)]
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60); // TCP keepalive interval

/// Write command for the writer task
#[allow(dead_code)]
enum WriteCommand {
    Data(Vec<u8>),
    Flush,
}

/// Async network request tracking
#[allow(dead_code)]
struct AsyncNetworkRequest {
    handle: i64,
    query: bool,
    sync: bool,
    num_bytes: i32,
    channel: mpsc::Sender<VoltTable>,
    created_at: Instant, // Used for timeout detection
}

impl Debug for AsyncNetworkRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncNetworkRequest")
            .field("handle", &self.handle)
            .field("query", &self.query)
            .field("age_ms", &self.created_at.elapsed().as_millis())
            .finish()
    }
}

/// Async VoltDB connection node
pub struct AsyncNode {
    /// Write command sender channel
    write_tx: mpsc::Sender<WriteCommand>,
    /// Connection info from authentication
    info: ConnInfo,
    /// Request map (using DashMap to reduce lock contention)
    requests: Arc<DashMap<i64, AsyncNetworkRequest>>,
    /// Stop signal for background tasks
    stop: Arc<watch::Sender<bool>>,
    /// Request sequence number counter
    counter: Arc<AtomicI64>,
    /// Pending request count (used for load balancing)
    pending_requests: Arc<AtomicUsize>,
}

impl Debug for AsyncNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncNode")
            .field(
                "pending_requests",
                &self.pending_requests.load(Ordering::Relaxed),
            )
            .field("total_requests", &self.requests.len())
            .finish()
    }
}

impl Drop for AsyncNode {
    fn drop(&mut self) {
        // Signal background tasks to stop
        let _ = self.stop.send(true);
    }
}

impl AsyncNode {
    /// Create a new async connection to VoltDB server
    pub async fn new(opt: NodeOpt) -> Result<AsyncNode, VoltError> {
        let addr = format!("{}:{}", opt.ip_port.ip_host, opt.ip_port.port);

        // Build authentication message
        let auth_msg = build_auth_message(opt.user.as_deref(), opt.pass.as_deref())?;

        // Async connect
        let mut stream = TcpStream::connect(&addr).await?;

        // TCP optimization configuration
        stream.set_nodelay(true)?; // Disable Nagle algorithm to reduce latency
        // if let Err(e) = stream.set_keepalive(Some(KEEPALIVE_INTERVAL)) {
        //     eprintln!("Warning: Failed to set keepalive: {}", e);
        // }

        // Async authentication handshake
        stream.write_all(&auth_msg).await?;
        stream.flush().await?;

        // Read authentication response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let read = BigEndian::read_u32(&len_buf) as usize;

        let mut all = vec![0; read];
        stream.read_exact(&mut all).await?;

        // Parse authentication response
        let info = parse_auth_response(&all)?;

        // Split into read and write halves
        let (read_half, write_half) = tokio::io::split(stream);

        // Create channels
        let requests = Arc::new(DashMap::new());
        let (stop_tx, stop_rx) = watch::channel(false);
        let (write_tx, write_rx) = mpsc::channel(WRITE_BUFFER_SIZE);

        let node = AsyncNode {
            stop: Arc::new(stop_tx),
            write_tx,
            info,
            requests: requests.clone(),
            counter: Arc::new(AtomicI64::new(1)),
            pending_requests: Arc::new(AtomicUsize::new(0)),
        };

        // Start background tasks
        node.spawn_writer(write_half, write_rx, stop_rx.clone());
        node.spawn_reader(read_half, stop_rx.clone());
        node.spawn_timeout_checker(stop_rx);

        Ok(node)
    }

    /// Get the next sequence number for request tracking
    #[inline]
    pub fn get_sequence(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current pending request count (used for load balancing)
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.pending_requests.load(Ordering::Relaxed)
    }

    /// Get connection info from authentication
    pub fn conn_info(&self) -> &ConnInfo {
        &self.info
    }

    /// List all stored procedures available in the database
    pub async fn list_procedures(&self) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        self.call_sp("@SystemCatalog", volt_param!("PROCEDURES"))
            .await
    }

    /// Call a stored procedure with parameters
    pub async fn call_sp(
        &self,
        query: &str,
        param: Vec<&dyn Value>,
    ) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        self.call_sp_with_timeout(query, param, DEFAULT_TIMEOUT)
            .await
    }

    /// Call a stored procedure with custom timeout
    pub async fn call_sp_with_timeout(
        &self,
        query: &str,
        param: Vec<&dyn Value>,
        _timeout_duration: Duration,
    ) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        let req = self.get_sequence();
        let mut proc = new_procedure_invocation(req, false, &param, query);

        // Create response channel
        let (tx, rx) = mpsc::channel(1);

        let seq = AsyncNetworkRequest {
            query: true,
            handle: req,
            num_bytes: proc.slen,
            sync: true,
            channel: tx,
            created_at: Instant::now(),
        };

        // Insert into request map
        self.requests.insert(req, seq);
        self.pending_requests.fetch_add(1, Ordering::Relaxed);

        // Send request data
        let bs = proc.bytes();
        self.write_tx
            .send(WriteCommand::Data(bs))
            .await
            .map_err(|_| VoltError::connection_closed())?;

        Ok(rx)
    }

    /// Upload a JAR file containing stored procedure classes
    pub async fn upload_jar(&self, bs: Vec<u8>) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        self.call_sp("@UpdateClasses", volt_param!(bs, "")).await
    }

    /// Execute an ad-hoc SQL query
    pub async fn query(&self, sql: &str) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        let mut zero_vec: Vec<&dyn Value> = Vec::new();
        zero_vec.push(&sql);
        self.call_sp("@AdHoc", zero_vec).await
    }

    /// Send a ping to keep the connection alive
    pub async fn ping(&self) -> Result<(), VoltError> {
        let zero_vec: Vec<&dyn Value> = Vec::new();
        let mut proc = new_procedure_invocation(PING_HANDLE, false, &zero_vec, "@Ping");
        let bs = proc.bytes();

        self.write_tx
            .send(WriteCommand::Data(bs))
            .await
            .map_err(|_| VoltError::connection_closed())?;

        Ok(())
    }

    /// Shutdown the connection gracefully
    pub async fn shutdown(&self) -> Result<(), VoltError> {
        let _ = self.stop.send(true);
        Ok(())
    }

    /// Spawn the writer task (supports batch write optimization)
    fn spawn_writer(
        &self,
        mut write_half: WriteHalf<TcpStream>,
        mut write_rx: mpsc::Receiver<WriteCommand>,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        tokio::spawn(async move {
            let mut batch_buffer = Vec::with_capacity(BATCH_WRITE_THRESHOLD * 2);

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    cmd = write_rx.recv() => {
                        match cmd {
                            Some(WriteCommand::Data(bytes)) => {
                                batch_buffer.extend_from_slice(&bytes);

                                // Try to batch collect more data
                                while batch_buffer.len() < BATCH_WRITE_THRESHOLD {
                                    match write_rx.try_recv() {
                                        Ok(WriteCommand::Data(more_bytes)) => {
                                            batch_buffer.extend_from_slice(&more_bytes);
                                        }
                                        Ok(WriteCommand::Flush) => break,
                                        Err(_) => break,
                                    }
                                }

                                // Batch write
                                if let Err(_e) = write_half.write_all(&batch_buffer).await {
                                    async_node_error!(error = %_e, "write error");
                                    break;
                                }
                                batch_buffer.clear();
                            }
                            Some(WriteCommand::Flush) => {
                                if !batch_buffer.is_empty() {
                                    if let Err(_e) = write_half.write_all(&batch_buffer).await {
                                        async_node_error!(error = %_e, "flush error");
                                        break;
                                    }
                                    batch_buffer.clear();
                                }
                                let _ = write_half.flush().await;
                            }
                            None => break,
                        }
                    }
                }
            }

            async_node_debug!("writer task terminated");
        });
    }

    /// Spawn the reader task for receiving responses
    fn spawn_reader(&self, mut read_half: ReadHalf<TcpStream>, mut stop_rx: watch::Receiver<bool>) {
        let requests = Arc::clone(&self.requests);
        let pending_requests = Arc::clone(&self.pending_requests);

        tokio::spawn(async move {
            let reason = loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break "shutdown requested";
                        }
                    }
                    result = Self::async_job(&mut read_half, &requests, &pending_requests) => {
                        if let Err(_e) = result {
                            if !*stop_rx.borrow() {
                                async_node_error!(error = %_e, "read error");
                            }
                            break "connection error";
                        }
                    }
                }
            };

            // Cleanup all pending requests
            Self::cleanup_requests(&requests, &pending_requests, reason).await;
        });
    }

    /// Spawn the timeout checker task for cleaning up stale requests
    fn spawn_timeout_checker(&self, mut stop_rx: watch::Receiver<bool>) {
        let requests = Arc::clone(&self.requests);
        let pending_requests = Arc::clone(&self.pending_requests);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let now = Instant::now();
                        let mut expired = Vec::new();

                        // Find expired requests
                        for entry in requests.iter() {
                            let age = now.duration_since(entry.created_at);
                            if age > DEFAULT_TIMEOUT * 2 {
                                expired.push(*entry.key());
                            }
                        }

                        // Cleanup expired requests
                        for handle in expired {
                            if let Some((_, _req)) = requests.remove(&handle) {
                                pending_requests.fetch_sub(1, Ordering::Relaxed);
                                async_node_warn!(
                                    handle = handle,
                                    elapsed = ?now.duration_since(_req.created_at),
                                    "request timed out"
                                );
                                // Channel drop will notify the caller
                            }
                        }
                    }
                }
            }
        });
    }

    /// Read and process a single response from the server
    async fn async_job(
        tcp: &mut ReadHalf<TcpStream>,
        requests: &Arc<DashMap<i64, AsyncNetworkRequest>>,
        pending_requests: &Arc<AtomicUsize>,
    ) -> Result<(), VoltError> {
        // Read message length
        let mut len_buf = [0u8; 4];
        tcp.read_exact(&mut len_buf).await?;
        let msg_len = BigEndian::read_u32(&len_buf) as usize;

        // Safety check for message size
        if msg_len > MAX_MESSAGE_SIZE {
            return Err(VoltError::MessageTooLarge(msg_len));
        }

        if msg_len == 0 {
            return Ok(());
        }

        // Use BytesMut to reduce memory copying
        let mut buf = BytesMut::with_capacity(msg_len);
        buf.resize(msg_len, 0);
        tcp.read_exact(&mut buf).await?;

        // Parse response header
        let _ = buf.get_u8();
        let handle = buf.get_i64();

        // Ping response - just return
        if handle == PING_HANDLE {
            return Ok(());
        }

        // Route response to waiting caller
        if let Some((_, req)) = requests.remove(&handle) {
            pending_requests.fetch_sub(1, Ordering::Relaxed);

            // Freeze buffer so it can be safely moved across tasks
            let frozen_buf = buf.freeze();

            // Parse in a separate task to avoid blocking the read loop
            tokio::spawn(async move {
                match Self::parse_response(frozen_buf, handle) {
                    Ok(table) => {
                        let _ = req.channel.send(table).await;
                    }
                    Err(_e) => {
                        async_node_error!(handle = handle, error = %_e, "parse error");
                        // Channel drop will notify the caller
                    }
                }
            });
        } else {
            async_node_warn!(handle = handle, "received response for unknown handle");
        }

        Ok(())
    }

    /// Parse response data (executed in a separate task)
    fn parse_response(buf: bytes::Bytes, handle: i64) -> Result<VoltTable, VoltError> {
        // Convert Bytes to ByteBuffer for parsing
        let mut byte_buf = bytebuffer::ByteBuffer::from_bytes(&buf[..]);
        let info = VoltResponseInfo::new(&mut byte_buf, handle)?;
        let table = new_volt_table(&mut byte_buf, info)?;
        Ok(table)
    }

    /// Cleanup all pending requests on shutdown or error
    async fn cleanup_requests(
        requests: &Arc<DashMap<i64, AsyncNetworkRequest>>,
        pending_requests: &Arc<AtomicUsize>,
        _reason: &str,
    ) {
        let pending_count = requests.len();

        if pending_count > 0 {
            async_node_warn!(
                pending_count = pending_count,
                reason = _reason,
                "cleaning up pending requests"
            );
        }

        // Clear the map (Drop will notify all waiters)
        requests.clear();
        pending_requests.store(0, Ordering::Relaxed);
    }
}

/// Async wait for response result
pub async fn async_block_for_result(
    rx: &mut mpsc::Receiver<VoltTable>,
) -> Result<VoltTable, VoltError> {
    match rx.recv().await {
        Some(mut table) => match table.has_error() {
            None => Ok(table),
            Some(err) => Err(err),
        },
        None => Err(VoltError::ConnectionNotAvailable),
    }
}

/// Async wait for response result with timeout
pub async fn async_block_for_result_with_timeout(
    rx: &mut mpsc::Receiver<VoltTable>,
    timeout_duration: Duration,
) -> Result<VoltTable, VoltError> {
    match timeout(timeout_duration, rx.recv()).await {
        Ok(Some(mut table)) => match table.has_error() {
            None => Ok(table),
            Some(err) => Err(err),
        },
        Ok(None) => Err(VoltError::ConnectionNotAvailable),
        Err(_) => Err(VoltError::Timeout),
    }
}

/// VoltError extension methods for async operations
impl VoltError {
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

// 单元测试
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sequence_generation() {
        let node = AsyncNode {
            write_tx: mpsc::channel(1).0,
            info: ConnInfo::default(),
            requests: Arc::new(DashMap::new()),
            stop: Arc::new(watch::channel(false).0),
            counter: Arc::new(AtomicI64::new(1)),
            pending_requests: Arc::new(AtomicUsize::new(0)),
        };

        let seq1 = node.get_sequence();
        let seq2 = node.get_sequence();
        assert_eq!(seq2, seq1 + 1);
    }

    #[tokio::test]
    async fn test_pending_count() {
        let node = AsyncNode {
            write_tx: mpsc::channel(1).0,
            info: ConnInfo::default(),
            requests: Arc::new(DashMap::new()),
            stop: Arc::new(watch::channel(false).0),
            counter: Arc::new(AtomicI64::new(1)),
            pending_requests: Arc::new(AtomicUsize::new(5)),
        };
        assert_eq!(node.pending_count(), 5);
    }
}
