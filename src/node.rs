//! # VoltDB Node Connection
//!
//! This module provides the core TCP connection handling for communicating with VoltDB servers.
//!
//! ## Architecture
//!
//! The connection uses a single-threaded TCP listener design:
//!
//! - We spawn one dedicated thread to synchronously read from the TcpStream.
//! - Each incoming message is dispatched via channels to the rest of the application,
//!   allowing users to perform asynchronous operations on the received data.
//! - Using `Mutex<TcpStream>` for single Stream would introduce blocking and contention,
//!   because locking for each read/write would stall other operations. This design
//!   avoids that while keeping the network I/O simple and efficient.
//!
//! ## Timeouts
//!
//! The module supports two types of timeouts:
//!
//! - **Connection timeout**: Limits how long the initial TCP connection attempt will wait.
//!   Use [`OptsBuilder::connect_timeout`] or set [`NodeOpt::connect_timeout`].
//! - **Read timeout**: Limits how long socket read operations will wait for data.
//!   This affects both the authentication handshake and the background listener thread.
//!   Use [`OptsBuilder::read_timeout`] or set [`NodeOpt::read_timeout`].
//!
//! ## Example
//!
//! ```no_run
//! use voltdb_client_rust::{Opts, Pool};
//! use std::time::Duration;
//!
//! // Create connection options with timeouts
//! let opts = Opts::builder()
//!     .host("localhost", 21212)
//!     .connect_timeout(Duration::from_secs(5))
//!     .read_timeout(Duration::from_secs(30))
//!     .build()
//!     .unwrap();
//!
//! // Create a connection pool
//! let mut pool = Pool::new(opts)?;
//! let mut conn = pool.get_conn()?;
//!
//! // Execute a query
//! let result = conn.query("SELECT * FROM my_table")?;
//! # Ok::<(), voltdb_client_rust::VoltError>(())
//! ```

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use bytebuffer::ByteBuffer;
use byteorder::{BigEndian, ReadBytesExt};

use crate::encode::{Value, VoltError};
use crate::procedure_invocation::new_procedure_invocation;
use crate::protocol::{PING_HANDLE, build_auth_message, parse_auth_response};
use crate::response::VoltResponseInfo;
use crate::table::{VoltTable, new_volt_table};
use crate::volt_param;

// ============================================================================
// Logging macros - use tracing if available, otherwise no-op
// ============================================================================

#[cfg(feature = "tracing")]
macro_rules! node_error {
    ($($arg:tt)*) => { tracing::error!($($arg)*) };
}
#[cfg(not(feature = "tracing"))]
macro_rules! node_error {
    ($($arg:tt)*) => {};
}

/// Connection options for VoltDB client.
///
/// This struct encapsulates all configuration options needed to establish
/// connections to a VoltDB cluster. Use [`Opts::builder()`] for a fluent
/// configuration API or [`Opts::new()`] for simple configurations.
///
/// # Example
/// ```no_run
/// use voltdb_client_rust::{Opts, IpPort};
///
/// // Simple configuration
/// let hosts = vec![IpPort::new("localhost".to_string(), 21212)];
/// let opts = Opts::new(hosts);
///
/// // Or use the builder for more options
/// let opts = Opts::builder()
///     .host("localhost", 21212)
///     .user("admin")
///     .password("secret")
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts(pub(crate) Box<InnerOpts>);

/// Host and port pair for VoltDB server connections.
///
/// Represents a single VoltDB server endpoint. Multiple `IpPort` instances
/// can be used with connection pools for cluster connectivity.
///
/// # Example
/// ```
/// use voltdb_client_rust::IpPort;
///
/// let endpoint = IpPort::new("192.168.1.100".to_string(), 21212);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IpPort {
    pub(crate) ip_host: String,
    pub(crate) port: u16,
}

impl IpPort {
    /// Creates a new `IpPort` with the given hostname/IP and port.
    ///
    /// # Arguments
    /// * `ip_host` - Hostname or IP address of the VoltDB server
    /// * `port` - Port number (typically 21212 for client connections)
    pub fn new(ip_host: String, port: u16) -> Self {
        IpPort { ip_host, port }
    }
}

impl Opts {
    /// Creates connection options with the given hosts and default settings.
    ///
    /// This is a convenience constructor for simple configurations without
    /// authentication or timeouts. For more control, use [`Opts::builder()`].
    ///
    /// # Arguments
    /// * `hosts` - List of VoltDB server endpoints to connect to
    pub fn new(hosts: Vec<IpPort>) -> Opts {
        Opts(Box::new(InnerOpts {
            ip_ports: hosts,
            user: None,
            pass: None,
            connect_timeout: None,
            read_timeout: None,
        }))
    }

    /// Creates a new [`OptsBuilder`] for fluent configuration.
    ///
    /// # Example
    /// ```no_run
    /// use voltdb_client_rust::Opts;
    /// use std::time::Duration;
    ///
    /// let opts = Opts::builder()
    ///     .host("localhost", 21212)
    ///     .connect_timeout(Duration::from_secs(5))
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> OptsBuilder {
        OptsBuilder::default()
    }
}

/// Builder for connection options.
///
/// # Example
/// ```no_run
/// use voltdb_client_rust::{Opts, IpPort};
/// use std::time::Duration;
///
/// let opts = Opts::builder()
///     .host("localhost", 21212)
///     .user("admin")
///     .password("password")
///     .connect_timeout(Duration::from_secs(10))
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct OptsBuilder {
    hosts: Vec<IpPort>,
    user: Option<String>,
    pass: Option<String>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
}

impl OptsBuilder {
    /// Add a host to connect to.
    pub fn host(mut self, ip: &str, port: u16) -> Self {
        self.hosts.push(IpPort::new(ip.to_string(), port));
        self
    }

    /// Add multiple hosts to connect to.
    pub fn hosts(mut self, hosts: Vec<IpPort>) -> Self {
        self.hosts.extend(hosts);
        self
    }

    /// Set the username for authentication.
    pub fn user(mut self, user: &str) -> Self {
        self.user = Some(user.to_string());
        self
    }

    /// Set the password for authentication.
    pub fn password(mut self, pass: &str) -> Self {
        self.pass = Some(pass.to_string());
        self
    }

    /// Set connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set read timeout for socket operations.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Build the Opts.
    ///
    /// Returns an error if no hosts are configured.
    pub fn build(self) -> Result<Opts, VoltError> {
        if self.hosts.is_empty() {
            return Err(VoltError::InvalidConfig);
        }
        Ok(Opts(Box::new(InnerOpts {
            ip_ports: self.hosts,
            user: self.user,
            pass: self.pass,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
        })))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct InnerOpts {
    pub(crate) ip_ports: Vec<IpPort>,
    pub(crate) user: Option<String>,
    pub(crate) pass: Option<String>,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) read_timeout: Option<Duration>,
}

/// Options for creating a single [`Node`] connection.
///
/// This struct holds the connection parameters for establishing a TCP connection
/// to a VoltDB server node.
///
/// # Example
/// ```no_run
/// use voltdb_client_rust::{NodeOpt, IpPort};
/// use std::time::Duration;
///
/// let opt = NodeOpt {
///     ip_port: IpPort::new("localhost".to_string(), 21212),
///     user: Some("admin".to_string()),
///     pass: Some("password".to_string()),
///     connect_timeout: Some(Duration::from_secs(10)),
///     read_timeout: Some(Duration::from_secs(30)),
/// };
/// ```
pub struct NodeOpt {
    /// The host and port to connect to.
    pub ip_port: IpPort,
    /// Optional username for authentication.
    pub user: Option<String>,
    /// Optional password for authentication.
    pub pass: Option<String>,
    /// Connection timeout. If `None`, the connection attempt will block indefinitely.
    pub connect_timeout: Option<Duration>,
    /// Read timeout for socket operations. If `None`, reads will block indefinitely.
    /// This affects the background listener thread that receives responses from the server.
    pub read_timeout: Option<Duration>,
}

/// Type alias for the pending requests map.
/// Maps request handles to their response channels.
type PendingRequests = HashMap<i64, Sender<VoltTable>>;

/// Marker trait for VoltDB connections.
///
/// Implemented by both synchronous [`Node`] and async `AsyncNode` connections.
pub trait Connection: Sync + Send + 'static {}

/// A single TCP connection to a VoltDB server node.
///
/// `Node` represents a persistent, stateful TCP connection used to execute
/// stored procedures and queries against a VoltDB cluster. Each `Node`
/// maintains its own socket and spawns a dedicated background thread to
/// asynchronously receive and dispatch responses from the server.
///
/// # Concurrency
///
/// `Node` is safe to use concurrently and supports multiple in-flight requests
/// over the same connection. For automatic reconnection, load balancing, or
/// managing multiple connections, use [`crate::Pool`].
///
/// # Example
///
/// ```no_run
/// use voltdb_client_rust::{Node, NodeOpt, IpPort, block_for_result};
///
/// let opt = NodeOpt {
///     ip_port: IpPort::new("localhost".to_string(), 21212),
///     user: None,
///     pass: None,
///     connect_timeout: None,
///     read_timeout: None,
/// };
///
/// let node = Node::new(opt)?;
/// let rx = node.query("SELECT * FROM my_table")?;
/// let table = block_for_result(&rx)?;
/// # Ok::<(), voltdb_client_rust::VoltError>(())
/// ```
#[allow(dead_code)]
pub struct Node {
    /// Write-side of the TCP stream, protected by a mutex for thread-safe writes.
    /// Multiple threads can call query/call_sp concurrently; writes are serialized.
    write_stream: Mutex<Option<TcpStream>>,
    info: ConnInfo,
    /// Pending requests awaiting responses. Uses Mutex instead of RwLock since
    /// both insert (main thread) and remove (listener thread) require exclusive access.
    requests: Arc<Mutex<PendingRequests>>,
    stop: Arc<Mutex<bool>>,
    counter: AtomicI64,
    /// Simple atomic lock for write operations. True = locked, False = unlocked.
    write_lock: AtomicBool,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pending request: {}", 1)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let res = self.shutdown();
        match res {
            Ok(_) => {}
            Err(_e) => {
                node_error!(error = ?_e, "error during node shutdown");
            }
        }
    }
}

impl Connection for Node {}

impl Node {
    /// Creates a new connection to a VoltDB server node.
    ///
    /// This method establishes a TCP connection to the specified host/port,
    /// performs authentication, and spawns a background listener thread for
    /// receiving asynchronous responses.
    ///
    /// # Arguments
    /// * `opt` - Connection options including host, port, credentials, and timeouts.
    ///
    /// # Timeouts
    /// * `connect_timeout` - If set, limits how long the connection attempt will wait.
    ///   If not set, the connection attempt blocks indefinitely.
    /// * `read_timeout` - If set, socket read operations will timeout after this duration.
    ///   This affects both the authentication phase and the background listener thread.
    ///
    /// # Errors
    /// Returns `VoltError` if:
    /// * The connection cannot be established (network error or timeout)
    /// * DNS resolution fails
    /// * Authentication fails
    /// * The server rejects the connection
    ///
    /// # Example
    /// ```no_run
    /// use voltdb_client_rust::{Node, NodeOpt, IpPort};
    /// use std::time::Duration;
    ///
    /// let opt = NodeOpt {
    ///     ip_port: IpPort::new("localhost".to_string(), 21212),
    ///     user: None,
    ///     pass: None,
    ///     connect_timeout: Some(Duration::from_secs(5)),
    ///     read_timeout: Some(Duration::from_secs(30)),
    /// };
    /// let node = Node::new(opt)?;
    /// # Ok::<(), voltdb_client_rust::VoltError>(())
    /// ```
    pub fn new(opt: NodeOpt) -> Result<Node, VoltError> {
        let ip_host = opt.ip_port;
        let addr_str = format!("{}:{}", ip_host.ip_host, ip_host.port);

        // Build authentication message using shared protocol code
        let auth_msg = build_auth_message(opt.user.as_deref(), opt.pass.as_deref())?;

        // Connect to server with optional timeout
        let mut stream: TcpStream = match opt.connect_timeout {
            Some(timeout) => {
                // Resolve address for connect_timeout (requires SocketAddr)
                let socket_addr: SocketAddr = addr_str
                    .to_socket_addrs()
                    .map_err(|_| VoltError::InvalidConfig)?
                    .find(|s| s.is_ipv4())
                    .ok_or(VoltError::InvalidConfig)?;
                TcpStream::connect_timeout(&socket_addr, timeout)?
            }
            None => TcpStream::connect(&addr_str)?,
        };

        // Set read timeout if configured
        if let Some(read_timeout) = opt.read_timeout {
            stream.set_read_timeout(Some(read_timeout))?;
        }

        // Send auth request
        stream.write_all(&auth_msg)?;
        stream.flush()?;

        // Read auth response
        let read = stream.read_u32::<BigEndian>()?;
        let mut all = vec![0; read as usize];
        stream.read_exact(&mut all)?;

        // Parse auth response using shared protocol code
        let info = parse_auth_response(&all)?;

        // Clone the stream for the read side (listener thread)
        let read_stream = stream.try_clone()?;

        let requests = Arc::new(Mutex::new(HashMap::new()));
        let stop = Arc::new(Mutex::new(false));

        // Start the listener thread with the read side
        Self::start_listener(read_stream, Arc::clone(&requests), Arc::clone(&stop));

        Ok(Node {
            stop,
            write_stream: Mutex::new(Some(stream)),
            info,
            requests,
            counter: AtomicI64::new(1),
            write_lock: AtomicBool::new(false),
        })
    }
    /// Returns the next unique sequence number for request tracking.
    ///
    /// Each request to VoltDB uses a unique handle (sequence number) for
    /// matching responses to requests.
    pub fn get_sequence(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Lists all stored procedures available in the VoltDB database.
    ///
    /// This calls the `@SystemCatalog` system procedure with "PROCEDURES" argument.
    ///
    /// # Returns
    /// A receiver that will yield a `VoltTable` containing procedure metadata.
    pub fn list_procedures(&self) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@SystemCatalog", volt_param!("PROCEDURES"))
    }

    /// Executes a stored procedure with the given parameters.
    ///
    /// # Arguments
    /// * `query` - The name of the stored procedure (e.g., "@AdHoc", "MyProcedure")
    /// * `param` - Vector of parameter values. Use [`volt_param!`] macro for convenience.
    ///
    /// # Returns
    /// A receiver that will yield the result `VoltTable` when available.
    ///
    /// # Example
    /// ```no_run
    /// use voltdb_client_rust::{Node, NodeOpt, IpPort, Value, block_for_result, volt_param};
    ///
    /// # let opt = NodeOpt {
    /// #     ip_port: IpPort::new("localhost".to_string(), 21212),
    /// #     user: None, pass: None, connect_timeout: None, read_timeout: None,
    /// # };
    /// let node = Node::new(opt)?;
    /// let id = 1i32;
    /// let name = "test".to_string();
    /// let rx = node.call_sp("MyProcedure", volt_param![id, name])?;
    /// let result = block_for_result(&rx)?;
    /// # Ok::<(), voltdb_client_rust::VoltError>(())
    /// ```
    pub fn call_sp(
        &self,
        query: &str,
        param: Vec<&dyn Value>,
    ) -> Result<Receiver<VoltTable>, VoltError> {
        let handle = self.get_sequence();
        let mut proc = new_procedure_invocation(handle, false, &param, query);
        let (tx, rx): (Sender<VoltTable>, Receiver<VoltTable>) = mpsc::channel();

        // Register the response channel before sending the request
        self.requests.lock()?.insert(handle, tx);
        let bs = proc.bytes();
        // Write to stream while holding the lock
        let result = {
            let mut stream_guard = self.write_stream.lock()?;
            match stream_guard.as_mut() {
                None => Err(VoltError::ConnectionNotAvailable),
                Some(stream) => {
                    stream.write_all(&bs)?;
                    Ok(rx)
                }
            }
        };

        // Release write lock
        self.write_lock.store(false, Ordering::Release);

        result
    }

    /// Uploads a JAR file containing stored procedure classes to VoltDB.
    ///
    /// This calls the `@UpdateClasses` system procedure to deploy new classes.
    ///
    /// # Arguments
    /// * `bs` - The JAR file contents as bytes
    pub fn upload_jar(&self, bs: Vec<u8>) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@UpdateClasses", volt_param!(bs, ""))
    }

    /// Executes an ad-hoc SQL query.
    ///
    /// This is a convenience method that calls the `@AdHoc` system procedure.
    ///
    /// # Arguments
    /// * `sql` - The SQL query string to execute
    ///
    /// # Returns
    /// A receiver that will yield the result `VoltTable` when available.
    ///
    /// # Example
    /// ```no_run
    /// use voltdb_client_rust::{Node, NodeOpt, IpPort, block_for_result};
    ///
    /// # let opt = NodeOpt {
    /// #     ip_port: IpPort::new("localhost".to_string(), 21212),
    /// #     user: None, pass: None, connect_timeout: None, read_timeout: None,
    /// # };
    /// let node = Node::new(opt)?;
    /// let rx = node.query("SELECT COUNT(*) FROM users")?;
    /// let result = block_for_result(&rx)?;
    /// # Ok::<(), voltdb_client_rust::VoltError>(())
    /// ```
    pub fn query(&self, sql: &str) -> Result<Receiver<VoltTable>, VoltError> {
        let zero_vec: Vec<&dyn Value> = vec![&sql];
        self.call_sp("@AdHoc", zero_vec)
    }

    /// Sends a ping to the VoltDB server.
    ///
    /// This can be used to keep the connection alive or verify connectivity.
    /// The ping response is handled internally and not returned to the caller.
    pub fn ping(&self) -> Result<(), VoltError> {
        let zero_vec: Vec<&dyn Value> = Vec::new();
        let mut proc = new_procedure_invocation(PING_HANDLE, false, &zero_vec, "@Ping");
        let bs = proc.bytes();

        // Acquire write lock (spin until acquired)
        while self
            .write_lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        let result = {
            let mut stream_guard = self.write_stream.lock()?;
            match stream_guard.as_mut() {
                None => Err(VoltError::ConnectionNotAvailable),
                Some(stream) => {
                    stream.write_all(&bs)?;
                    Ok(())
                }
            }
        };

        // Release write lock
        self.write_lock.store(false, Ordering::Release);

        result
    }

    /// Reads and processes a single message from the TCP stream.
    ///
    /// # Arguments
    /// * `tcp` - The TCP stream to read from
    /// * `requests` - Map of pending requests awaiting responses
    /// * `buffer` - Reusable buffer for reading message data (reduces allocations)
    fn job(
        tcp: &mut impl Read,
        requests: &Arc<Mutex<PendingRequests>>,
        buffer: &mut Vec<u8>,
    ) -> Result<(), VoltError> {
        // Read message length (4 bytes, big-endian)
        let msg_len = tcp.read_u32::<BigEndian>()?;
        if msg_len == 0 {
            return Ok(());
        }

        // Reuse buffer: resize if needed, but capacity is retained
        buffer.resize(msg_len as usize, 0);
        tcp.read_exact(buffer)?;

        let mut res = ByteBuffer::from_bytes(buffer);
        // Skip protocol version byte (always 0 for current protocol)
        let _ = res.read_u8()?;
        let handle = res.read_i64()?;

        if handle == PING_HANDLE {
            return Ok(()); // Ping response, nothing else to do
        }

        if let Some(sender) = requests.lock()?.remove(&handle) {
            let info = VoltResponseInfo::new(&mut res, handle)?;
            let table = new_volt_table(&mut res, info)?;
            // Ignore send error - receiver may have been dropped if caller
            // timed out or cancelled the request
            let _ = sender.send(table);
        }

        Ok(())
    }
    /// Gracefully shuts down the connection.
    ///
    /// This stops the background listener thread and closes the TCP connection.
    /// The `Node` will be unusable after calling this method.
    ///
    /// Note: This is automatically called when the `Node` is dropped.
    pub fn shutdown(&mut self) -> Result<(), VoltError> {
        let mut stop = self.stop.lock()?;
        *stop = true;

        let mut stream_guard = self.write_stream.lock()?;
        if let Some(stream) = stream_guard.take() {
            stream.shutdown(Shutdown::Both)?;
        }
        Ok(())
    }

    /// Starts the background listener thread for receiving responses.
    ///
    /// This is a static method that takes ownership of the read-side stream.
    fn start_listener(
        mut tcp: TcpStream,
        requests: Arc<Mutex<PendingRequests>>,
        stopping: Arc<Mutex<bool>>,
    ) {
        thread::spawn(move || {
            // Reusable buffer to reduce allocation pressure.
            // Starts with 4KB capacity, grows as needed but rarely shrinks.
            let mut buffer = Vec::with_capacity(4096);

            loop {
                // Check stop flag before blocking on read
                let should_stop = stopping
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *should_stop {
                    break;
                }
                drop(should_stop); // Release lock before blocking on I/O

                if let Err(_err) = Node::job(&mut tcp, &requests, &mut buffer) {
                    // Only log error if we're not intentionally stopping
                    let is_stopping = stopping
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if !*is_stopping {
                        node_error!(error = %_err, "VoltDB listener error");
                    }
                }
            }
        });
    }
}

/// Connection metadata returned by the VoltDB server during authentication.
///
/// This struct contains information about the server that the client connected to,
/// including the host ID, connection ID, and cluster leader address.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    /// The ID of the host in the VoltDB cluster.
    pub host_id: i32,
    /// Unique connection identifier assigned by the server.
    pub connection: i64,
    /// IPv4 address of the cluster leader node.
    pub leader_addr: Ipv4Addr,
    /// VoltDB server build/version string.
    pub build: String,
}

impl Default for ConnInfo {
    fn default() -> Self {
        Self {
            host_id: 0,
            connection: 0,
            leader_addr: Ipv4Addr::new(127, 0, 0, 1),
            build: String::new(),
        }
    }
}

/// Blocks until a response is received and converts any VoltDB errors.
///
/// This is a convenience function for synchronous usage. It waits for the
/// response on the channel and converts VoltDB-level errors (from the response)
/// into `VoltError`.
///
/// # Arguments
/// * `res` - The receiver from a query or stored procedure call
///
/// # Returns
/// The result `VoltTable` on success, or `VoltError` if the operation failed.
///
/// # Example
/// ```no_run
/// use voltdb_client_rust::{Node, NodeOpt, IpPort, block_for_result};
///
/// # let opt = NodeOpt {
/// #     ip_port: IpPort::new("localhost".to_string(), 21212),
/// #     user: None, pass: None, connect_timeout: None, read_timeout: None,
/// # };
/// let node = Node::new(opt)?;
/// let rx = node.query("SELECT * FROM users")?;
/// let mut table = block_for_result(&rx)?;
///
/// while table.advance_row() {
///     // Process rows...
/// }
/// # Ok::<(), voltdb_client_rust::VoltError>(())
/// ```
pub fn block_for_result(res: &Receiver<VoltTable>) -> Result<VoltTable, VoltError> {
    let mut table = res.recv()?;
    let err = table.has_error();
    match err {
        None => Ok(table),
        Some(err) => Err(err),
    }
}

pub fn reset() {}

/// Creates a new connection to a VoltDB server using an address string.
///
/// This is a convenience function that parses the address string and creates
/// a connection with default settings (no authentication, no timeouts).
///
/// # Arguments
/// * `addr` - The server address in "host:port" format (e.g., "localhost:21212")
///
/// # Errors
/// Returns `VoltError::InvalidConfig` if the address cannot be parsed or resolved.
///
/// # Example
/// ```no_run
/// use voltdb_client_rust::get_node;
///
/// let node = get_node("localhost:21212")?;
/// # Ok::<(), voltdb_client_rust::VoltError>(())
/// ```
pub fn get_node(addr: &str) -> Result<Node, VoltError> {
    let addrs = addr
        .to_socket_addrs()
        .map_err(|_| VoltError::InvalidConfig)?;

    let socket_addr = addrs
        .into_iter()
        .find(|s| s.is_ipv4())
        .ok_or(VoltError::InvalidConfig)?;

    let ip_port = IpPort::new(socket_addr.ip().to_string(), socket_addr.port());

    let opt = NodeOpt {
        ip_port,
        user: None,
        pass: None,
        connect_timeout: None,
        read_timeout: None,
    };
    Node::new(opt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opts_builder_basic() {
        let opts = Opts::builder().host("localhost", 21212).build().unwrap();

        assert_eq!(opts.0.ip_ports.len(), 1);
        assert_eq!(opts.0.ip_ports[0].ip_host, "localhost");
        assert_eq!(opts.0.ip_ports[0].port, 21212);
        assert!(opts.0.user.is_none());
        assert!(opts.0.pass.is_none());
    }

    #[test]
    fn test_opts_builder_with_auth() {
        let opts = Opts::builder()
            .host("127.0.0.1", 21211)
            .user("admin")
            .password("secret")
            .build()
            .unwrap();

        assert_eq!(opts.0.user, Some("admin".to_string()));
        assert_eq!(opts.0.pass, Some("secret".to_string()));
    }

    #[test]
    fn test_opts_builder_multiple_hosts() {
        let opts = Opts::builder()
            .host("host1", 21212)
            .host("host2", 21212)
            .host("host3", 21212)
            .build()
            .unwrap();

        assert_eq!(opts.0.ip_ports.len(), 3);
        assert_eq!(opts.0.ip_ports[0].ip_host, "host1");
        assert_eq!(opts.0.ip_ports[1].ip_host, "host2");
        assert_eq!(opts.0.ip_ports[2].ip_host, "host3");
    }

    #[test]
    fn test_opts_builder_with_hosts_vec() {
        let hosts = vec![
            IpPort::new("node1".to_string(), 21212),
            IpPort::new("node2".to_string(), 21213),
        ];
        let opts = Opts::builder().hosts(hosts).build().unwrap();

        assert_eq!(opts.0.ip_ports.len(), 2);
    }

    #[test]
    fn test_opts_builder_with_timeouts() {
        let opts = Opts::builder()
            .host("localhost", 21212)
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        assert_eq!(opts.0.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(opts.0.read_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_opts_builder_no_hosts_fails() {
        let result = Opts::builder().build();
        assert!(result.is_err());
        match result {
            Err(VoltError::InvalidConfig) => {}
            _ => panic!("Expected InvalidConfig error"),
        }
    }

    #[test]
    fn test_opts_new_compatibility() {
        let hosts = vec![IpPort::new("localhost".to_string(), 21212)];
        let opts = Opts::new(hosts);

        assert_eq!(opts.0.ip_ports.len(), 1);
        assert!(opts.0.user.is_none());
        assert!(opts.0.connect_timeout.is_none());
    }

    #[test]
    fn test_ip_port_new() {
        let ip_port = IpPort::new("192.168.1.1".to_string(), 8080);
        assert_eq!(ip_port.ip_host, "192.168.1.1");
        assert_eq!(ip_port.port, 8080);
    }
}
