use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::str::from_utf8;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use bytebuffer::ByteBuffer;
use byteorder::{BigEndian, ByteOrder};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex, RwLock};

use crate::encode::{Value, VoltError};
use crate::node::{ConnInfo, NodeOpt};
use crate::procedure_invocation::new_procedure_invocation;
use crate::response::VoltResponseInfo;
use crate::table::{new_volt_table, VoltTable};
use crate::volt_param;

const PING_HANDLE: i64 = 1 << 63 - 1;

/// Async network request tracking
#[derive(Debug)]
pub(crate) struct AsyncNetworkRequest {
    #[allow(dead_code)]
    handle: i64,
    #[allow(dead_code)]
    query: bool,
    #[allow(dead_code)]
    sync: bool,
    #[allow(dead_code)]
    num_bytes: i32,
    channel: mpsc::Sender<VoltTable>,
}

/// Async VoltDB connection
pub struct AsyncNode {
    tcp_write: Arc<Mutex<WriteHalf<TcpStream>>>,
    #[allow(dead_code)]
    info: ConnInfo,
    requests: Arc<RwLock<HashMap<i64, AsyncNetworkRequest>>>,
    stop: Arc<watch::Sender<bool>>,
    counter: Mutex<AtomicI64>,
}

impl Debug for AsyncNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncNode")
    }
}

impl AsyncNode {
    /// Create a new async connection to VoltDB server
    pub async fn new(opt: NodeOpt) -> Result<AsyncNode, VoltError> {
        let ip_host = &opt.ip_port;
        let addr = format!("{}:{}", ip_host.ip_host, ip_host.port);

        // Build auth message
        let mut buffer = ByteBuffer::new();
        let result = [1; 1];
        buffer.write_u32(0);
        buffer.write_bytes(&result);
        buffer.write_bytes(&result);
        buffer.write_string("database");

        match &opt.user {
            None => {
                buffer.write_string("");
            }
            Some(user) => {
                buffer.write_string(user.as_str());
            }
        }

        match &opt.pass {
            None => {
                let password = [];
                let mut hasher: Sha256 = Sha256::new();
                Digest::update(&mut hasher, password);
                buffer.write(&hasher.finalize())?;
            }
            Some(password) => {
                let password = password.as_bytes();
                let mut hasher: Sha256 = Sha256::new();
                Digest::update(&mut hasher, password);
                buffer.write(&hasher.finalize())?;
            }
        }

        buffer.set_wpos(0);
        buffer.write_u32((buffer.len() - 4) as u32);
        let bs = buffer.into_vec();

        // Async connect
        let mut stream = TcpStream::connect(&addr).await?;

        // Async write auth request
        stream.write_all(&bs).await?;
        stream.flush().await?;

        // Async read auth response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let read = BigEndian::read_u32(&len_buf) as usize;

        let mut all = vec![0; read];
        stream.read_exact(&mut all).await?;

        // Parse auth response
        let mut res = ByteBuffer::from_bytes(&all);
        let _version = res.read_u8()?;
        let auth = res.read_u8()?;
        if auth != 0 {
            return Err(VoltError::AuthFailed);
        }

        let host_id = res.read_i32()?;
        let connection = res.read_i64()?;
        let _ = res.read_i64()?;
        let leader = res.read_i32()?;
        let bs = (leader as u32).to_be_bytes();
        let leader_addr = Ipv4Addr::from(bs);

        let length = res.read_i32()?;
        let mut build = vec![0; length as usize];
        res.read_exact(&mut build)?;
        let b = from_utf8(&build)?;

        let info = ConnInfo {
            host_id,
            connection,
            leader_addr,
            build: String::from(b),
        };

        // Split stream for concurrent read/write
        let (read_half, write_half) = tokio::io::split(stream);

        let data = Arc::new(RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);

        let node = AsyncNode {
            stop: Arc::new(stop_tx),
            tcp_write: Arc::new(Mutex::new(write_half)),
            info,
            requests: data.clone(),
            counter: Mutex::new(AtomicI64::new(1)),
        };

        // Spawn background reader task
        node.listen(read_half, stop_rx);

        Ok(node)
    }

    /// Get next sequence number for request tracking
    pub async fn get_sequence(&self) -> i64 {
        let lock = self.counter.lock().await;
        lock.fetch_add(1, Ordering::Relaxed)
    }

    /// List all stored procedures
    pub async fn list_procedures(&self) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        self.call_sp("@SystemCatalog", volt_param!("PROCEDURES")).await
    }

    /// Call a stored procedure with parameters
    pub async fn call_sp(
        &self,
        query: &str,
        param: Vec<&dyn Value>,
    ) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        let req = self.get_sequence().await;
        let mut proc = new_procedure_invocation(req, false, &param, query);

        // Create async channel (bounded to 1 for single response)
        let (tx, rx) = mpsc::channel(1);

        let seq = AsyncNetworkRequest {
            query: true,
            handle: req,
            num_bytes: proc.slen,
            sync: true,
            channel: tx,
        };

        self.requests.write().await.insert(req, seq);

        let bs = proc.bytes();
        let mut stream = self.tcp_write.lock().await;
        stream.write_all(&bs).await?;

        Ok(rx)
    }

    /// Upload a JAR file to VoltDB
    pub async fn upload_jar(&self, bs: Vec<u8>) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        self.call_sp("@UpdateClasses", volt_param!(bs, "")).await
    }

    /// Execute an ad-hoc SQL query
    pub async fn query(&self, sql: &str) -> Result<mpsc::Receiver<VoltTable>, VoltError> {
        let mut zero_vec: Vec<&dyn Value> = Vec::new();
        zero_vec.push(&sql);
        self.call_sp("@AdHoc", zero_vec).await
    }

    /// Send a ping to keep connection alive
    pub async fn ping(&self) -> Result<(), VoltError> {
        let zero_vec: Vec<&dyn Value> = Vec::new();
        let mut proc = new_procedure_invocation(PING_HANDLE, false, &zero_vec, "@Ping");
        let bs = proc.bytes();

        let mut stream = self.tcp_write.lock().await;
        stream.write_all(&bs).await?;

        Ok(())
    }

    /// Shutdown the connection
    pub async fn shutdown(&self) -> Result<(), VoltError> {
        let _ = self.stop.send(true);
        Ok(())
    }

    /// Spawn background task to read responses
    fn listen(&self, mut read_half: ReadHalf<TcpStream>, mut stop_rx: watch::Receiver<bool>) {
        let requests = Arc::clone(&self.requests);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    result = Self::async_job(&mut read_half, &requests) => {
                        if let Err(e) = result {
                            if !*stop_rx.borrow() {
                                eprintln!("Async job error: {}", e);
                            }
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Read and process a single response from the server
    async fn async_job(
        tcp: &mut ReadHalf<TcpStream>,
        requests: &Arc<RwLock<HashMap<i64, AsyncNetworkRequest>>>,
    ) -> Result<(), VoltError> {
        // Read message length
        let mut len_buf = [0u8; 4];
        tcp.read_exact(&mut len_buf).await?;
        let read = BigEndian::read_u32(&len_buf) as usize;

        if read > 0 {
            let mut all = vec![0; read];
            tcp.read_exact(&mut all).await?;

            // Parse response
            let mut res = ByteBuffer::from_bytes(&all);
            let _ = res.read_u8()?;
            let handle = res.read_i64()?;

            if handle == PING_HANDLE {
                return Ok(());
            }

            // Route response to waiting caller
            if let Some(req) = requests.write().await.remove(&handle) {
                let info = VoltResponseInfo::new(&mut res, handle)?;
                let table = new_volt_table(&mut res, info)?;
                let _ = req.channel.send(table).await;
            }
        }

        Ok(())
    }
}

/// Async wait for response, convert response error to VoltError
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
