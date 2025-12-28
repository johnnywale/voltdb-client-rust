use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use crate::async_node::{AsyncNode, async_block_for_result};
use crate::node::{NodeOpt, Opts};
use crate::{Value, VoltError, VoltTable};

struct AsyncInnerPool {
    opts: Opts,
    pool: Vec<Arc<AsyncNode>>,
}

impl AsyncInnerPool {
    pub fn node_sizes(&self) -> usize {
        self.opts.0.ip_ports.len()
    }

    fn to_node_opt(&self, i: usize) -> NodeOpt {
        NodeOpt {
            ip_port: self.opts.0.ip_ports.get(i).cloned().unwrap(),
            pass: self.opts.0.pass.clone(),
            user: self.opts.0.user.clone(),
        }
    }

    fn get_node(&self, idx: usize) -> Arc<AsyncNode> {
        Arc::clone(self.pool.get(idx).unwrap())
    }

    async fn new(size: usize, opts: Opts) -> Result<AsyncInnerPool, VoltError> {
        let mut pool = AsyncInnerPool {
            opts,
            pool: Vec::with_capacity(size),
        };
        let total = pool.node_sizes();
        for i in 0..size {
            let z = i % total;
            pool.new_conn(z).await?;
        }
        Ok(pool)
    }

    async fn new_conn(&mut self, idx: usize) -> Result<(), VoltError> {
        let node = AsyncNode::new(self.to_node_opt(idx)).await?;
        self.pool.push(Arc::new(node));
        Ok(())
    }
}

/// Async connection pool for VoltDB
pub struct AsyncPool {
    size: usize,
    total: Arc<AtomicUsize>,
    inner_pool: AsyncInnerPool,
}

impl fmt::Debug for AsyncPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AsyncPool total: {}, size: {}",
            self.total.load(Ordering::Relaxed),
            self.size
        )
    }
}

impl AsyncPool {
    /// Create a new async connection pool with default size (10 connections)
    pub async fn new<T: Into<Opts>>(opts: T) -> Result<AsyncPool, VoltError> {
        AsyncPool::new_manual(10, opts).await
    }

    /// Create a new async connection pool with specified size
    pub async fn new_manual<T: Into<Opts>>(size: usize, opts: T) -> Result<AsyncPool, VoltError> {
        let pool = AsyncInnerPool::new(size, opts.into()).await?;
        Ok(AsyncPool {
            inner_pool: pool,
            size,
            total: Arc::new(AtomicUsize::from(0)),
        })
    }

    /// Get a connection from the pool (round-robin)
    pub fn get_conn(&self) -> AsyncPooledConn {
        let total = self.total.fetch_add(1, Ordering::Relaxed);
        let idx = total % self.size;
        AsyncPooledConn {
            created: SystemTime::now(),
            conn: self.inner_pool.get_node(idx),
        }
    }
}

/// A connection borrowed from the async pool
pub struct AsyncPooledConn {
    #[allow(dead_code)]
    created: SystemTime,
    conn: Arc<AsyncNode>,
}

impl fmt::Debug for AsyncPooledConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AsyncPooledConn created: {:?}", self.created)
    }
}

impl AsyncPooledConn {
    /// Execute an ad-hoc SQL query
    pub async fn query(&self, sql: &str) -> Result<VoltTable, VoltError> {
        let mut rx = self.conn.query(sql).await?;
        async_block_for_result(&mut rx).await
    }

    /// List all stored procedures
    pub async fn list_procedures(&self) -> Result<VoltTable, VoltError> {
        let mut rx = self.conn.list_procedures().await?;
        async_block_for_result(&mut rx).await
    }

    /// Call a stored procedure with parameters
    pub async fn call_sp(
        &self,
        query: &str,
        param: Vec<&dyn Value>,
    ) -> Result<VoltTable, VoltError> {
        let mut rx = self.conn.call_sp(query, param).await?;
        async_block_for_result(&mut rx).await
    }

    /// Upload a JAR file to VoltDB
    pub async fn upload_jar(&self, bs: Vec<u8>) -> Result<VoltTable, VoltError> {
        let mut rx = self.conn.upload_jar(bs).await?;
        async_block_for_result(&mut rx).await
    }
}
