use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use std::sync::mpsc::Receiver;
use std::time::SystemTime;

use crate::{Node, node, NodeOpt, Opts, Value, VoltError, VoltTable};

#[derive(Debug)]
struct InnerPool {
    opts: Opts,
    pool: Vec<Node>,
}


impl InnerPool {
    pub fn node_sizes(&self) -> usize {
        return self.opts.0.ip_ports.len();
    }

    fn to_node_opt(&self, i: usize) -> NodeOpt {
        return NodeOpt {
            ip_port: self.opts.0.ip_ports.get(i).cloned().unwrap(),
            pass: self.opts.0.pass.clone(),
            user: self.opts.0.user.clone(),
        };
    }
    fn get_node(&mut self, idx: usize) -> &mut Node {
        return self.pool.get_mut(idx).unwrap();
    }
    fn new(size: usize, opts: Opts) -> Result<InnerPool, VoltError> {
        let mut pool = InnerPool {
            opts,
            pool: Vec::with_capacity(size),
        };
        let total = pool.node_sizes();
        for i in 0..size {
            let z = i % total;
            pool.new_conn(z)?;
        }
        Ok(pool)
    }
    fn new_conn(&mut self, idx: usize) -> Result<(), VoltError> {
        match node::Node::new(self.to_node_opt(idx)) {
            Ok(conn) => {
                self.pool.push(conn);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}


pub struct Pool {
    size: usize,
    total: Arc<AtomicUsize>,
    inner_pool: InnerPool,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pool total: {}, size: {}",
            self.total.load(Ordering::Relaxed),
            self.size
        )
    }
}

impl Pool {
    fn _get_conn(&mut self) -> Result<PooledConn, VoltError> {
        let total = self.total.fetch_add(1, Ordering::Relaxed);
        let idx = total % self.size;
        Ok(PooledConn {
            created: SystemTime::now(),
            conn: self.inner_pool.get_node(idx),
        })
    }

    pub fn new<T: Into<Opts>>(opts: T) -> Result<Pool, VoltError> {
        Pool::new_manual(10, opts)
    }

    pub fn new_manual<T: Into<Opts>>(size: usize, opts: T) -> Result<Pool, VoltError> {
        let pool = InnerPool::new(size, opts.into())?;
        Ok(Pool {
            inner_pool: pool,
            size,
            total: Arc::new(AtomicUsize::from(0 as usize)),

        })
    }

    pub fn get_conn(&mut self) -> Result<PooledConn, VoltError> {
        self._get_conn()
    }
}

#[derive(Debug)]
pub struct PooledConn<'a> {
    created: SystemTime,
    conn: &'a mut Node,
}

impl<'a> Drop for PooledConn<'a> {
    fn drop(&mut self) {
        let since = SystemTime::now().duration_since(self.created);
        // TODO record error ,
        //   println!("used {:?} ", since)
    }
}

impl<'a> PooledConn<'a> {
    pub fn query(&mut self, sql: &str) -> Result<Receiver<VoltTable>, VoltError> {
        return self.conn.query(sql);
    }
    pub fn list_procedures(&mut self) -> Result<Receiver<VoltTable>, VoltError> {
        return self.conn.list_procedures();
    }
    pub fn call_sp(&mut self, query: &str, param: Vec<&dyn Value>) -> Result<Receiver<VoltTable>, VoltError> {
        return self.conn.call_sp(query, param);
    }
    pub fn upload_jar(&mut self, bs: Vec<u8>) -> Result<Receiver<VoltTable>, VoltError> {
        return self.conn.upload_jar(bs);
    }
}
