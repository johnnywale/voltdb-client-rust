use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::str::from_utf8;
use std::sync::{Arc, mpsc, Mutex, RwLock};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use bytebuffer::ByteBuffer;
use byteorder::{BigEndian, ReadBytesExt};
use sha2::{Digest, Sha256};

use crate::encode::{Value, VoltError};
use crate::procedure_invocation::new_procedure_invocation;
use crate::response::VoltResponseInfo;
use crate::table::{new_volt_table, VoltTable};
use crate::volt_param;

const PING_HANDLE: i64 = 1 << 63 - 1;


#[derive(Debug)]
pub(crate) struct NetworkRequest {
    handle: i64,
    query: bool,
    sync: bool,
    num_bytes: i32,
    channel: Mutex<Sender<VoltTable>>,
}

pub trait Connection: Sync + Send + 'static {}

#[allow(dead_code)]
pub struct Node {
    tcp_stream: Box<TcpStream>,
    info: ConnInfo,
    requests: Arc<RwLock<HashMap<i64, NetworkRequest>>>,
    counter: Mutex<AtomicI64>,
}

impl Connection for Node {}


impl Node {
    pub fn get_sequence(&self) -> i64 {
        let lock = self.counter.lock();
        let mut seq = lock.unwrap();
        let i = seq.load(Ordering::Relaxed);
        let res = i + 1;
        *seq.get_mut() = res;
        return res;
    }

    pub fn list_procedures(&mut self) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@SystemCatalog", volt_param!("PROCEDURES"))
    }


    pub fn call_sp(&mut self, query: &str, param: Vec<&dyn Value>) -> Result<Receiver<VoltTable>, VoltError> {
        let req = self.get_sequence();
        let mut proc = new_procedure_invocation(
            req,
            false,
            &param,
            query);
        let (tx, rx): (Sender<VoltTable>, Receiver<VoltTable>) = mpsc::channel();
        let shared_sender = Mutex::new(tx);
        let seq = NetworkRequest {
            query: true,
            handle: req,
            num_bytes: proc.slen,
            sync: true,
            channel: shared_sender,
        };
        self.requests.write()?.insert(req, seq);
        let bs = proc.bytes();
        self.tcp_stream.write_all(&*bs)?;
        self.tcp_stream.flush()?;
        return Ok(rx);
    }

    pub fn upload_jar(&mut self, bs: Vec<u8>) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@UpdateClasses", volt_param!(bs,""))
    }
    /// Use `@AdHoc` proc to query .
    pub fn query(&mut self, sql: &str) -> Result<Receiver<VoltTable>, VoltError> {
        let mut zero_vec: Vec<&dyn Value> = Vec::new();
        zero_vec.push(&sql);
        return Ok(self.call_sp("@AdHoc", zero_vec)?);
    }

    pub fn ping(&mut self) -> Result<(), VoltError> {
        let zero_vec: Vec<&dyn Value> = Vec::new();
        let mut proc = new_procedure_invocation(PING_HANDLE, false, &zero_vec, "@Ping");
        let bs = proc.bytes();
        self.tcp_stream.write_all(&*bs)?;
        // TODO add more logic here.
        Ok({})
    }


    fn job(mut tcp: &TcpStream, requests: &Arc<RwLock<HashMap<i64, NetworkRequest>>>) -> Result<(), VoltError> {
        let read_res = tcp.read_u32::<BigEndian>();
        match read_res {
            Ok(read) => {
                if read > 0 {
                    let mut all = vec![0; read as usize];
                    tcp.read_exact(&mut all)?;
                    let mut res = ByteBuffer::from_bytes(&*all);
                    let _ = res.read_u8()?;
                    let handle = res.read_i64()?;
                    if handle == PING_HANDLE {
                        return Ok({});
                    }
                    if let Some(t) = requests.write()?.remove(&handle) {
                        let info = VoltResponseInfo::new(&mut res, handle)?;
                        let table = new_volt_table(&mut res, info)?;
                        let sender = t.channel.lock()?;
                        sender.send(table).unwrap();
                    }
                }
            }
            Err(_) => {}
        }
        Ok({})
    }
    /// Listen on new message come in .
    fn listen(&mut self) -> Result<(), VoltError>
    {
        let requests = Arc::clone(&self.requests);
        let tcp = self.tcp_stream.try_clone()?;
        thread::spawn(move || {
            loop {
                let res = crate::node::Node::job(&tcp, &requests);
                match res {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{} ", err)
                    }
                }
            }
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ConnInfo {
    host_id: i32,
    connection: i64,
    leader_addr: Ipv4Addr,
    build: String,
}

/// Wait for response, convert response error from volt error to `VoltError`.
pub fn block_for_result(res: &Receiver<VoltTable>) -> Result<VoltTable, VoltError> {
    let mut table = res.recv()?;
    let err = table.has_error();
    return match err {
        None => { Ok(table) }
        Some(err) => { Err(err) }
    };
}

/// Create new connection to server .
pub fn get_node(addr: &str) -> Result<Node, VoltError> {
    let mut buffer = ByteBuffer::new();
    let result = [1; 1];
    buffer.write_u32(0);
    buffer.write_bytes(&result);
    buffer.write_bytes(&result);
    buffer.write_string("database");
    buffer.write_string("");
    let password = [];
    let mut hasher: Sha256 = Sha256::new();
    Digest::update(&mut hasher, password);
    buffer.write(&hasher.finalize())?;
    buffer.set_wpos(0);
    buffer.write_u32((buffer.len() - 4) as u32);
    let bs = buffer.to_bytes();
    let mut stream: TcpStream = TcpStream::connect(addr)?;
    stream.write(&bs)?;
    stream.flush()?;
    let read = stream.read_u32::<BigEndian>()?;
    let mut all = vec![0; read as usize];
    stream.read_exact(&mut all)?;
    let mut res = ByteBuffer::from_bytes(&*all);
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
    // TODO check IP
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
    let data = Arc::new(RwLock::new(HashMap::new()));
    let mut res = Node {
        tcp_stream: Box::new(stream),
        info,
        requests: data,
        counter: Mutex::new(AtomicI64::new(1)),
    };
    res.listen()?;
    return Ok(res);
}
