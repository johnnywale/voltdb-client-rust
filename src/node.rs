use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpStream};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use bytebuffer::ByteBuffer;
use byteorder::{BigEndian, ReadBytesExt};

use crate::encode::{Value, VoltError};
use crate::procedure_invocation::new_procedure_invocation;
use crate::protocol::{build_auth_message, parse_auth_response, PING_HANDLE};
use crate::response::VoltResponseInfo;
use crate::table::{new_volt_table, VoltTable};
use crate::volt_param;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts(pub(crate) Box<InnerOpts>);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IpPort {
    pub(crate) ip_host: String,
    pub(crate) port: u16,
}

impl IpPort {
    pub fn new(ip_host: String, port: u16) -> Self {
        return IpPort { ip_host, port };
    }
}

impl Opts {
    pub fn new(hosts: Vec<IpPort>) -> Opts {
        Opts(Box::new(InnerOpts {
            ip_ports: hosts,
            user: None,
            pass: None,
            connect_timeout: None,
            read_timeout: None,
        }))
    }

    /// Create a new OptsBuilder for fluent configuration.
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

pub struct NodeOpt {
    pub ip_port: IpPort,
    pub user: Option<String>,
    pub pass: Option<String>,
}

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
    tcp_stream: Box<Option<TcpStream>>,
    info: ConnInfo,
    requests: Arc<RwLock<HashMap<i64, NetworkRequest>>>,
    stop: Arc<Mutex<bool>>,
    counter: Mutex<AtomicI64>,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return write!(f, "Pending request: {}", 1);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let res = self.shutdown();
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}

impl Connection for Node {}

impl Node {
    pub fn new(opt: NodeOpt) -> Result<Node, VoltError> {
        let ip_host = opt.ip_port;
        let addr = format!("{}:{}", ip_host.ip_host, ip_host.port);

        // Build authentication message using shared protocol code
        let auth_msg = build_auth_message(opt.user.as_deref(), opt.pass.as_deref())?;

        // Connect to server
        let mut stream: TcpStream = TcpStream::connect(addr)?;

        // Send auth request
        stream.write_all(&auth_msg)?;
        stream.flush()?;

        // Read auth response
        let read = stream.read_u32::<BigEndian>()?;
        let mut all = vec![0; read as usize];
        stream.read_exact(&mut all)?;

        // Parse auth response using shared protocol code
        let info = parse_auth_response(&all)?;

        let data = Arc::new(RwLock::new(HashMap::new()));
        let mut res = Node {
            stop: Arc::new(Mutex::new(false)),
            tcp_stream: Box::new(Option::Some(stream)),
            info,
            requests: data,
            counter: Mutex::new(AtomicI64::new(1)),
        };
        res.listen()?;
        Ok(res)
    }
    pub fn get_sequence(&self) -> i64 {
        let lock = self.counter.lock();
        let seq = lock.unwrap();
        let i = seq.fetch_add(1, Ordering::Relaxed);
        return i;
    }

    pub fn list_procedures(&mut self) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@SystemCatalog", volt_param!("PROCEDURES"))
    }

    pub fn call_sp(
        &mut self,
        query: &str,
        param: Vec<&dyn Value>,
    ) -> Result<Receiver<VoltTable>, VoltError> {
        let req = self.get_sequence();
        let mut proc = new_procedure_invocation(req, false, &param, query);
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
        let tcp_stream = self.tcp_stream.as_mut();
        match tcp_stream {
            None => {
                return Err(VoltError::ConnectionNotAvailable);
            }
            Some(stream) => {
                stream.write_all(&*bs)?;
            }
        }
        return Ok(rx);
    }

    pub fn upload_jar(&mut self, bs: Vec<u8>) -> Result<Receiver<VoltTable>, VoltError> {
        self.call_sp("@UpdateClasses", volt_param!(bs, ""))
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
        let res = self.tcp_stream.as_mut();
        match res {
            None => {
                return Err(VoltError::ConnectionNotAvailable);
            }
            Some(stream) => {
                stream.write_all(&*bs)?;
            }
        }
        Ok({})
    }

    fn job(
        mut tcp: &TcpStream,
        requests: &Arc<RwLock<HashMap<i64, NetworkRequest>>>,
    ) -> Result<(), VoltError> {
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
            Err(e) => {
                return Err(VoltError::Io(e));
            }
        }
        Ok({})
    }
    pub fn shutdown(&mut self) -> Result<(), VoltError> {
        let mut stop = self.stop.lock().unwrap();
        *stop = true;
        let res = self.tcp_stream.as_mut();
        match res {
            None => {}
            Some(stream) => {
                stream.shutdown(Shutdown::Both)?;
            }
        }
        self.tcp_stream = Box::new(Option::None);
        return Ok({});
    }
    /// Listen on new message come in .
    fn listen(&mut self) -> Result<(), VoltError> {
        let requests = Arc::clone(&self.requests);

        let res = self.tcp_stream.as_mut();
        return match res {
            None => Ok(()),
            Some(res) => {
                let tcp = res.try_clone()?;
                let stopping = Arc::clone(&self.stop);
                thread::spawn(move || loop {
                    if *stopping.lock().unwrap() {
                        break;
                    } else {
                        let res = crate::node::Node::job(&tcp, &requests);
                        match res {
                            Ok(_) => {}
                            Err(err) => {
                                if !*stopping.lock().unwrap() {
                                    eprintln!("{} ", err)
                                }
                            }
                        }
                    }
                });
                Ok(())
            }
        };
    }
}

#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub host_id: i32,
    pub connection: i64,
    pub leader_addr: Ipv4Addr,
    pub build: String,
}

/// Wait for response, convert response error from volt error to `VoltError`.
pub fn block_for_result(res: &Receiver<VoltTable>) -> Result<VoltTable, VoltError> {
    let mut table = res.recv()?;
    let err = table.has_error();
    return match err {
        None => Ok(table),
        Some(err) => Err(err),
    };
}

pub fn reset() {}

/// Create new connection to server .
pub fn get_node(addr: &str) -> Result<Node, VoltError> {
    let url = addr.split(":").collect::<Vec<&str>>();
    let host = url.get(0).unwrap().to_string();
    let port = u16::from_str(url.get(1).unwrap()).unwrap();
    let ip_port = IpPort::new(host, port);
    let opt = NodeOpt {
        ip_port,
        user: None,
        pass: None,
    };
    return Node::new(opt);
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
