use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Acquire;
use std::thread;
use std::thread::JoinHandle;
use std::time::SystemTime;

use testcontainers::core::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

use voltdb_client_rust::*;

#[test]
fn test_pool() -> Result<(), VoltError> {
    let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
        .with_wait_for(WaitFor::message_on_stdout("Server completed initialization."))
        .with_env_var("HOST_COUNT", "1");
    let docker = voltdb.start().unwrap();
    let host_port = docker.get_host_port_ipv4(21211).unwrap();
    let host_ip = IpPort::new("localhost".to_string(), host_port);
    let hosts = vec![host_ip];
    let mut pool = Pool::new(Opts::new(hosts)).unwrap();
    let rc = Arc::new(AtomicPtr::new(&mut pool));
    let mut vec: Vec<JoinHandle<_>> = vec![];
    let start = SystemTime::now();

    for _ in 0..512 {
        let local = Arc::clone(&rc);
        let handle = thread::spawn(move || unsafe {
            let pool = local.load(Acquire);
            let mut c = (*pool).get_conn().unwrap();
            let mut table = c.list_procedures().unwrap();
            table.advance_row();
        }
        );
        vec.push(handle);
    }
    for handle in vec {
        handle.join().unwrap();
    }
    let since_the_epoch = SystemTime::now()
        .duration_since(start)
        .expect("Time went backwards");
    println!("{:?}", since_the_epoch);
    Ok(())
}
