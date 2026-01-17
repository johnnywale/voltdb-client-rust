use std::sync::Arc;
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
        .with_wait_for(WaitFor::message_on_stdout(
            "Server completed initialization.",
        ))
        .with_env_var("HOST_COUNT", "1");
    let docker = voltdb.start().unwrap();
    let host_port = docker.get_host_port_ipv4(21211).unwrap();
    let host_ip = IpPort::new("localhost".to_string(), host_port);
    let hosts = vec![host_ip];

    // VoltDB connections support concurrent requests via handle-based tracking,
    // so multiple threads can share the same connection safely.
    // No Block policy needed - connections are shared, not exclusive.
    let pool = Arc::new(Pool::new(Opts::new(hosts)).unwrap());
    let mut vec: Vec<JoinHandle<_>> = vec![];
    let start = SystemTime::now();

    for _ in 0..512 {
        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            let mut c = pool_clone.get_conn().unwrap();
            let mut table = c.list_procedures().unwrap();
            table.advance_row();
        });
        vec.push(handle);
    }
    for handle in vec {
        handle.join().unwrap();
    }
    let since_the_epoch = SystemTime::now()
        .duration_since(start)
        .expect("Time went backwards");
    println!("{:?}", since_the_epoch);

    // Verify pool stats
    let stats = pool.stats();
    println!("Pool stats: {:?}", stats);
    assert_eq!(stats.size, 10);
    assert_eq!(stats.healthy, 10);
    assert_eq!(stats.total_requests, 512);
    assert!(!stats.is_shutdown);

    Ok(())
}
