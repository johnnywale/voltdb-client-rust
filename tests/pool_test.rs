extern crate lazy_static;

use std::{fs, panic, thread};
use std::borrow::BorrowMut;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, null_mut};
use std::rc::Rc;
use std::sync::{Arc, Mutex, Once};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::atomic::Ordering::Acquire;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use testcontainers::clients::Cli;
use testcontainers::Docker;
use testcontainers::images::generic::{GenericImage, Stream, WaitFor};

use voltdb_client_rust::*;

#[test]
fn test_pool() -> Result<(), VoltError> {
    let c = Cli::default();
    let wait = WaitFor::LogMessage { message: "Server completed initialization.".to_owned(), stream: Stream::StdOut };
    let voltdb = GenericImage::new("voltdb/voltdb-community:9.2.1")
        .with_env_var("HOST_COUNT", "1")
        .with_wait_for(wait);
    let docker = c.run(voltdb);
    let host_port = docker.get_host_port(21211);
    let mut pool = Pool::new(Opts::new("localhost".to_string(), host_port.unwrap())).unwrap();
    let rc = Arc::new(AtomicPtr::new(&mut pool));
    let mut vec: Vec<JoinHandle<_>> = vec![];
    let start = SystemTime::now();

    for i in 0..512 {
        let mut local = Arc::clone(&rc);
        let handle = thread::spawn(move || unsafe {
            let pool = local.load(Acquire);
            let mut c = (*pool).get_conn().unwrap();
            let res = c.list_procedures().unwrap();
            let mut table = block_for_result(&res).unwrap();
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
