#![cfg(feature = "tokio")]

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use voltdb_client_rust::*;

#[tokio::test]
async fn test_async_node() -> Result<(), VoltError> {
    let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "Server completed initialization.",
        ))
        .with_env_var("HOST_COUNT", "1");
    let docker = voltdb.start().await.unwrap();
    let host_port = docker.get_host_port_ipv4(21211).await.unwrap();

    let opt = NodeOpt {
        ip_port: IpPort::new("localhost".to_string(), host_port),
        user: None,
        pass: None,
        connect_timeout: None,
        read_timeout: None,
    };

    let node = AsyncNode::new(opt).await?;

    let mut rx = node.list_procedures().await?;
    let mut table = async_block_for_result(&mut rx).await?;

    assert!(table.has_error().is_none());

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_async_pool() -> Result<(), VoltError> {
    let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "Server completed initialization.",
        ))
        .with_env_var("HOST_COUNT", "1");
    let docker = voltdb.start().await.unwrap();
    let host_port = docker.get_host_port_ipv4(21211).await.unwrap();

    let hosts = vec![IpPort::new("localhost".to_string(), host_port)];
    let pool = AsyncPool::new(Opts::new(hosts)).await?;

    let conn = pool.get_conn();
    let mut table = conn.list_procedures().await?;
    assert!(table.has_error().is_none());

    Ok(())
}

#[tokio::test]
async fn test_async_pool_multiple_queries() -> Result<(), VoltError> {
    let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "Server completed initialization.",
        ))
        .with_env_var("HOST_COUNT", "1");
    let docker = voltdb.start().await.unwrap();
    let host_port = docker.get_host_port_ipv4(21211).await.unwrap();

    let hosts = vec![IpPort::new("localhost".to_string(), host_port)];
    let pool = AsyncPool::new(Opts::new(hosts)).await?;

    // Test multiple queries using different connections from the pool
    for _ in 0..10 {
        let conn = pool.get_conn();
        let mut table = conn.list_procedures().await?;
        assert!(table.has_error().is_none());
    }

    Ok(())
}
