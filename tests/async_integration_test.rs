extern crate lazy_static;

use std::fs;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Acquire;
use std::sync::{Arc, Once};
use std::thread::JoinHandle;
use std::time::{Instant, SystemTime};

use futures::future::join_all;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use voltdb_client_rust::async_node::{AsyncNode, async_block_for_result};
use voltdb_client_rust::*;

static POPULATE: Once = Once::new();

async fn populate(node: &AsyncNode) {
    POPULATE.call_once(|| ());

    let jars = fs::read("tests/procedures.jar").unwrap();

    let mut rx = node.upload_jar(jars).await.unwrap();
    let mut table = async_block_for_result(&mut rx).await.unwrap();
    assert!(table.has_error().is_none());

    let create = "CREATE TABLE test_types
                    (
                    t1 TINYINT,
                    t2 SMALLINT,
                    t3 INTEGER,
                    t4 BIGINT,
                    t5 FLOAT,
                    t6 DECIMAL,
                    t7 VARCHAR,
                    t8 VARBINARY,
                    t9 TIMESTAMP,
                    );";

    execute_success(node, create).await;

    let script = "CREATE PROCEDURE FROM CLASS com.johnny.ApplicationCreate;";
    let mut rx = node.query(script).await.unwrap();
    let mut table = async_block_for_result(&mut rx).await.unwrap();
    assert!(table.has_error().is_none());
}

async fn execute_success(node: &AsyncNode, sql: &str) {
    let mut rx = node.query(sql).await.unwrap();
    let mut table = async_block_for_result(&mut rx).await.unwrap();
    if let Some(err) = table.has_error() {
        panic!("err {:?}", err);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_async_multiple_tasks() -> Result<(), VoltError> {
    // ------------------------------------------------------------------
    // Start VoltDB container
    // ------------------------------------------------------------------
    let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
        .with_wait_for(WaitFor::message_on_stdout(
            "Server completed initialization.",
        ))
        .with_env_var("HOST_COUNT", "1");

    let docker = voltdb.start().await.unwrap();
    let port = docker.get_host_port_ipv4(21211).await.unwrap();

    let mut node = AsyncNode::new(NodeOpt {
        ip_port: IpPort::new("127.0.0.1".to_string(), port),
        user: None,
        pass: None,
    })
    .await?;

    populate(&node).await;

    // ------------------------------------------------------------------
    // Inserts
    // ------------------------------------------------------------------
    execute_success(&node, "insert into test_types (T1) values (NULL);").await;

    let mut rx = node
        .query(
            "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','8',NOW());",
        )
        .await?;
    let mut table = async_block_for_result(&mut rx).await?;
    assert!(table.has_error().is_some());

    execute_success(
        &node,
        "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9)
         values (1,2,3,4,5,6,'7','089CD7B35220FFB6',NOW());",
    )
    .await;

    // ------------------------------------------------------------------
    // Validate row
    // ------------------------------------------------------------------
    let mut rx = node.query("select * from test_types where t1 = 1;").await?;
    let mut table = async_block_for_result(&mut rx).await.unwrap();
    table.advance_row();

    #[derive(Debug)]
    struct Test {
        t1: Option<bool>,
        t2: Option<i16>,
        t3: Option<i32>,
        t4: Option<i64>,
        t5: Option<f64>,
        t6: Option<BigDecimal>,
        t7: Option<String>,
    }

    impl From<&mut VoltTable> for Test {
        fn from(table: &mut VoltTable) -> Self {
            Test {
                t1: table.get_bool_by_column("t1").unwrap(),
                t2: table.get_i16_by_column("t2").unwrap(),
                t3: table.get_i32_by_column("t3").unwrap(),
                t4: table.get_i64_by_column("t4").unwrap(),
                t5: table.get_f64_by_column("t5").unwrap(),
                t6: table.get_decimal_by_column("t6").unwrap(),
                t7: table.get_string_by_column("t7").unwrap(),
            }
        }
    }

    let row: Test = table.map_row();
    assert_eq!(row.t1, Some(true));
    assert_eq!(row.t2, Some(2));
    assert_eq!(row.t3, Some(3));

    // ------------------------------------------------------------------
    // Massive concurrent async queries (replacement for threads)
    // ------------------------------------------------------------------
    let start = Instant::now();

    let mut tasks = Vec::with_capacity(512);
    let node = Arc::new(node);

    for _ in 0..512 {
        let node = Arc::clone(&node);

        tasks.push(tokio::spawn(async move {
            let mut rx = node
                .query("select * from test_types where t1 = 1;")
                .await
                .unwrap();

            let mut table = async_block_for_result(&mut rx).await.unwrap();
            table.advance_row();

            let _: Test = table.map_row();
        }));
    }

    join_all(tasks).await;

    println!("Elapsed: {:?}", start.elapsed());

    node.shutdown().await?;
    Ok(())
}
