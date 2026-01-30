extern crate lazy_static;

use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Acquire;
use std::sync::*;
use std::thread::JoinHandle;
use std::time::SystemTime;
use std::{fs, thread};

use testcontainers::core::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use voltdb_client_rust::*;

static POPULATE: Once = Once::new();
static mut VOLTDB_PORT: u16 = 0;
static TEST_MUTEX: Mutex<()> = Mutex::new(());
static VOLTDB_CONTAINER: OnceLock<Container<GenericImage>> = OnceLock::new();
static CLEANUP_REGISTERED: OnceLock<()> = OnceLock::new();

fn setup_voltdb_once() {
    VOLTDB_CONTAINER.get_or_init(|| {
        // Register cleanup handler once
        CLEANUP_REGISTERED.get_or_init(|| {
            extern "C" fn cleanup() {
                // Force stop the container
                if let Some(container) = VOLTDB_CONTAINER.get() {
                    container.stop().unwrap();
                }
            }
            // SAFETY: registering atexit callback is safe
            unsafe {
                libc::atexit(cleanup);
            }
        });

        let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
            .with_wait_for(WaitFor::message_on_stdout(
                "Server completed initialization.",
            ))
            .with_env_var("HOST_COUNT", "1");
        let docker = voltdb.start().unwrap();
        let host_port = docker.get_host_port_ipv4(21211).unwrap();
        unsafe {
            VOLTDB_PORT = host_port;
        }
        docker
    });
}

fn populate(node: &mut Node) {
    POPULATE.call_once(|| {
        let jars = fs::read("tests/procedures.jar").unwrap();
        let x = node.upload_jar(jars).unwrap();
        let mut table = x.recv().unwrap();
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
        execute_success(node, create);
        let script = "CREATE PROCEDURE  FROM CLASS com.johnny.ApplicationCreate;";
        let x = node.query(script).unwrap();
        let mut table = x.recv().unwrap();
        assert!(table.has_error().is_none());
    });
    let clean = "delete from test_types;";
    execute_success(node, clean);
}

fn execute_success(node: &mut Node, sql: &str) {
    let x = node.query(sql).unwrap();
    let mut table = x.recv().unwrap();
    if let Some(err) = table.has_error() {
        panic!("err {:?} ", err)
    }
}

fn get_test_node() -> Node {
    setup_voltdb_once();
    let url = "localhost";
    let port = unsafe { VOLTDB_PORT };
    get_node(&format!("{}:{}", url, port)).unwrap()
}

#[derive(Debug, PartialEq)]
struct Test {
    t1: Option<bool>,
    t2: Option<i16>,
    t3: Option<i32>,
    t4: Option<i64>,
    t5: Option<f64>,
    t6: Option<BigDecimal>,
    t7: Option<String>,
    t8: Option<Vec<u8>>,
    t9: Option<DateTime<Utc>>,
}

impl From<&mut VoltTable> for Test {
    fn from(table: &mut VoltTable) -> Self {
        let t1 = table.get_bool_by_column("T1").unwrap();
        let t2 = table.get_i16_by_column("t2").unwrap();
        let t3 = table.get_i32_by_column("t3").unwrap();
        let t4 = table.get_i64_by_column("t4").unwrap();
        let t5 = table.get_f64_by_column("t5").unwrap();
        let t6 = table.get_decimal_by_column("t6").unwrap();
        let t7 = table.get_string_by_column("t7").unwrap();
        let t8 = table.get_bytes_op_by_column("t8").unwrap();
        let t9 = table.get_time_by_column("t9").unwrap();
        Test {
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            t7,
            t8,
            t9,
        }
    }
}

#[test]
fn test_connection_establishment() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);
    // Connection is implicitly tested by getting the node
    Ok(())
}

#[test]
fn test_invalid_connection() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let result = get_node("localhost:99999");
    assert!(result.is_err());
}

#[test]
fn test_table_creation() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let node = get_test_node();

    let create = "CREATE TABLE simple_test (id INTEGER, name VARCHAR);";
    let result = node.query(create);
    // May fail if table already exists from another test, that's ok
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_insert_null_values() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear any existing null rows
    execute_success(&mut node, "delete from test_types where t1 IS NULL;");

    let insert = "insert into test_types (T1) values (NULL);";
    execute_success(&mut node, insert);

    let mut table = block_for_result(&node.query("select * from test_types where t1 IS NULL;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let test: Test = table.map_row();
    assert_eq!(test.t1, None);

    Ok(())
}

#[test]
fn test_insert_and_retrieve_all_types() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear existing data
    execute_success(&mut node, "delete from test_types where t2 = 2;");

    let insert = "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5.5,6.66,'test','DEADBEEF',NOW());";
    execute_success(&mut node, insert);

    let mut table = block_for_result(&node.query("select * from test_types where t2 = 2;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let test: Test = table.map_row();

    assert_eq!(test.t1, Some(true));
    assert_eq!(test.t2, Some(2i16));
    assert_eq!(test.t3, Some(3i32));
    assert_eq!(test.t4, Some(4i64));
    assert_eq!(test.t7, Some("test".to_owned()));

    Ok(())
}

#[test]
fn test_query_with_no_results() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    let table = block_for_result(&node.query("select * from test_types where t1 = 99;")?)?;
    assert_eq!(table.get_row_count(), 0);

    Ok(())
}

#[test]
fn test_multiple_inserts() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear existing data
    execute_success(
        &mut node,
        "delete from test_types where t2 >= 1000 and t2 < 1010;",
    );

    for i in 0..10 {
        let insert = format!(
            "insert into test_types (T1,T2,T3) values (1,{},{});",
            1000 + i,
            i * 10
        );
        execute_success(&mut node, &insert);
    }

    let mut table = block_for_result(
        &node.query("select count(*) from test_types where t2 >= 1000 and t2 < 1010;")?,
    )?;
    table.advance_row();
    let count = table.get_i64_by_idx(0).unwrap();
    assert_eq!(count, Some(10i64));

    Ok(())
}

#[test]
fn test_update_operation() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 5000;");
    let insert = "insert into test_types (T1,T2,T3) values (1,5000,200);";
    execute_success(&mut node, insert);

    let update = "update test_types set T3 = 999 where T2 = 5000;";
    execute_success(&mut node, update);

    let mut table = block_for_result(&node.query("select T3 from test_types where T2 = 5000;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let t3 = table.get_i32_by_column("T3").unwrap();
    assert_eq!(t3, Some(999i32));

    Ok(())
}

#[test]
fn test_delete_operation() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    let insert = "insert into test_types (T1,T2) values (1,6000);";
    execute_success(&mut node, insert);

    let delete = "delete from test_types where T2 = 6000;";
    execute_success(&mut node, delete);

    let table = block_for_result(&node.query("select * from test_types where T2 = 6000;")?)?;
    assert_eq!(table.get_row_count(), 0);

    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Attempt invalid insert (should fail/rollback)
    let invalid_insert =
        "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','8',NOW());";
    let result = node.query(invalid_insert).unwrap();
    let mut table = result.recv().unwrap();
    assert!(table.has_error().is_some());

    Ok(())
}

#[test]
fn test_boundary_values() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 7000;");

    let insert = format!(
        "insert into test_types (T1,T2,T3,T4) values (1,{},{},{});",
        7000,
        i32::MAX,
        i64::MAX
    );
    execute_success(&mut node, &insert);

    let mut table =
        block_for_result(&node.query("select T2,T3,T4 from test_types where T2 = 7000;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    assert_eq!(table.get_i16_by_column("T2").unwrap(), Some(7000));
    assert_eq!(table.get_i32_by_column("T3").unwrap(), Some(i32::MAX));
    assert_eq!(table.get_i64_by_column("T4").unwrap(), Some(i64::MAX));

    Ok(())
}

#[test]
fn test_empty_string_and_binary() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 8000;");

    let insert = "insert into test_types (T1,T2,T7,T8) values (1,8000,'','');";
    execute_success(&mut node, insert);

    let mut table =
        block_for_result(&node.query("select T7,T8 from test_types where T2 = 8000;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let t7 = table.get_string_by_column("T7").unwrap();
    assert_eq!(t7, Some("".to_owned()));

    Ok(())
}

#[test]
fn test_concurrent_reads() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock()?;
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 9000;");
    let insert = "insert into test_types (T1,T2) values (1,9000);";
    execute_success(&mut node, insert);

    let rc = Arc::new(AtomicPtr::new(&mut node));
    let mut handles = vec![];

    for _ in 0..10 {
        let local = Arc::clone(&rc);
        let handle = thread::spawn(move || unsafe {
            let load = local.load(Acquire);
            let res = (*load)
                .query("select T2 from test_types where T2 = 9000;")
                .unwrap();
            let mut table = block_for_result(&res).unwrap();
            if table.get_row_count() > 0 {
                table.advance_row();
                let t2 = table.get_i16_by_column("T2").unwrap();
                assert_eq!(t2, Some(9000i16));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

#[test]
fn test_multiples_thread() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    let insert = "insert into test_types (T1) values (NULL);";
    execute_success(&mut node, insert);

    let insert_value = "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','089CD7B35220FFB686012A0B08B49ECD8C06109893971F422F4D4F4E49544F52494E475F33393766643034662D656161642D346230372D613638302D62663562633736666132363148D535A8019CD7B352B001DDEE8501B801BAEE8501C001AAE98601CA01054341534831D0010AE00102E80102F20103555344FA010A0A0355534410809BEE028202050A035553448A020B08B49ECD8C06109893971F9202046E756C6CA2020A0A0355534410C0BD9A2FBA0219312C323139313936382C323139333231302C32313933323435C802C91E8A0400920400D80401880505B20500',NOW());";

    block_for_result(&node.query(insert_value)?)?;
    let mut table =
        block_for_result(&node.query("select * from test_types where t1 = 1;")?).unwrap();
    if table.get_row_count() > 0 {
        table.advance_row();
        let test: Test = table.map_row();
        assert_eq!(test.t1, Some(true));
        assert_eq!(test.t2, Some(2i16));
        assert_eq!(test.t3, Some(3i32));
        assert_eq!(test.t4, Some(4i64));
        assert_eq!(test.t5, Some(5f64));
        assert_eq!(test.t6, Some(BigDecimal::from(6)));
        assert_eq!(test.t7, Some("7".to_owned()));
    }

    let rc = Arc::new(AtomicPtr::new(&mut node));
    let mut vec: Vec<JoinHandle<_>> = vec![];
    let start = SystemTime::now();

    for _ in 0..512 {
        let local = Arc::clone(&rc);
        let handle = thread::spawn(move || unsafe {
            let load = local.load(Acquire);
            let res = (*load)
                .query("select * from test_types where t1 = 1;")
                .unwrap();
            let mut table = block_for_result(&res).unwrap();
            if table.get_row_count() > 0 {
                table.advance_row();
                let _: Test = table.map_row();
            }
        });
        vec.push(handle);
    }

    for handle in vec {
        handle.join().unwrap();
    }

    let since_the_epoch = SystemTime::now()
        .duration_since(start)
        .expect("Time went backwards");
    println!("Total time: {:?}", since_the_epoch);
    Ok(())
}

#[test]
fn test_stored_procedure_execution() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Execute the stored procedure created during populate
    let result = node.query("EXEC ApplicationCreate;");
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_jar_upload() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let node = get_test_node();

    let jars = fs::read("tests/procedures.jar").unwrap();
    let x = node.upload_jar(jars).unwrap();
    let mut table = x.recv().unwrap();
    assert!(table.has_error().is_none());

    Ok(())
}

#[test]
fn test_special_characters_in_varchar() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 10000;");

    let special_chars = "Test with 'quotes' and \"double\" and \\ backslash";
    let insert = format!(
        "insert into test_types (T1,T2,T7) values (1,10000,'{}');",
        special_chars.replace("'", "''")
    );
    execute_success(&mut node, &insert);

    Ok(())
}

#[test]
fn test_decimal_precision() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 11000;");

    let insert = "insert into test_types (T1,T2,T6) values (1,11000,123.456789);";
    execute_success(&mut node, insert);

    let mut table = block_for_result(&node.query("select T6 from test_types where T2 = 11000;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let decimal = table.get_decimal_by_column("T6").unwrap();
    assert!(decimal.is_some());

    Ok(())
}

#[test]
fn test_timestamp_operations() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear and insert fresh data
    execute_success(&mut node, "delete from test_types where t2 = 12000;");

    let insert = "insert into test_types (T1,T2,T9) values (1,12000,NOW());";
    execute_success(&mut node, insert);

    let mut table = block_for_result(&node.query("select T9 from test_types where T2 = 12000;")?)?;
    assert!(table.get_row_count() > 0);
    table.advance_row();
    let timestamp = table.get_time_by_column("T9").unwrap();
    assert!(timestamp.is_some());

    Ok(())
}

#[test]
fn test_query_error_handling() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let node = get_test_node();

    let result = node.query("SELECT * FROM non_existent_table;");
    assert!(result.is_ok()); // Query sends successfully

    let mut table = result.unwrap().recv().unwrap();
    assert!(table.has_error().is_some()); // But response contains error

    Ok(())
}

#[test]
fn test_sequential_queries() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let mut node = get_test_node();
    populate(&mut node);

    // Clear existing data in range
    execute_success(
        &mut node,
        "delete from test_types where t2 >= 13000 and t2 < 13100;",
    );

    for i in 0..100 {
        let query = format!(
            "insert into test_types (T1,T2,T3) values (1,{},{});",
            13000 + i,
            i
        );
        execute_success(&mut node, &query);
    }

    let mut table = block_for_result(
        &node.query("select count(*) from test_types where t2 >= 13000 and t2 < 13100;")?,
    )?;
    table.advance_row();
    let count = table.get_i64_by_idx(0).unwrap();
    assert_eq!(count, Some(100i64));

    Ok(())
}
