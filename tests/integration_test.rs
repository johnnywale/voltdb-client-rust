extern crate lazy_static;

use std::{fs, panic};
use std::sync::Once;


use lazy_static::lazy_static;
use testcontainers::{*};
use testcontainers::clients::Cli;
use testcontainers::images::generic::{GenericImage, Stream, WaitFor};

use voltdb_client_rust::*;


lazy_static! {
    static ref CLI: Cli = {
        clients::Cli::default()
    };
     static ref DOCKER: Container<'static, Cli, GenericImage> = {
            let wait = WaitFor::LogMessage { message: "Server completed initialization.".to_owned(), stream: Stream::StdOut };
            let voltdb = GenericImage::new("voltdb/voltdb-community:9.2.1")
                .with_env_var("HOST_COUNT", "1")
                .with_wait_for(wait);
            CLI.run(voltdb)
    };
}

static mut VAL: String = String::new();
static INIT: Once = Once::new();
static POPULATE: Once = Once::new();

fn init() -> String {
    unsafe {
        INIT.call_once(|| {
            let host_port = DOCKER.get_host_port(21211);
            VAL = String::from(format!("localhost:{}", host_port.unwrap()).to_owned());
        });
        return VAL.clone();
    }
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
}

#[test]
fn jar_upload() -> Result<(), VoltError> {
    let url = init();
    let mut node = get_node(url.as_str()).unwrap();
    populate(&mut node);
    Ok(())
}

fn execute_success(node: &mut Node, sql: &str) {
    let x = node.query(sql).unwrap();
    let mut table = x.recv().unwrap();
    let err = table.has_error();
    if err.is_some() {
        panic!("err {:?} ", err.unwrap())
    }
}


#[test]
fn table_column_test() -> Result<(), VoltError> {
    #[derive(Debug)]
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

    let url = init();
    let mut node = get_node(url.as_str()).unwrap();
    populate(&mut node);
    let insert = "insert into test_types (T1) values (NULL);";
    execute_success(&mut node, insert);
    let x = node.query("insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','8',NOW());").unwrap();
    let mut table = x.recv().unwrap();
    assert!(table.has_error().is_some());


    let insert_value = "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','089CD7B35220FFB686012A0B08B49ECD8C06109893971F422F4D4F4E49544F52494E475F33393766643034662D656161642D346230372D613638302D62663562633736666132363148D535A8019CD7B352B001DDEE8501B801BAEE8501C001AAE98601CA01054341534831D0010AE00102E80102F20103555344FA010A0A0355534410809BEE028202050A035553448A020B08B49ECD8C06109893971F9202046E756C6CA2020A0A0355534410C0BD9A2FBA0219312C323139313936382C323139333231302C32313933323435C802C91E8A0400920400D80401880505B20500',NOW());";

    block_for_result(&node.query(insert_value)?)?;
    let mut table = block_for_result(&node.query("select * from test_types where t1 = 1;")?).unwrap();
    table.advance_row();
    let test: Test = table.map_row();
    assert_eq!(test.t1, Some(true));
    assert_eq!(test.t2, Some(2 as i16));
    assert_eq!(test.t3, Some(3 as i32));
    assert_eq!(test.t4, Some(4 as i64));
    assert_eq!(test.t5, Some(5 as f64));
    assert_eq!(test.t6, Some(BigDecimal::from(6)));
    assert_eq!(test.t7, Some("7".to_owned()));
    Ok(())
}
