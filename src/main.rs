use std::fs;
use std::time::SystemTime;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};

use voltdb_client_rust::encode::*;
use voltdb_client_rust::node::*;
use voltdb_client_rust::table::VoltTable;
use voltdb_client_rust::volt_param;

fn main() -> Result<(), VoltError> {
    #[derive(Debug)]
    struct Test {
        t1: Option<i8>,
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
            Test {
                t1: table.get_i8_by_column("T1").unwrap(),
                t2: table.get_i16_by_column("t2").unwrap(),
                t3: table.get_i32_by_column("t3").unwrap(),
                t4: table.get_i64_by_column("t4").unwrap(),
                t5: table.get_f64_by_column("t5").unwrap(),
                t6: table.get_decimal_by_column("t6").unwrap(),
                t7: table.get_string_by_column("t7").unwrap(),
                t8: table.get_bytes_op_by_column("t8").unwrap(),
                t9: table.get_time_by_column("t9").unwrap(),
            }
        }
    }

    let mut node = get_node("localhost:21211")?;

    let has_table_check = block_for_result(&node.query("select t1 from test_types limit 1")?);
    match has_table_check {
        Ok(_) => {}
        Err(err) => {
            println!("{:?}", err);
            println!("will create table");
            let create_table = "CREATE TABLE  test_types
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
            block_for_result(&node.query(create_table)?)?;
        }
    }

    let insert = "insert into test_types (T1) values (NULL);";
    block_for_result(&node.query(insert)?)?;
    block_for_result(&node.query("insert into test_types (T1,T2,T3,T4) values (1, -32767, -2147483647, -9223372036854775807 );")?)?;
    block_for_result(&node.query("insert into test_types (T1,T2,T3,T4) values (1, 32767, 2147483647, 9223372036854775807 );")?)?;
    let mut table = block_for_result(&node.query("select * from test_types")?)?;
    while table.advance_row() {
        let test: Test = table.map_row();
        println!("{:?}", test);
    }
    let bs = vec![1 as u8, 2, 3, 4];
    let time = DateTime::from(SystemTime::now());


    // call proc with parameters
    let mut table = block_for_result(&node.call_sp("test_types.insert", volt_param![1,2,3,4,5,6,"7",bs,time])?)?;
    while table.advance_row() {
        println!("{}", table.debug_row());
    }
    // upload proc into server
    let jars = fs::read("tests/procedures.jar").unwrap();
    let x = node.upload_jar(jars).unwrap();
    let mut table = x.recv().unwrap();
    assert!(table.has_error().is_none());
    // create sp
    let script = "CREATE PROCEDURE  FROM CLASS com.johnny.ApplicationCreate;";
    let res = block_for_result(&node.query(script)?);
    match res {
        Ok(_) => {}
        Err(err) => {
            println!("{}", err)
        }
    };
    let header = vec!["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9"];
    let tp = vec![TINYINT_COLUMN, SHORT_COLUMN, INT_COLUMN, LONG_COLUMN, FLOAT_COLUMN, DECIMAL_COLUMN, STRING_COLUMN, VAR_BIN_COLUMN, TIMESTAMP_COLUMN];
    let header: Vec<String> = header.iter().map(|f| f.to_string()).collect::<Vec<String>>();
    let mut table = VoltTable::new_table(tp, header);
    let decimal = BigDecimal::from(16);
    let data = volt_param! {true, 32767 as i16 , 2147483647 as i32,  9223372036854775807 as i64 ,  15.0, decimal  , "17",bs, time   };
    table.add_row(data)?;
    // call proc with volt table
    let mut res = block_for_result(&node.call_sp("ApplicationCreate", volt_param![table])?)?;
    while res.advance_row() {
        println!("{:?}", res.debug_row());
    }
    Ok({})
}
