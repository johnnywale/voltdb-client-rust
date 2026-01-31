//! This crate offers:
//!
//! *   Voltdb database driver in pure rust;
//!
//! ## Installation
//!
//! Put the desired version of the crate into the `dependencies` section of your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! voltdb-client-rust = "0.1"
//! ```
//!
//! ## Example
//!
//!
//!
//! ```no_run
//!use std::fs;
//!use std::time::SystemTime;
//!
//!use voltdb_client_rust::*;
//!
//!fn main() -> Result<(), VoltError> {
//!    #[derive(Debug)]
//!    struct Test {
//!        t1: Option<i8>,
//!        t2: Option<i16>,
//!        t3: Option<i32>,
//!        t4: Option<i64>,
//!        t5: Option<f64>,
//!        t6: Option<BigDecimal>,
//!        t7: Option<String>,
//!        t8: Option<Vec<u8>>,
//!        t9: Option<DateTime<Utc>>,
//!    }
//!    /// Convert table to struct.
//!    impl From<&mut VoltTable> for Test {
//!        fn from(table: &mut VoltTable) -> Self {
//!            Test {
//!                t1: table.fetch("T1").unwrap(),
//!                t2: table.fetch("t2").unwrap(),
//!                t3: table.fetch("t3").unwrap(),
//!                t4: table.fetch("t4").unwrap(),
//!                t5: table.fetch("t5").unwrap(),
//!                t6: table.fetch("t6").unwrap(),
//!                t7: table.fetch("t7").unwrap(),
//!                t8: table.fetch("t8").unwrap(),
//!                t9: table.fetch("t9").unwrap(),
//!            }
//!        }
//!    }
//!
//!    let hosts = vec![IpPort::new("localhost".to_string(), 21211)];
//!    let mut pool = Pool::new(Opts::new(hosts)).unwrap();
//!
//!    let mut node = pool.get_conn()?;
//!    // Create table if not exists.
//!    let has_table_check = node.query("select t1 from test_types limit 1");
//!    match has_table_check {
//!        Ok(_) => {}
//!        Err(err) => {
//!            println!("{:?}", err);
//!            println!("will create table");
//!            let create_table = "CREATE TABLE  test_types (t1 TINYINT,t2 SMALLINT,t3 INTEGER,t4 BIGINT,t5 FLOAT,t6 DECIMAL,t7 VARCHAR,t8 VARBINARY, t9 TIMESTAMP);";
//!            node.query(create_table)?;
//!        }
//!    }
//!
//!    // Insert empty data into table , make sure None is working.
//!    let insert = "insert into test_types (T1) values (NULL);";
//!    node.query(insert)?;
//!    // Insert min/max value to table to validate the encoding.
//!    node.query("insert into test_types (T1,T2,T3,T4) values (1, -32767, -2147483647, -9223372036854775807 );")?;
//!    node.query("insert into test_types (T1,T2,T3,T4) values (1, 32767, 2147483647, 9223372036854775807 );")?;
//!    let mut table = node.query("select * from test_types")?;
//!    while table.advance_row() {
//!        let test: Test = table.map_row();
//!        println!("{:?}", test);
//!    }
//!    let bs = vec![1 as u8, 2, 3, 4];
//!    let time = DateTime::from(SystemTime::now());
//!    let none_i16: Option<i16> = Option::None;
//!
//!
//!    // call sp with marco `volt_parma!` , test_types.insert is crated with table.
//!    let mut table = node.call_sp("test_types.insert", volt_param![1,none_i16,3,4,5,6,"7",bs,time])?;
//!    while table.advance_row() {
//!        println!("{}", table.debug_row());
//!    }
//!
//!    // upload and create sp if not exists. the proc is looks like this
//!    // package com.johnny;
//!    //
//!    // import org.voltdb.SQLStmt;
//!    // import org.voltdb.VoltProcedure;
//!    // import org.voltdb.VoltTable;
//!    // import org.voltdb.VoltType;
//!    //
//!    //
//!    // public class ApplicationCreate extends VoltProcedure {
//!    //
//!    //     final SQLStmt appCreate = new SQLStmt("insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (?,?,?,?,?,?,?,?,?);");
//!    //
//!    //
//!    //     public VoltTable run(VoltTable application) {
//!    //         if (application.advanceRow()) {
//!    //             Object[] row = new Object[application.getColumnCount()];
//!    //             row[0] = application.get(0, VoltType.TINYINT);
//!    //             row[1] = application.get(1, VoltType.SMALLINT);
//!    //             row[2] = application.get(2, VoltType.INTEGER);
//!    //             row[3] = application.get(3, VoltType.BIGINT);
//!    //             row[4] = application.get(4, VoltType.FLOAT);
//!    //             row[5] = application.get(5, VoltType.DECIMAL);
//!    //             row[6] = application.get(6, VoltType.STRING);
//!    //             row[7] = application.get(7, VoltType.VARBINARY);
//!    //             row[8] = application.get(8, VoltType.TIMESTAMP);
//!    //             voltQueueSQL(appCreate, row);
//!    //         }
//!    //         voltExecuteSQL(true);
//!    //         return application;
//!    //     }
//!    // }
//!    let mut table = node.list_procedures()?;
//!    let mut sp_created = false;
//!    while table.advance_row() {
//!        if table.get_string_by_column("PROCEDURE_NAME")?.unwrap() == "ApplicationCreate" {
//!            sp_created = true;
//!            println!("already created {}", table.debug_row());
//!            break;
//!        }
//!    }
//!
//!    if !sp_created {
//!        // upload proc into server
//!        let jars = fs::read("tests/procedures.jar").unwrap();
//!        let mut table = node.upload_jar(jars).unwrap();
//!        assert!(table.has_error().is_none());
//!        let script = "CREATE PROCEDURE  FROM CLASS com.johnny.ApplicationCreate;";
//!        node.query(script)?;
//!    }
//!
//!
//!    let null_i16: Option<i16> = Option::None;
//!    let header = vec!["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9"];
//!    let tp = vec![TINYINT_COLUMN, SHORT_COLUMN, INT_COLUMN, LONG_COLUMN, FLOAT_COLUMN, DECIMAL_COLUMN, STRING_COLUMN, VAR_BIN_COLUMN, TIMESTAMP_COLUMN];
//!    let header: Vec<String> = header.iter().map(|f| f.to_string()).collect::<Vec<String>>();
//!    let mut table = VoltTable::new_table(tp, header);
//!    let decimal = BigDecimal::from(16);
//!    let data = volt_param! {true, null_i16 , 2147483647 as i32,  9223372036854775807 as i64 ,  15.0, decimal  , "17",bs, time   };
//!    table.add_row(data)?;
//!    // call proc with volt table
//!    let mut res = node.call_sp("ApplicationCreate", volt_param![table])?;
//!    while res.advance_row() {
//!        println!("{:?}", res.debug_row());
//!    }
//!    Ok({})
//!}

#![crate_name = "voltdb_client_rust"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]

pub mod encode;
mod encode_option;
pub mod error;
mod generate;
pub mod node;
mod pool;
pub mod pool_core;
mod procedure_invocation;
pub mod protocol;
mod response;
pub mod table;

pub mod async_node;
pub mod async_pool;

pub use bigdecimal;
pub use chrono;

pub use crate::bigdecimal::BigDecimal;
pub use crate::chrono::{DateTime, Utc};
pub use crate::encode::*;
pub use crate::error::{
    ConcurrencyError, ConnectionError, IntoVoltError, ProtocolError, QueryError,
};
pub use crate::node::{OptsBuilder, *};
pub use crate::pool::*;
pub use crate::pool_core::{ExhaustionPolicy, ValidationMode};
pub use crate::table::*;

#[cfg(feature = "tokio")]
pub use crate::async_node::{AsyncNode, async_block_for_result};
#[cfg(feature = "tokio")]
pub use crate::async_pool::{AsyncPool, AsyncPooledConn};

#[macro_export]
macro_rules! volt_param {
    () => (
        std::vec::Vec::new()
    );
    ( $( $x:expr ),* ) => {
        {
            #[allow(clippy::vec_init_then_push)]
            {
                let mut temp_vec = Vec::new();
                $(
                    temp_vec.push(&$x as &dyn Value);
                )*
                temp_vec
            }
        }
    };
}
