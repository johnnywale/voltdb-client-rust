use std::{fs, panic};
use std::net::SocketAddr;
use std::sync::{Once};

use testcontainers::{*};
use testcontainers::images::generic::{GenericImage, Stream, WaitFor};
use testcontainers::clients::Cli;

use voltdb_client_rust::encode::{VoltError, Value};
use voltdb_client_rust::node::*;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::collections::HashMap;
use lazy_static::LazyStatic;

extern crate lazy_static;


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

use lazy_static::lazy_static;

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
        let script = "-- simple example table
CREATE TABLE app_session (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);
-- partitioning this table will make it fast and scalable
PARTITION TABLE app_session ON COLUMN deviceid;
-- create an index to allow faster access to the table based on a given deviceid
CREATE INDEX app_session_idx ON app_session (deviceid);


-- this view summarizes how many sessions have been inserted for each app / device combination
CREATE VIEW app_usage AS
SELECT appid, deviceid, count(*) as ct
FROM app_session
GROUP BY appid, deviceid;

-- you can declare any SQL statement as a procedure
CREATE PROCEDURE apps_by_unique_devices AS
SELECT appid, COUNT(deviceid) as unique_devices, SUM(ct) as total_sessions
FROM app_usage
GROUP BY appid
ORDER BY unique_devices DESC;

-- another example of making a procedure from a SQL statement, in this case to insert into the table without a ts value, the default to set it to now
CREATE PROCEDURE insert_session PARTITION ON TABLE app_session COLUMN deviceid PARAMETER 1 AS
INSERT INTO app_session (appid, deviceid) VALUES (?,?);

-- create a procedure from a java class
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS simple.SelectDeviceSessions;

-- execute the batch of DDL statements";
        let x = node.query(script).unwrap();
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

fn execute_print_res(node: &mut Node, sql: &str) {
    let x = node.query(sql).unwrap();
    let mut table = x.recv().unwrap();
    table.column_types();
    let column = table.columns();
    let size = column.len();
    while table.advance_row() {
        println!("{}", table.debug_row());
    }
}

#[test]
fn table_column_test() {
    let url = init();
    let mut node = get_node(url.as_str()).unwrap();
    populate(&mut node);
    let insert = "insert into test_types (T1) values (NULL);";
    execute_success(&mut node, insert);
    let x = node.query("insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','8',NOW());").unwrap();
    let mut table = x.recv().unwrap();
    assert!(table.has_error().is_some());


    let insert_value = "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','089CD7B35220FFB686012A0B08B49ECD8C06109893971F422F4D4F4E49544F52494E475F33393766643034662D656161642D346230372D613638302D62663562633736666132363148D535A8019CD7B352B001DDEE8501B801BAEE8501C001AAE98601CA01054341534831D0010AE00102E80102F20103555344FA010A0A0355534410809BEE028202050A035553448A020B08B49ECD8C06109893971F9202046E756C6CA2020A0A0355534410C0BD9A2FBA0219312C323139313936382C323139333231302C32313933323435C802C91E8A0400920400D80401880505B20500',NOW());";
    execute_success(&mut node, insert_value);
    execute_print_res(&mut node, "select * from test_types;");
}
