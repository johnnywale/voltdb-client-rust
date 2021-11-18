# Voltdb-client-rust

## Overview

Voltdb-client-rust is a socket client library for [voltdb] written in rust

Here list the data type and rust data type mapping 

| SQL Datatype 	| Compatible Rust Datatypes 	| Option Supported 	|
|---	|---	|---	|
| TINYINT 	| bool 	|  ✓	|
| SMALLINT 	| i8/u8 	|  ✓	|
| INTEGER 	| i16/u16 	|  ✓	|
| BIGINT 	| i32/u32 	|  ✓	|
| FLOAT 	| f64 	|  ✓	|
| DECIMAL 	| bigdecimal::BigDecimal 	|  ✓	|
| GEOGRAPHY 	| - 	|  	|
| GEOGRAPHY_POINT 	| - 	|  	|
| VARCHAR 	| String 	| ✓ 	|
| VARBINARY 	| Vec< u8> 	|  ✓	|
| TIMESTAMP 	| chrono::DateTime 	|  ✓	|


[voltdb]: https://github.com/VoltDB/voltdb
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg

## Example


```toml
[dependencies]
voltdb-client-rust = { version = "0.1.0"}
```
Then, on your main.rs:

```rust,no_run
use voltdb_client_rust::node::*;
use std::fmt::Error;
use voltdb_client_rust::encode::VoltError;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};

fn main() -> Result<(), VoltError> {
    #[derive(Debug)]
    struct Test {
        t1: Option<bool>,
        t2: Option<i8>,
        t3: Option<i16>,
        t4: Option<i32>,
        t5: Option<f64>,
        t6: Option<BigDecimal>,
        t7: Option<String>,
        t8: Option<Vec<u8>>,
        t9: Option<DateTime<Utc>>,
    }

    let mut node = get_node("localhost:21211")?;

    let has_table_check = block_for_result(&node.query("select * from test_types")?);
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
    let mut table = block_for_result(&node.query("select * from test_types")?)?;
    while table.advance_row() {
        let t1 = table.get_bool_by_column("T1")?;
        let t2 = table.get_i8_by_column("t2")?;
        let t3 = table.get_i16_by_column("t3")?;
        let t4 = table.get_i32_by_column("t4")?;
        let t5 = table.get_f64_by_column("t5")?;
        let t6 = table.get_decimal_by_column("t6")?;
        let t7 = table.get_string_by_column("t7")?;
        let t8 = table.get_bytes_op_by_column("t8")?;
        let t9 = table.get_time_by_column("t9")?;
        println!("{:?}", Test {
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            t7,
            t8,
            t9,
        });
    }
    let insert_value = "insert into test_types (T1,T2,T3,T4,T5,T6,T7,T8,T9) values (1,2,3,4,5,6,'7','089CD7B35220FFB686012A0B08B49ECD8C06109893971F422F4D4F4E49544F52494E475F33393766643034662D656161642D346230372D613638302D62663562633736666132363148D535A8019CD7B352B001DDEE8501B801BAEE8501C001AAE98601CA01054341534831D0010AE00102E80102F20103555344FA010A0A0355534410809BEE028202050A035553448A020B08B49ECD8C06109893971F9202046E756C6CA2020A0A0355534410C0BD9A2FBA0219312C323139313936382C323139333231302C32313933323435C802C91E8A0400920400D80401880505B20500',NOW());";
    block_for_result(&node.query(insert_value)?)?;
    let mut table = block_for_result(&node.query("select * from test_types")?)?;
    while table.advance_row() {
        let t1 = table.get_bool_by_column("t1")?;
        let t2 = table.get_i8_by_column("t2")?;
        let t3 = table.get_i16_by_column("t3")?;
        let t4 = table.get_i32_by_column("t4")?;
        let t5 = table.get_f64_by_column("t5")?;
        let t6 = table.get_decimal_by_column("t6")?;
        let t7 = table.get_string_by_column("t7")?;
        let t8 = table.get_bytes_op_by_column("t8")?;
        let t9 = table.get_time_by_column("t9")?;
        println!("{:?}", Test {
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            t7,
            t8,
            t9,
        });
    }
    Ok({})
}

```

More examples can be found [here][examples].


[examples]: https://github.com/johnnywale/voltdb-client-rust/blob/master/tests/integration_test.rs



## License

This project is licensed under the [MIT license].

[MIT license]: https://github.com/johnnywale/voltdb-client-rust/blob/master/LICENSE
