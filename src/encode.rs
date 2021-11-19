use std::fmt::Debug;
use std::str::Utf8Error;
use std::sync::PoisonError;

use bigdecimal::BigDecimal;
use bytebuffer::ByteBuffer;
use chrono::{DateTime, Utc};
use quick_error::quick_error;

use crate::response::VoltResponseInfo;

#[allow(dead_code)]
pub const ARRAY_COLUMN: i8 = -99;
pub const NULL_COLUMN: i8 = 1;
pub const TINYINT_COLUMN: i8 = 3;
pub const SHORT_COLUMN: i8 = 4;
pub const INT_COLUMN: i8 = 5;
pub const LONG_COLUMN: i8 = 6;
pub const FLOAT_COLUMN: i8 = 8;
pub const STRING_COLUMN: i8 = 9;
pub const TIMESTAMP_COLUMN: i8 = 11;
pub const TABLE: i8 = 21;
pub const DECIMAL_COLUMN: i8 = 22;
pub const VAR_BIN_COLUMN: i8 = 25; // varbinary (int)(bytes)


pub const NULL_DECIMAL: [u8; 16] = [128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
pub const NULL_BIT_VALUE: [u8; 1] = [128];
pub const NULL_BYTE_VALUE: [u8; 2] = [128, 0];
pub const NULL_SHORT_VALUE: [u8; 4] = [128, 0, 0, 0];
pub const NULL_INT_VALUE: [u8; 8] = [128, 0, 0, 0, 0, 0, 0, 0];
pub const NULL_LONG_VALUE: [u8; 16] = [128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
pub const NULL_TIMESTAMP: [u8; 8] = [128, 0, 0, 0, 0, 0, 0, 0];


pub const NULL_FLOAT_VALUE: [u8; 8] = [255, 239, 255, 255, 255, 255, 255, 255];
pub const NULL_VARCHAR: [u8; 4] = [255, 255, 255, 255];

#[macro_export]
macro_rules! volt_param {
    () => (
        std::vec::Vec::new()
    );
      ( $( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(&$x as & dyn Value);
            )*
            temp_vec
        }
    };
}
quick_error! {
#[derive(Debug)]
pub enum VoltError {
        Io(err: std::io::Error) {
            from()
            display("I/O error: {}", err)
            source(err)
        }

        RecvError(err: std::sync::mpsc::RecvError){
            from()
            display("Recv error: {}", err)
            source(err)
        }

        ExecuteFail ( info: VoltResponseInfo ){
            display("volt execute failed: {:?}", info)
        }
        InvalidColumnType(tp: i8) {
            display("InvalidColumnType {}", tp)
        }

        NoValue (descr : String) {
            display("Error {}", descr)
        }
        Other(descr: String) {
            display("Error {}", descr)
        }
        NegativeNumTables (num: i16) {
             display("Error {}", num)
        }

         Utf8Error(err : Utf8Error){
            from()
            display("Utf8 error: {}", err)
            source(err)
        }

        PoisonError (descr: String){
              display("Error {}", descr)
        }
        BadReturnStatusOnTable (status: i8) {
             display("Error {}", status)
        }
        AuthFailed


}}

impl<T> From<PoisonError<T>> for VoltError {
    fn from(p: PoisonError<T>) -> VoltError {
        VoltError::PoisonError(p.to_string().to_owned())
    }
}

//pub trait
pub trait Value: Debug {
    fn get_write_length(&self) -> i32;
    fn marshal(&self, bytebuffer: &mut ByteBuffer);
    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8);
    fn to_value_string(&self) -> String;
}


trait WriteBool {
    fn write_bool(&mut self, val: bool);
}

impl WriteBool for ByteBuffer {
    fn write_bool(&mut self, val: bool) {
        if val {
            self.write_i8(1)
        } else {
            self.write_i8(0);
        }
    }
}


impl Value for bool {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        bytebuffer.write_bool(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_bool(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}


impl Value for BigDecimal {
    fn get_write_length(&self) -> i32 {
        return 17;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(DECIMAL_COLUMN);
        self.marshal_in_table(bytebuffer, DECIMAL_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        let (b, _) = self.clone().with_scale(12).into_bigint_and_exponent();
        let bs = b.to_signed_bytes_be();
        let pad = 16 - bs.len();
        if pad > 0 {
            let arr = vec![0; pad];
            bytebuffer.write_bytes(&arr)
        }
        bytebuffer.write_bytes(&bs);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for i8 {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        bytebuffer.write_i8(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_i8(0);
        bytebuffer.write_i8(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for i16 {
    fn get_write_length(&self) -> i32 {
        return 3;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        bytebuffer.write_i16(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_i16(0);
        bytebuffer.write_i16(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for i32 {
    fn get_write_length(&self) -> i32 {
        return 5;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        bytebuffer.write_i32(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_i32(0);
        bytebuffer.write_i32(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for i64 {
    fn get_write_length(&self) -> i32 {
        return 9;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        bytebuffer.write_i64(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_i64(0);
        bytebuffer.write_i64(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for f64 {
    fn get_write_length(&self) -> i32 {
        return 9;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(FLOAT_COLUMN);
        bytebuffer.write_f64(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_f64(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}


impl Value for String {
    fn get_write_length(&self) -> i32 {
        return (5 + self.len()) as i32;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        // write length , then data
        bytebuffer.write_string(self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_string(self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

impl Value for &str {
    fn get_write_length(&self) -> i32 {
        return (5 + self.len()) as i32;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        // write length , then data
        bytebuffer.write_string(self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_string(self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}


impl Value for Vec<u8> {
    fn get_write_length(&self) -> i32 {
        return (5 + self.len()) as i32;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(VAR_BIN_COLUMN);
        bytebuffer.write_u32(self.len() as u32);
        bytebuffer.write_bytes(&self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_u32(self.len() as u32);
        bytebuffer.write_bytes(&self);
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }
}

impl Value for [u8] {
    fn get_write_length(&self) -> i32 {
        return (5 + self.len()) as i32;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(VAR_BIN_COLUMN);
        bytebuffer.write_u32(self.len() as u32);
        bytebuffer.write_bytes(&self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_u32(self.len() as u32);
        bytebuffer.write_bytes(&self);
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }
}

impl Value for DateTime<Utc> {
    fn get_write_length(&self) -> i32 {
        return 9;
    }
    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TIMESTAMP_COLUMN);
        bytebuffer.write_i64(self.timestamp_millis() * 1000);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, column_type: i8) {
        bytebuffer.write_i64(self.timestamp_millis() * 1000);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::num_bigint::BigInt;
    use chrono::TimeZone;

    use crate::procedure_invocation::new_procedure_invocation;

    use super::*;

    #[test]
    fn test_encoding_proc() {
        let mut zero_vec: Vec<&dyn Value> = Vec::new();
        zero_vec.push(&"select * from account limit 1;");

        let mut proc = new_procedure_invocation(1, false, &zero_vec, "@AdHoc");
        let bs = proc.bytes();
        assert_eq!(bs, vec!(0, 0, 0, 56, 0, 0, 0, 0, 6, 64, 65, 100, 72, 111, 99, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 9, 0, 0, 0, 30, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 97, 99, 99, 111, 117, 110, 116, 32, 108, 105, 109, 105, 116, 32, 49, 59));
    }

    #[test]
    fn test_time_stamp() {
        let time = Utc.timestamp_millis(1637323002445000 / 1000);
        println!("{}", time.timestamp_millis() * 1000);
    }

    #[test]
    fn test_big_decimal() {
        let bs: Vec<u8> = vec!(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 243, 16, 122, 64, 0);
        let int = BigInt::from_signed_bytes_be(&*bs);
        let decimal = BigDecimal::new(int, 12);
        let (b, _) = decimal.into_bigint_and_exponent();
        let b = b.to_signed_bytes_be();
        println!("{:?}", b);

        let decimal = BigDecimal::from_str("1.11").unwrap().with_scale(12);
        println!("{:?}", decimal.into_bigint_and_exponent());
    }

    #[test]
    fn test_big_test_bytes() {
        let i = ByteBuffer::from_bytes(&NULL_BYTE_VALUE).read_i8().unwrap();
        assert_eq!(i, -128);
        let i = ByteBuffer::from_bytes(&NULL_SHORT_VALUE).read_i16().unwrap();
        assert_eq!(i, -32768);
        let i = ByteBuffer::from_bytes(&NULL_INT_VALUE).read_i32().unwrap();
        assert_eq!(i, -2147483648);
        let i = ByteBuffer::from_bytes(&NULL_LONG_VALUE).read_i64().unwrap();
        assert_eq!(i, -9223372036854775808);


        let mut byte = ByteBuffer::new();
        byte.write_i32(-1);
        let bs = byte.to_bytes();
        println!("{:?}", bs)
    }

    #[test]
    fn test_error() {
        let err = VoltError::NoValue("key is af".to_owned());
        println!("{:?} {} ", err, err);
    }

    #[test]
    fn test_vec() {
        let xs: [u8; 5] = [1, 2, 3, 4, 5];
        println!("{}", xs.get_write_length());
        let mut ve = Vec::new();
        ve.push(1 as u8);
        ve.push(10 as u8);
        println!("{}", &ve.get_write_length());
    }
}
