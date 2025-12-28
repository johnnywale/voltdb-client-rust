use std::fmt::Debug;
use std::str::Utf8Error;
use std::sync::PoisonError;

use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use bytebuffer::ByteBuffer;
use chrono::{DateTime, Utc};
use quick_error::quick_error;

use crate::chrono::TimeZone;
use crate::response::VoltResponseInfo;
use crate::Column;

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
pub const NULL_SHORT_VALUE: [u8; 2] = [128, 0];
pub const NULL_INT_VALUE: [u8; 4] = [128, 0, 0, 0];
pub const NULL_LONG_VALUE: [u8; 8] = [128, 0, 0, 0, 0, 0, 0, 0];
pub const NULL_TIMESTAMP: [u8; 8] = [128, 0, 0, 0, 0, 0, 0, 0];

pub const NULL_FLOAT_VALUE: [u8; 8] = [255, 239, 255, 255, 255, 255, 255, 255];
pub const NULL_VARCHAR: [u8; 4] = [255, 255, 255, 255];

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
        AuthFailed {
             display("Auth failed")
        }
        ConnectionNotAvailable {
             display("Connection lost")
        }
        InvalidConfig {
             display("Invalid Config")
        }
        Timeout {
             display("Operation timeout")
        }
        /// Returned when a non-Option type encounters a NULL value.
        /// Use Option<T> if the column may contain NULL values.
        UnexpectedNull(column: String) {
            display("Unexpected NULL value in column '{}'. Use Option<T> for nullable columns.", column)
        }
}}

impl<T> From<PoisonError<T>> for VoltError {
    fn from(p: PoisonError<T>) -> VoltError {
        VoltError::PoisonError(p.to_string().to_owned())
    }
}

pub trait ValuePrimary {}

//pub trait
pub trait Value: Debug {
    fn get_write_length(&self) -> i32;
    fn marshal(&self, bytebuffer: &mut ByteBuffer);
    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8);
    fn to_value_string(&self) -> String;
    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError>
    where
        Self: Sized;
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_bool(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs[0] == 0 {
            return Ok(false);
        }
        return Ok(true);
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
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

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError>
    where
        Self: Sized,
    {
        if bs == NULL_DECIMAL {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let int = BigInt::from_signed_bytes_be(&*bs);
        let decimal = BigDecimal::new(int, 12);
        Ok(decimal)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_i8(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i8()?;
        Ok(value)
    }
}

impl Value for u8 {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        bytebuffer.write_u8(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_u8(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u8()?;
        Ok(value)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_i16(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i16()?;
        Ok(value)
    }
}

impl Value for u16 {
    fn get_write_length(&self) -> i32 {
        return 3;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        bytebuffer.write_u16(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_u16(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u16()?;
        Ok(value)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_i32(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i32()?;
        Ok(value)
    }
}

impl Value for u32 {
    fn get_write_length(&self) -> i32 {
        return 5;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        bytebuffer.write_u32(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_u32(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u32()?;
        Ok(value)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_i64(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i64()?;
        Ok(value)
    }
}

impl Value for u64 {
    fn get_write_length(&self) -> i32 {
        return 9;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        bytebuffer.write_u64(*self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_u64(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u64()?;
        Ok(value)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_f64(*self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_FLOAT_VALUE {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_f64()?;
        Ok(value)
    }
}

impl Value for String {
    fn get_write_length(&self) -> i32 {
        return (5 + self.len()) as i32;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        bytebuffer.write_string(self);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_string(self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, table_column: &Column) -> Result<Self, VoltError> {
        match table_column.header_type {
            STRING_COLUMN => {
                if bs == NULL_VARCHAR {
                    return Err(VoltError::UnexpectedNull(table_column.header_name.clone()));
                }
                let mut buffer = ByteBuffer::from_bytes(&bs);
                Ok(buffer.read_string()?)
            }
            _ => {
                let res = crate::table::VoltTable::get_value_by_idx_column(table_column, bs)?;
                match res {
                    Some(v) => Ok(v.to_value_string()),
                    None => Err(VoltError::UnexpectedNull(table_column.header_name.clone())),
                }
            }
        }
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_string(self);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(_bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        todo!()
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_u32(self.len() as u32);
        bytebuffer.write_bytes(&self);
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_VARCHAR {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut cp = bs.clone();
        cp.drain(0..4);
        Ok(cp)
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

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        bytebuffer.write_i64(self.timestamp_millis() * 1000);
    }

    fn to_value_string(&self) -> String {
        return self.to_string();
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError>
    where
        Self: Sized,
    {
        if bs == NULL_TIMESTAMP {
            return Err(VoltError::UnexpectedNull(_column.header_name.clone()));
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let time = buffer.read_i64()?;
        Ok(Utc.timestamp_millis_opt(time / 1000).unwrap())
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
        assert_eq!(
            bs,
            vec!(
                0, 0, 0, 56, 0, 0, 0, 0, 6, 64, 65, 100, 72, 111, 99, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1,
                9, 0, 0, 0, 30, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32,
                97, 99, 99, 111, 117, 110, 116, 32, 108, 105, 109, 105, 116, 32, 49, 59
            )
        );
    }

    #[test]
    fn test_time_stamp() {
        let time = Utc.timestamp_millis_opt(1637323002445000 / 1000).unwrap();
        println!("{}", time.timestamp_millis() * 1000);
    }

    #[test]
    fn test_big_decimal() {
        let bs: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 243, 16, 122, 64, 0];
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
        let i = ByteBuffer::from_bytes(&NULL_BIT_VALUE).read_i8().unwrap();
        assert_eq!(i, -128);
        let i = ByteBuffer::from_bytes(&NULL_SHORT_VALUE)
            .read_i16()
            .unwrap();
        assert_eq!(i, -32768);
        let i = ByteBuffer::from_bytes(&NULL_INT_VALUE).read_i32().unwrap();
        assert_eq!(i, -2147483648);
        let i = ByteBuffer::from_bytes(&NULL_LONG_VALUE).read_i64().unwrap();
        assert_eq!(i, -9223372036854775808);

        let column = Column {
            header_name: "".to_string(),
            header_type: STRING_COLUMN,
        };

        let vec = NULL_BIT_VALUE.to_vec();
        let op: Option<i8> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_BIT_VALUE.to_vec();
        let op: Option<u8> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_SHORT_VALUE.to_vec();
        let op: Option<i16> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_SHORT_VALUE.to_vec();
        let op: Option<u16> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_INT_VALUE.to_vec();
        let op: Option<i32> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_INT_VALUE.to_vec();
        let op: Option<u32> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_LONG_VALUE.to_vec();
        let op: Option<i64> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
        let vec = NULL_LONG_VALUE.to_vec();
        let op: Option<u64> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_VARCHAR.to_vec();
        let op: Option<String> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_VARCHAR.to_vec();
        let op: Option<Vec<u8>> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_FLOAT_VALUE.to_vec();
        let op: Option<f64> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_DECIMAL.to_vec();
        let op: Option<BigDecimal> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);

        let vec = NULL_TIMESTAMP.to_vec();
        let op: Option<DateTime<Utc>> = Option::from_bytes(vec, &column).unwrap();
        assert_eq!(None, op);
    }

    #[test]
    fn test_error() {
        let err = VoltError::NoValue("key is af".to_owned());
        println!("{:?} {} ", err, err);
    }

    #[test]
    fn test_non_option_null_returns_error_i8() {
        let column = Column {
            header_name: "test_col".to_string(),
            header_type: TINYINT_COLUMN,
        };
        let result = i8::from_bytes(NULL_BIT_VALUE.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "test_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_i16() {
        let column = Column {
            header_name: "short_col".to_string(),
            header_type: SHORT_COLUMN,
        };
        let result = i16::from_bytes(NULL_SHORT_VALUE.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "short_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_i32() {
        let column = Column {
            header_name: "int_col".to_string(),
            header_type: INT_COLUMN,
        };
        let result = i32::from_bytes(NULL_INT_VALUE.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "int_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_i64() {
        let column = Column {
            header_name: "long_col".to_string(),
            header_type: LONG_COLUMN,
        };
        let result = i64::from_bytes(NULL_LONG_VALUE.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "long_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_f64() {
        let column = Column {
            header_name: "float_col".to_string(),
            header_type: FLOAT_COLUMN,
        };
        let result = f64::from_bytes(NULL_FLOAT_VALUE.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "float_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_string() {
        let column = Column {
            header_name: "str_col".to_string(),
            header_type: STRING_COLUMN,
        };
        let result = String::from_bytes(NULL_VARCHAR.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "str_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_vec_u8() {
        let column = Column {
            header_name: "bin_col".to_string(),
            header_type: VAR_BIN_COLUMN,
        };
        let result = Vec::<u8>::from_bytes(NULL_VARCHAR.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "bin_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_datetime() {
        let column = Column {
            header_name: "time_col".to_string(),
            header_type: TIMESTAMP_COLUMN,
        };
        let result = DateTime::<Utc>::from_bytes(NULL_TIMESTAMP.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "time_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_option_null_returns_error_decimal() {
        let column = Column {
            header_name: "dec_col".to_string(),
            header_type: DECIMAL_COLUMN,
        };
        let result = BigDecimal::from_bytes(NULL_DECIMAL.to_vec(), &column);
        assert!(result.is_err());
        match result {
            Err(VoltError::UnexpectedNull(col)) => assert_eq!(col, "dec_col"),
            _ => panic!("Expected UnexpectedNull error"),
        }
    }

    #[test]
    fn test_non_null_values_work() {
        let column = Column {
            header_name: "col".to_string(),
            header_type: INT_COLUMN,
        };
        // Non-NULL value (42 as big-endian i32)
        let result = i32::from_bytes(vec![0, 0, 0, 42], &column);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_unexpected_null_error_message() {
        let err = VoltError::UnexpectedNull("my_column".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("my_column"));
        assert!(msg.contains("Option<T>"));
    }

    // Marshal tests for basic types
    #[test]
    fn test_i8_marshal() {
        let val: i8 = 42;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
        assert_eq!(bytes[1], 42);
    }

    #[test]
    fn test_i8_get_write_length() {
        let val: i8 = 1;
        assert_eq!(val.get_write_length(), 2);
    }

    #[test]
    fn test_u8_marshal() {
        let val: u8 = 255;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
        assert_eq!(bytes[1], 255);
    }

    #[test]
    fn test_i16_marshal() {
        let val: i16 = 1000;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, SHORT_COLUMN);
        assert_eq!(val.get_write_length(), 3);
    }

    #[test]
    fn test_u16_marshal() {
        let val: u16 = 65535;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, SHORT_COLUMN);
    }

    #[test]
    fn test_i32_marshal() {
        let val: i32 = 123456;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, INT_COLUMN);
        assert_eq!(val.get_write_length(), 5);
    }

    #[test]
    fn test_u32_marshal() {
        let val: u32 = 4294967295;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, INT_COLUMN);
    }

    #[test]
    fn test_i64_marshal() {
        let val: i64 = 9876543210;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, LONG_COLUMN);
        assert_eq!(val.get_write_length(), 9);
    }

    #[test]
    fn test_u64_marshal() {
        let val: u64 = 18446744073709551615;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, LONG_COLUMN);
    }

    #[test]
    fn test_f64_marshal() {
        let val: f64 = 3.14159;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, FLOAT_COLUMN);
        assert_eq!(val.get_write_length(), 9);
    }

    #[test]
    fn test_bool_marshal_true() {
        let val: bool = true;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
        assert_eq!(bytes[1], 1);
    }

    #[test]
    fn test_bool_marshal_false() {
        let val: bool = false;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
        assert_eq!(bytes[1], 0);
    }

    #[test]
    fn test_string_marshal() {
        let val: String = "hello".to_string();
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
        assert_eq!(val.get_write_length(), 5 + 5); // header + "hello"
    }

    #[test]
    fn test_str_marshal() {
        let val: &str = "world";
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
        assert_eq!(val.get_write_length(), 5 + 5); // header + "world"
    }

    #[test]
    fn test_vec_u8_marshal() {
        let val: Vec<u8> = vec![1, 2, 3, 4, 5];
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, VAR_BIN_COLUMN);
        assert_eq!(val.get_write_length(), 5 + 5); // header + 5 bytes
    }

    #[test]
    fn test_datetime_marshal() {
        let val = Utc.timestamp_millis_opt(1000000).unwrap();
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TIMESTAMP_COLUMN);
        assert_eq!(val.get_write_length(), 9);
    }

    #[test]
    fn test_bigdecimal_marshal() {
        let val = BigDecimal::from_str("123.456789").unwrap();
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, DECIMAL_COLUMN);
        assert_eq!(val.get_write_length(), 17);
    }

    // to_value_string tests
    #[test]
    fn test_i32_to_value_string() {
        let val: i32 = 42;
        assert_eq!(val.to_value_string(), "42");
    }

    #[test]
    fn test_f64_to_value_string() {
        let val: f64 = 3.14;
        assert!(val.to_value_string().starts_with("3.14"));
    }

    #[test]
    fn test_bool_to_value_string() {
        assert_eq!(true.to_value_string(), "true");
        assert_eq!(false.to_value_string(), "false");
    }

    #[test]
    fn test_string_to_value_string() {
        let val = "hello".to_string();
        assert_eq!(val.to_value_string(), "hello");
    }

    // Round-trip tests
    #[test]
    fn test_i32_roundtrip() {
        let col = Column {
            header_name: "test".to_string(),
            header_type: INT_COLUMN,
        };
        let original: i32 = 12345;
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, INT_COLUMN);
        let bytes = buf.into_vec();
        let result = i32::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_i64_roundtrip() {
        let col = Column {
            header_name: "test".to_string(),
            header_type: LONG_COLUMN,
        };
        let original: i64 = 9876543210;
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, LONG_COLUMN);
        let bytes = buf.into_vec();
        let result = i64::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_f64_roundtrip() {
        let col = Column {
            header_name: "test".to_string(),
            header_type: FLOAT_COLUMN,
        };
        let original: f64 = 3.14159;
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, FLOAT_COLUMN);
        let bytes = buf.into_vec();
        let result = f64::from_bytes(bytes, &col).unwrap();
        assert!((result - original).abs() < 0.00001);
    }
}
