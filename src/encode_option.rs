use bigdecimal::BigDecimal;
use bytebuffer::ByteBuffer;
use chrono::{DateTime, Utc};

use crate::*;

impl Value for Option<bool> {
    fn get_write_length(&self) -> i32 {
        2
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_BIT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, TINYINT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(bool::from_bytes(bs, _column)?))
    }
}

impl Value for Option<BigDecimal> {
    fn get_write_length(&self) -> i32 {
        17
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(DECIMAL_COLUMN);
        self.marshal_in_table(bytebuffer, DECIMAL_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_DECIMAL),
            Some(v) => v.marshal_in_table(bytebuffer, DECIMAL_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError>
    where
        Self: Sized,
    {
        if bs == NULL_DECIMAL {
            return Ok(Option::None);
        }
        Ok(Some(BigDecimal::from_bytes(bs, _column)?))
    }
}

impl Value for Option<i8> {
    fn get_write_length(&self) -> i32 {
        2
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_BIT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, TINYINT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(i8::from_bytes(bs, _column)?))
    }
}

impl Value for Option<u8> {
    fn get_write_length(&self) -> i32 {
        2
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_BIT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, TINYINT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(u8::from_bytes(bs, _column)?))
    }
}

impl Value for Option<i16> {
    fn get_write_length(&self) -> i32 {
        3
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        self.marshal_in_table(bytebuffer, SHORT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_SHORT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, SHORT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(i16::from_bytes(bs, _column)?))
    }
}

impl Value for Option<u16> {
    fn get_write_length(&self) -> i32 {
        3
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        self.marshal_in_table(bytebuffer, SHORT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_SHORT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, SHORT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(u16::from_bytes(bs, _column)?))
    }
}

impl Value for Option<i32> {
    fn get_write_length(&self) -> i32 {
        5
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        self.marshal_in_table(bytebuffer, INT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_INT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, INT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(i32::from_bytes(bs, _column)?))
    }
}

impl Value for Option<u32> {
    fn get_write_length(&self) -> i32 {
        5
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        self.marshal_in_table(bytebuffer, INT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_INT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, INT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(u32::from_bytes(bs, _column)?))
    }
}

impl Value for Option<i64> {
    fn get_write_length(&self) -> i32 {
        9
    }
    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        self.marshal_in_table(bytebuffer, LONG_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_LONG_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, LONG_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(i64::from_bytes(bs, _column)?))
    }
}

impl Value for Option<u64> {
    fn get_write_length(&self) -> i32 {
        9
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        self.marshal_in_table(bytebuffer, LONG_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_LONG_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, LONG_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(u64::from_bytes(bs, _column)?))
    }
}

impl Value for Option<f64> {
    fn get_write_length(&self) -> i32 {
        9
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(FLOAT_COLUMN);
        self.marshal_in_table(bytebuffer, FLOAT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_FLOAT_VALUE),
            Some(v) => v.marshal_in_table(bytebuffer, FLOAT_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_FLOAT_VALUE {
            return Ok(Option::None);
        }
        Ok(Some(f64::from_bytes(bs, _column)?))
    }
}

impl Value for Option<String> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => 5,
            Some(v) => (5 + v.len()) as i32,
        }
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => v.marshal_in_table(bytebuffer, STRING_COLUMN),
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => v.marshal_in_table(bytebuffer, STRING_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, table_column: &Column) -> Result<Self, VoltError> {
        match table_column.header_type {
            STRING_COLUMN => {
                if bs == NULL_VARCHAR {
                    return Ok(Option::None);
                }
                let mut buffer = ByteBuffer::from_bytes(&bs);
                Ok(Option::Some(buffer.read_string()?))
            }
            _ => {
                let res = crate::table::VoltTable::get_value_by_idx_column(table_column, bs)?;
                match res {
                    Some(v) => Ok(Option::Some(v.to_value_string())),
                    None => Ok(Option::None),
                }
            }
        }
    }
}

impl Value for Option<&str> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => 5,
            Some(v) => (5 + v.len()) as i32,
        }
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => v.marshal_in_table(bytebuffer, STRING_COLUMN),
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => v.marshal_in_table(bytebuffer, STRING_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(_bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        todo!()
    }
}

impl Value for Option<Vec<u8>> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => 5,
            Some(v) => (5 + v.len()) as i32,
        }
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(VAR_BIN_COLUMN);
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => {
                bytebuffer.write_bytes(v);
            }
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_VARCHAR),
            Some(v) => {
                bytebuffer.write_bytes(v);
            }
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_VARCHAR {
            let res: Option<Vec<u8>> = Option::None;
            return Ok(res);
        }
        let mut bs = bs;
        bs.drain(0..4);
        Ok(Option::Some(bs))
    }
}

impl Value for Option<DateTime<Utc>> {
    fn get_write_length(&self) -> i32 {
        9
    }
    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TIMESTAMP_COLUMN);
        match self {
            None => bytebuffer.write_bytes(&NULL_TIMESTAMP),
            Some(v) => v.marshal_in_table(bytebuffer, TIMESTAMP_COLUMN),
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => bytebuffer.write_bytes(&NULL_TIMESTAMP),
            Some(v) => v.marshal_in_table(bytebuffer, TIMESTAMP_COLUMN),
        }
    }

    fn to_value_string(&self) -> String {
        format!("{:?}", self)
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError>
    where
        Self: Sized,
    {
        if bs == NULL_TIMESTAMP {
            return Ok(Option::None);
        }
        Ok(Option::Some(DateTime::from_bytes(bs, _column)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    fn make_column(name: &str, col_type: i8) -> Column {
        Column {
            header_name: name.to_string(),
            header_type: col_type,
        }
    }

    // Option<bool> tests
    #[test]
    fn test_option_bool_marshal_none() {
        let val: Option<bool> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, TINYINT_COLUMN);
    }

    #[test]
    fn test_option_bool_marshal_some() {
        let val: Option<bool> = Some(true);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
        assert_eq!(bytes[1], 1);
    }

    #[test]
    fn test_option_bool_to_value_string() {
        let val: Option<bool> = Some(true);
        assert!(val.to_value_string().contains("true"));
        let val: Option<bool> = None;
        assert!(val.to_value_string().contains("None"));
    }

    // Option<i8> tests
    #[test]
    fn test_option_i8_marshal_none() {
        let val: Option<i8> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
    }

    #[test]
    fn test_option_i8_marshal_some() {
        let val: Option<i8> = Some(42);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TINYINT_COLUMN);
    }

    #[test]
    fn test_option_i8_get_write_length() {
        let val: Option<i8> = Some(1);
        assert_eq!(val.get_write_length(), 2);
    }

    // Option<i16> tests
    #[test]
    fn test_option_i16_marshal_none() {
        let val: Option<i16> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, SHORT_COLUMN);
    }

    #[test]
    fn test_option_i16_marshal_some() {
        let val: Option<i16> = Some(1000);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, SHORT_COLUMN);
    }

    // Option<i32> tests
    #[test]
    fn test_option_i32_marshal_none() {
        let val: Option<i32> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, INT_COLUMN);
    }

    #[test]
    fn test_option_i32_marshal_some() {
        let val: Option<i32> = Some(123456);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, INT_COLUMN);
    }

    // Option<i64> tests
    #[test]
    fn test_option_i64_marshal_none() {
        let val: Option<i64> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, LONG_COLUMN);
    }

    #[test]
    fn test_option_i64_marshal_some() {
        let val: Option<i64> = Some(9876543210);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, LONG_COLUMN);
    }

    // Option<f64> tests
    #[test]
    fn test_option_f64_marshal_none() {
        let val: Option<f64> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, FLOAT_COLUMN);
    }

    #[test]
    fn test_option_f64_marshal_some() {
        #[allow(clippy::approx_constant)]
        let val: Option<f64> = Some(3.14159);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, FLOAT_COLUMN);
    }

    // Option<String> tests
    #[test]
    fn test_option_string_marshal_none() {
        let val: Option<String> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
    }

    #[test]
    fn test_option_string_marshal_some() {
        let val: Option<String> = Some("hello".to_string());
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
    }

    #[test]
    fn test_option_string_get_write_length() {
        let val: Option<String> = None;
        assert_eq!(val.get_write_length(), 5);
        let val: Option<String> = Some("test".to_string());
        assert_eq!(val.get_write_length(), 5 + 4);
    }

    // Option<Vec<u8>> tests
    #[test]
    fn test_option_vec_u8_marshal_none() {
        let val: Option<Vec<u8>> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, VAR_BIN_COLUMN);
    }

    #[test]
    fn test_option_vec_u8_marshal_some() {
        let val: Option<Vec<u8>> = Some(vec![1, 2, 3, 4]);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, VAR_BIN_COLUMN);
    }

    // Option<BigDecimal> tests
    #[test]
    fn test_option_decimal_marshal_none() {
        let val: Option<BigDecimal> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, DECIMAL_COLUMN);
    }

    #[test]
    fn test_option_decimal_marshal_some() {
        let val: Option<BigDecimal> = Some(BigDecimal::from_str("123.45").unwrap());
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, DECIMAL_COLUMN);
    }

    // Option<DateTime<Utc>> tests
    #[test]
    fn test_option_datetime_marshal_none() {
        let val: Option<DateTime<Utc>> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TIMESTAMP_COLUMN);
    }

    #[test]
    fn test_option_datetime_marshal_some() {
        let val: Option<DateTime<Utc>> = Some(Utc.timestamp_millis_opt(1000000).unwrap());
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, TIMESTAMP_COLUMN);
    }

    // Round-trip tests
    #[test]
    fn test_option_i32_roundtrip_some() {
        let col = make_column("test", INT_COLUMN);
        let original: Option<i32> = Some(42);
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, INT_COLUMN);
        let bytes = buf.into_vec();
        let result: Option<i32> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_option_i32_roundtrip_none() {
        let col = make_column("test", INT_COLUMN);
        let original: Option<i32> = None;
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, INT_COLUMN);
        let bytes = buf.into_vec();
        let result: Option<i32> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_option_string_roundtrip_some() {
        let col = make_column("test", STRING_COLUMN);
        let original: Option<String> = Some("hello world".to_string());
        let mut buf = ByteBuffer::new();
        original.marshal_in_table(&mut buf, STRING_COLUMN);
        let bytes = buf.into_vec();
        let result: Option<String> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, original);
    }

    // marshal_in_table tests
    #[test]
    fn test_option_i64_marshal_in_table_none() {
        let val: Option<i64> = None;
        let mut buf = ByteBuffer::new();
        val.marshal_in_table(&mut buf, LONG_COLUMN);
        assert_eq!(buf.into_vec(), NULL_LONG_VALUE.to_vec());
    }

    #[test]
    fn test_option_str_marshal() {
        let val: Option<&str> = Some("test");
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
    }

    #[test]
    fn test_option_str_marshal_none() {
        let val: Option<&str> = None;
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        let bytes = buf.into_vec();
        assert_eq!(bytes[0] as i8, STRING_COLUMN);
    }

    // Unsigned types tests
    #[test]
    fn test_option_u8_marshal() {
        let val: Option<u8> = Some(255);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, TINYINT_COLUMN);
    }

    #[test]
    fn test_option_u16_marshal() {
        let val: Option<u16> = Some(65535);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, SHORT_COLUMN);
    }

    #[test]
    fn test_option_u32_marshal() {
        let val: Option<u32> = Some(4294967295);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, INT_COLUMN);
    }

    #[test]
    fn test_option_u64_marshal() {
        let val: Option<u64> = Some(18446744073709551615);
        let mut buf = ByteBuffer::new();
        val.marshal(&mut buf);
        assert_eq!(buf.into_vec()[0] as i8, LONG_COLUMN);
    }
}
