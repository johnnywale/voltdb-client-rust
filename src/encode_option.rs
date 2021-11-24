use bigdecimal::BigDecimal;
use bytebuffer::ByteBuffer;
use chrono::{DateTime, Utc};

use crate::{*};

impl Value for Option<bool> {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_BIT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, TINYINT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(bool::from_bytes(bs, _column)?));
    }
}


impl Value for Option<BigDecimal> {
    fn get_write_length(&self) -> i32 {
        return 17;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(DECIMAL_COLUMN);
        self.marshal_in_table(bytebuffer, DECIMAL_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_DECIMAL)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, DECIMAL_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> where Self: Sized {
        if bs == NULL_DECIMAL {
            return Ok(Option::None);
        }
        Ok(Some(BigDecimal::from_bytes(bs, _column)?))
    }
}

impl Value for Option<i8> {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_BIT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, TINYINT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(i8::from_bytes(bs, _column)?));
    }
}

impl Value for Option<u8> {
    fn get_write_length(&self) -> i32 {
        return 2;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TINYINT_COLUMN);
        self.marshal_in_table(bytebuffer, TINYINT_COLUMN)
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_BIT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, TINYINT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(u8::from_bytes(bs, _column)?));
    }
}

impl Value for Option<i16> {
    fn get_write_length(&self) -> i32 {
        return 3;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        self.marshal_in_table(bytebuffer, SHORT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_SHORT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, SHORT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(i16::from_bytes(bs, _column)?));
    }
}

impl Value for Option<u16> {
    fn get_write_length(&self) -> i32 {
        return 3;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(SHORT_COLUMN);
        self.marshal_in_table(bytebuffer, SHORT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_SHORT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, SHORT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(u16::from_bytes(bs, _column)?));
    }
}

impl Value for Option<i32> {
    fn get_write_length(&self) -> i32 {
        return 5;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        self.marshal_in_table(bytebuffer, INT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_INT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, INT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(i32::from_bytes(bs, _column)?));
    }
}

impl Value for Option<u32> {
    fn get_write_length(&self) -> i32 {
        return 5;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(INT_COLUMN);
        self.marshal_in_table(bytebuffer, INT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_INT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, INT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(u32::from_bytes(bs, _column)?));
    }
}

impl Value for Option<i64> {
    fn get_write_length(&self) -> i32 {
        return 9;
    }
    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        self.marshal_in_table(bytebuffer, LONG_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_LONG_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, LONG_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(i64::from_bytes(bs, _column)?));
    }
}

impl Value for Option<u64> {
    fn get_write_length(&self) -> i32 {
        return 9;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(LONG_COLUMN);
        self.marshal_in_table(bytebuffer, LONG_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_LONG_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, LONG_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(u64::from_bytes(bs, _column)?));
    }
}

impl Value for Option<f64> {
    fn get_write_length(&self) -> i32 {
        return 9;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(FLOAT_COLUMN);
        self.marshal_in_table(bytebuffer, FLOAT_COLUMN);
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_FLOAT_VALUE)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, FLOAT_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_FLOAT_VALUE {
            return Ok(Option::None);
        }
        return Ok(Some(f64::from_bytes(bs, _column)?));
    }
}


impl Value for Option<String> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => {
                return 5;
            }
            Some(v) => {
                return (5 + v.len()) as i32;
            }
        }
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, STRING_COLUMN)
            }
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, STRING_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, table_column: &Column) -> Result<Self, VoltError> {
        return match table_column.header_type {
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
                    Some(v) => {
                        Ok(Option::Some(v.to_value_string()))
                    }
                    None => {
                        Ok(Option::None)
                    }
                }
            }
        };
    }
}

impl Value for Option<&str> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => {
                return 5;
            }
            Some(v) => {
                return (5 + v.len()) as i32;
            }
        }
    }


    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(STRING_COLUMN);
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, STRING_COLUMN)
            }
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, STRING_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(_bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        todo!()
    }
}

impl Value for Option<Vec<u8>> {
    fn get_write_length(&self) -> i32 {
        match self {
            None => {
                return 5;
            }
            Some(v) => {
                return (5 + v.len()) as i32;
            }
        }
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(VAR_BIN_COLUMN);
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                bytebuffer.write_bytes(&v);
            }
        }
    }

    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_VARCHAR)
            }
            Some(v) => {
                bytebuffer.write_bytes(&v);
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> {
        if bs == NULL_VARCHAR {
            let res: Option<Vec<u8>> = Option::None;
            return Ok(res);
        }
        let mut cp = bs.clone();
        cp.drain(0..4);
        return Ok(Option::Some(cp));
    }
}


impl Value for Option<DateTime<Utc>> {
    fn get_write_length(&self) -> i32 {
        return 9;
    }
    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TIMESTAMP_COLUMN);
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_TIMESTAMP)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, TIMESTAMP_COLUMN)
            }
        }
    }


    fn marshal_in_table(&self, bytebuffer: &mut ByteBuffer, _column_type: i8) {
        match self {
            None => {
                bytebuffer.write_bytes(&NULL_TIMESTAMP)
            }
            Some(v) => {
                v.marshal_in_table(bytebuffer, TIMESTAMP_COLUMN)
            }
        }
    }

    fn to_value_string(&self) -> String {
        return format!("{:?}", self);
    }

    fn from_bytes(bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> where Self: Sized {
        if bs == NULL_TIMESTAMP {
            return Ok(Option::None);
        }
        return Ok(Option::Some(DateTime::from_bytes(bs, _column)?));
    }
}
