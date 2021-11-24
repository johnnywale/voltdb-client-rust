use std::collections::HashMap;
use std::io::Read;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use bytebuffer::ByteBuffer;
use chrono::{DateTime, TimeZone, Utc};

use crate::encode::{*};
use crate::response::ResponseStatus::Success;
use crate::response::VoltResponseInfo;

const MIN_INT8: i8 = -1 << 7;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub header_name: String,
    pub header_type: i8,
}

#[derive(Debug)]
pub struct VoltTable {
    info: VoltResponseInfo,
    column_count: i16,
    info_bytes: ByteBuffer,
    column_info_bytes: ByteBuffer,
    columns: Vec<Column>,
    num_rows: i32,
    rows: Vec<ByteBuffer>,
    row_index: i32,
    cn_to_ci: HashMap<String, i16>,
    column_offsets: Vec<i32>,
    header_size: i32,
    total_size: i32,
}

impl Value for VoltTable {
    fn get_write_length(&self) -> i32 {
        return self.total_size + 5;
    }

    fn marshal(&self, bytebuffer: &mut ByteBuffer) {
        bytebuffer.write_i8(TABLE);
        bytebuffer.write_u32(self.total_size as u32);
        bytebuffer.write_u32(self.header_size as u32);
        bytebuffer.write_u8(128);
        bytebuffer.write_bytes(&*self.column_info_bytes.to_bytes());
        bytebuffer.write_u32(self.num_rows as u32);
        self.rows.iter().for_each(|f| {
            bytebuffer.write_u32(f.len() as u32);
            bytebuffer.write_bytes(&*f.to_bytes());
        });
        println!("{}", bytebuffer.len())
    }

    fn marshal_in_table(&self, _bytebuffer: &mut ByteBuffer, _column_type: i8) {
        //
    }

    fn to_value_string(&self) -> String {
        return "table".to_owned();
    }

    fn from_bytes(_bs: Vec<u8>, _column: &Column) -> Result<Self, VoltError> where Self: Sized {
        todo!()
    }
}


impl VoltTable {
    /// COLUMN HEADER:
    /// [int: column header size in bytes (non-inclusive)]
    /// [byte: table status code]
    /// [short: num columns]
    /// [byte: column type] * num columns
    /// [string: column name] * num columns
    /// TABLE BODY DATA:
    /// [int: num tuples]
    /// [int: row size (non inclusive), blob row data] * num tuples
    pub fn new_table(types: Vec<i8>, header: Vec<String>) -> Self {
        let mut columns = Vec::with_capacity(types.len());
        for (i, tp) in types.iter().enumerate() {
            columns.push(Column {
                header_name: header.get(i).unwrap().clone(),
                header_type: *tp,
            })
        }
        return crate::table::VoltTable::new_voltdb_table(columns);
    }

    pub fn new_voltdb_table(columns: Vec<Column>) -> Self {
        let mut column_info_bytes = ByteBuffer::new();

        let column_count = columns.len() as i16;
        column_info_bytes.write_i16(column_count);
        columns.iter().for_each(|f| column_info_bytes.write_i8(f.header_type));
        columns.iter().for_each(|f| column_info_bytes.write_string(f.header_name.as_str()));
        let header_size = (1 + column_info_bytes.len()) as i32;
        let total_size = header_size + 8;
        //
        return VoltTable {
            info: Default::default(),
            column_count,
            info_bytes: Default::default(),
            column_info_bytes,
            columns,
            num_rows: 0,
            rows: vec![],
            row_index: 0,
            cn_to_ci: Default::default(),
            column_offsets: vec![],
            header_size,
            total_size,
        };
    }

    pub fn add_row(&mut self, row: Vec<&dyn Value>) -> Result<i16, VoltError> {
        let mut bf: ByteBuffer = ByteBuffer::new();
        self.columns.iter().enumerate().for_each(|(f, v)| {
            let da = *row.get(f).unwrap();
            da.marshal_in_table(&mut bf, v.header_type);
        });
        let len = bf.len();
        self.rows.push(bf);
        self.num_rows = self.num_rows + 1;
        self.total_size = self.total_size + (len + 4) as i32;
        return Ok(1);
    }


    pub fn get_column_index(&mut self, column: &str) -> Result<i16, VoltError> {
        let idx = self.cn_to_ci.get(column.to_uppercase().as_str()).ok_or(VoltError::NoValue(column.to_owned()))?;
        return Ok(*idx);
    }

    pub fn map_row<'a, T: From<&'a mut VoltTable>>(&'a mut self) -> T {
        return T::from(self);
    }
    pub fn take<T: Value>(&mut self, column: i16) -> Result<T, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        let column = self.get_column_by_index(column)?;
        return T::from_bytes(bs, column);
    }

    pub fn fetch<T: Value>(&mut self, column: &str) -> Result<T, VoltError> {
        let idx = self.get_column_index(column)?;
        let bs = self.get_bytes_by_idx(idx)?;
        let column = self.get_column_by_index(idx)?;
        return T::from_bytes(bs, column);
    }

    pub fn debug_row(&mut self) -> String {
        let x: Vec<String> = self.columns().into_iter().enumerate().map(|(idx, column)| {
            return format!("{} {:?}", column.header_name, self.get_value_by_idx_type(idx as i16, column.header_type));
        }).collect();
        return x.join(" ");
    }


    pub fn has_error(&mut self) -> Option<VoltError> {
        if self.info.get_status() == Success {
            return Option::None;
        }
        return Option::Some(VoltError::ExecuteFail(self.info.clone()));
    }

    pub fn advance_row(&mut self) -> bool {
        return self.advance_to_row(self.row_index + 1);
    }

    pub fn columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    pub fn col_length(r: &mut ByteBuffer, offset: i32, col_type: i8) -> Result<i32, VoltError> {
        match col_type {
            crate::encode::ARRAY_COLUMN => {
                return Err(VoltError::InvalidColumnType(col_type));
            }
            crate::encode::NULL_COLUMN => {
                return Ok(0);
            }
            crate::encode::TINYINT_COLUMN => {
                return Ok(1);
            }
            //SHORT_COLUMN
            crate::encode::SHORT_COLUMN => {
                return Ok(2);
            }

            crate::encode::INT_COLUMN => {
                return Ok(4);
            }
            crate::encode::LONG_COLUMN => {
                return Ok(8);
            }
            crate::encode::FLOAT_COLUMN => {
                return Ok(8);
            }
            crate::encode::STRING_COLUMN => {
                r.set_rpos(offset as usize);
                let str_len = r.read_i32()?;
                if str_len == -1 { // encoding for null string.
                    return Ok(4);
                }
                return Ok(4 + str_len);
            }
            crate::encode::TIMESTAMP_COLUMN => {
                return Ok(8);
            }
            crate::encode::DECIMAL_COLUMN => {
                return Ok(16);
            }
            crate::encode::VAR_BIN_COLUMN => {
                r.set_rpos(offset as usize);
                let str_len = r.read_i32()?;
                if str_len == -1 { // encoding for null string.
                    return Ok(4);
                }
                return Ok(4 + str_len); //   4 + str_len;
            }
            _ => Err(VoltError::InvalidColumnType(col_type))
        }
    }

    fn calc_offsets(&mut self) -> Result<(), VoltError> {
        let mut offsets = Vec::with_capacity((self.column_count + 1) as usize);
        let reader = self.rows.get_mut(self.row_index as usize).ok_or(VoltError::NoValue(self.row_index.to_string()))?;
        offsets.push(0);
        let mut offset = 0;
        for i in 0..self.column_count {
            let column = self.columns.get(i as usize).ok_or(VoltError::NoValue(i.to_string()))?;
            let length = crate::table::VoltTable::col_length(reader, offset, column.header_type)?;
            offset = offset + length;
            offsets.push(offset);
        }
        reader.set_rpos(0);
        self.column_offsets = offsets;
        return Ok({});
    }

    pub fn get_value_by_column(&mut self, column: &str) -> Result<Option<Box<dyn Value>>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_value_by_idx(idx)?);
    }

    pub(crate) fn get_value_by_idx_column(column: &Column, bs: Vec<u8>) -> Result<Option<Box<dyn Value>>, VoltError> {
        return match column.header_type {
            crate::encode::TINYINT_COLUMN => {
                let res = i8::from_bytes(bs, column)?;
                match res {
                    i8::MIN => {
                        Ok(None)
                    }
                    _ => {
                        Ok(Some(Box::new(res)))
                    }
                }
            }
            crate::encode::SHORT_COLUMN => {
                let res = i16::from_bytes(bs, column)?;
                match res {
                    i16::MIN => {
                        Ok(None)
                    }
                    _ => {
                        Ok(Some(Box::new(res)))
                    }
                }
            }
            crate::encode::INT_COLUMN => {
                let res = i32::from_bytes(bs, column)?;
                match res {
                    i32::MIN => {
                        Ok(None)
                    }
                    _ => {
                        Ok(Some(Box::new(res)))
                    }
                }
            }

            crate::encode::LONG_COLUMN => {
                let res = i64::from_bytes(bs, column)?;
                match res {
                    i64::MIN => {
                        Ok(None)
                    }
                    _ => {
                        Ok(Some(Box::new(res)))
                    }
                }
            }

            crate::encode::FLOAT_COLUMN => {
                if bs == NULL_FLOAT_VALUE {
                    return Ok(None);
                }
                let res = f64::from_bytes(bs, column)?;
                return Ok(Some(Box::new(res)));
            }

            crate::encode::STRING_COLUMN => {
                if bs.len() == 4 {
                    return Ok(None);
                }
                let res = String::from_bytes(bs, column)?;
                return Ok(Some(Box::new(res)));
            }

            crate::encode::TIMESTAMP_COLUMN => {
                if bs == NULL_TIMESTAMP {
                    return Ok(None);
                }
                let res = DateTime::from_bytes(bs, column)?;
                return Ok(Some(Box::new(res)));
            }
            crate::encode::DECIMAL_COLUMN => {
                if bs == NULL_DECIMAL {
                    return Ok(None);
                }
                let res = DateTime::from_bytes(bs, column)?;
                return Ok(Some(Box::new(res)));
            }
            crate::encode::VAR_BIN_COLUMN => {
                if bs.len() == 4 {
                    return Ok(None);
                }
                let res = Vec::from_bytes(bs, column)?;
                return Ok(Some(Box::new(res)));
            }
            _ => {
                let res = i16::from_bytes(bs, column)?;
                // match res {
                //     Some(v) => {
                Ok(Some(Box::new(res)))
                //     }
                // }
            }
        };
    }

    pub(crate) fn get_value_by_idx_type(&mut self, column: i16, _tp: i8) -> Result<Option<Box<dyn Value>>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        let column = self.get_column_by_index(column)?;
        return crate::table::VoltTable::get_value_by_idx_column(column, bs);
    }


    pub fn get_value_by_idx(&mut self, column: i16) -> Result<Option<Box<dyn Value>>, VoltError> {
        let tp = self.get_column_type_by_idx(column)?;
        return self.get_value_by_idx_type(column, tp);
    }

    pub fn get_bool_by_column(&mut self, column: &str) -> Result<Option<bool>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_bool_by_idx(idx)?);
    }

    pub fn get_bool_by_idx(&mut self, column: i16) -> Result<Option<bool>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        if bs[0] == 0 {
            return Ok(Some(false));
        }
        return Ok(Some(true));
    }

    pub fn get_bytes_op_by_column(&mut self, column: &str) -> Result<Option<Vec<u8>>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_bytes_op_by_idx(idx)?);
    }

    pub fn get_bytes_op_by_idx(&mut self, column: i16) -> Result<Option<Vec<u8>>, VoltError> {
        let mut bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_VARCHAR {
            return Ok(Option::None);
        }
        bs.drain(0..4);
        return Ok(Option::Some(bs));
    }

    pub fn get_bytes_by_column(&mut self, column: &str) -> Result<Vec<u8>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_bytes_by_idx(idx)?);
    }

    pub fn get_decimal_by_column(&mut self, column: &str) -> Result<Option<BigDecimal>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_decimal_by_idx(idx)?);
    }

    pub fn get_decimal_by_idx(&mut self, column: i16) -> Result<Option<BigDecimal>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_DECIMAL {
            return Ok(Option::None);
        }
        let int = BigInt::from_signed_bytes_be(&*bs);
        let decimal = BigDecimal::new(int, 12);
        return Ok(Some(decimal));
    }

    pub fn get_string_by_column(&mut self, column: &str) -> Result<Option<String>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_string_by_idx(idx)?);
    }

    pub fn get_column_by_index(&self, column: i16) -> Result<&Column, VoltError> {
        let res = self.columns.get(column as usize);
        return match res {
            None => {
                Err(VoltError::NoValue(self.row_index.to_string()))
            }
            Some(e) => {
                Ok(e)
            }
        };
    }

    #[allow(mutable_borrow_reservation_conflict)]
    pub fn get_string_by_idx(&mut self, column: i16) -> Result<Option<String>, VoltError> {
        let table_column = self.get_column_by_index(column)?; // will change type and name into one map
        return match table_column.header_type {
            STRING_COLUMN => {
                let bs = self.get_bytes_by_idx(column)?;
                if bs == NULL_VARCHAR {
                    return Ok(Option::None);
                }
                let mut buffer = ByteBuffer::from_bytes(&bs);
                Ok(Option::Some(buffer.read_string()?))
            }
            _ => {
                let res = self.get_value_by_idx_type(column, table_column.header_type)?;
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

    pub fn get_time_by_column(&mut self, column: &str) -> Result<Option<DateTime<Utc>>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_time_by_idx(idx)?);
    }

    pub fn get_time_by_idx(&mut self, column: i16) -> Result<Option<DateTime<Utc>>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_TIMESTAMP {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let time = buffer.read_i64()?;
        return Ok(Option::Some(Utc.timestamp_millis(time / 1000)));
    }


    pub fn get_bytes_by_idx(&mut self, column_index: i16) -> Result<Vec<u8>, VoltError> {
        if self.column_offsets.len() == 0 {
            self.calc_offsets()?;
        }
        let buffer = self.rows.get_mut(self.row_index as usize).ok_or(VoltError::NoValue(self.row_index.to_string()))?;
        let start = self.column_offsets.get(column_index as usize).ok_or(VoltError::NoValue(column_index.to_string()))?;
        let end = self.column_offsets.get(column_index as usize + 1).ok_or(VoltError::NoValue(column_index.to_string()))?;
        buffer.set_rpos(*start as usize);
        let rsize = (*end - *start) as usize;
        Ok(buffer.read_bytes(rsize)?)
    }

    pub fn advance_to_row(&mut self, row_index: i32) -> bool {
        if row_index >= self.num_rows {
            return false;
        }
        self.column_offsets = vec![];
        self.row_index = row_index;
        return true;
    }

    fn get_column_type_by_idx(&self, column_idx: i16) -> Result<i8, VoltError> {
        let v = self.columns.get(column_idx as usize);
        if v.is_some() {
            return Ok(v.unwrap().header_type);
        }
        return Err(VoltError::NoValue(column_idx.to_string()));
    }
}


pub fn new_volt_table(bytebuffer: &mut ByteBuffer, info: VoltResponseInfo) -> Result<VoltTable, VoltError> {
    if info.get_status() != Success {
        return Ok(VoltTable {
            info,
            column_count: -1,
            info_bytes: Default::default(),
            column_info_bytes: Default::default(),
            columns: vec![],

            num_rows: -1,
            rows: vec![],
            row_index: -1,
            cn_to_ci: Default::default(),
            column_offsets: vec![],
            header_size: 0,
            total_size: 0,
        });
    }

    let column_counts = decode_table_common(bytebuffer)?;
    let mut column_types: Vec<i8> = Vec::with_capacity(column_counts as usize);
    let mut column_info_bytes = ByteBuffer::new();
    for _ in 0..column_counts {
        let tp = bytebuffer.read_i8()?;
        column_types.push(tp);
        column_info_bytes.write_i8(tp);
    }
    let mut columns: Vec<Column> = Vec::with_capacity(column_counts as usize);
    let mut cn_to_ci = HashMap::with_capacity(column_counts as usize);
    for i in 0..column_counts {
        let name = bytebuffer.read_string()?;
        columns.push(Column {
            header_name: name.clone(),
            header_type: *(column_types.get(i as usize).unwrap()),
        });
        column_info_bytes.write_string(name.as_str());
        cn_to_ci.insert(name, i);
    }
    let row_count = bytebuffer.read_i32()?;
    let mut rows: Vec<ByteBuffer> = Vec::with_capacity(column_counts as usize);
    for _ in 0..row_count {
        let row_len = bytebuffer.read_i32()?;
        let mut build = vec![0; row_len as usize];
        bytebuffer.read_exact(&mut build)?;
        let row = ByteBuffer::from_bytes(&*build);
        rows.push(row);
    }
    return Ok(VoltTable {
        info,
        column_count: column_counts,
        info_bytes: Default::default(),
        column_info_bytes,
        columns,
        num_rows: row_count,
        rows,
        row_index: -1,
        cn_to_ci,
        column_offsets: vec![],
        header_size: 0,
        total_size: 0,
    });
}

fn decode_table_common(bytebuffer: &mut ByteBuffer) -> Result<i16, VoltError> {
    let _ttl_length = bytebuffer.read_i32();
    let _meta_length = bytebuffer.read_i32();
    let status_code = bytebuffer.read_i8()?;
    if status_code != 0 && status_code != MIN_INT8 {
        // copy from golang , but why ?
        return Err(VoltError::BadReturnStatusOnTable(status_code));
    }
    return Ok(bytebuffer.read_i16()?);
}


#[cfg(test)]
mod tests {
    use crate::encode::{DECIMAL_COLUMN, FLOAT_COLUMN, INT_COLUMN, LONG_COLUMN, SHORT_COLUMN, STRING_COLUMN, TIMESTAMP_COLUMN, TINYINT_COLUMN, VAR_BIN_COLUMN};
    use crate::volt_param;

    use super::*;

    fn template(tps: Vec<&str>, none: &str) {
        for tp in tps {
            let shader = r#"
                pub fn get_${type}_by_column (&mut self, column: &str) -> Result<Option<${type}>, VoltError> {
                    let idx = self.get_column_index(column)?;
                    return Ok(self.get_${type}_by_idx>](idx)?);
                }

                pub fn get_${type}_by_idx (&mut self, column: i16) -> Result<Option<${type}>, VoltError> {
                    let bs = self.get_bytes_by_idx(column)?;
                    if bs == ${none} {
                       return Ok(Option::None);
                    }
                    let mut buffer = ByteBuffer::from_bytes(&bs);
                    let value = buffer.read_${type}()?;
                    return Ok(Some(value));
                }
"#;
            println!("{}", shader.replace("${type}", tp).replace("${none}", none));
        }
    }

    #[test]
    fn test_encode_table() -> Result<(), VoltError> {
        let bs = vec! {21, 0, 0, 0, 86, 0, 0, 0, 49, 128, 0, 4, 6, 6, 3, 6, 0, 0, 0, 2, 73, 68, 0, 0, 0, 7, 86, 69, 82, 83, 73, 79, 78, 0, 0, 0, 7, 68, 69, 76, 69, 84, 69, 68, 0, 0, 0, 10, 67, 82, 69, 65, 84, 69, 68, 95, 66, 89, 0, 0, 0, 1, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1};
        let header = vec!["ID", "VERSION", "DELETED", "CREATED_BY"];
        let tp = vec![LONG_COLUMN, LONG_COLUMN, TINYINT_COLUMN, LONG_COLUMN];
        let header: Vec<String> = header.iter().map(|f| f.to_string()).collect::<Vec<String>>();
        let mut x = VoltTable::new_table(tp, header);
        let data = volt_param! {1 as i64, 1 as i64, false,  1 as i64 };
        x.add_row(data)?;
        let mut bf = ByteBuffer::new();
        x.marshal(&mut bf);
        assert_eq!(bs, bf.to_bytes());
        Ok({})
    }

    #[test]
    fn test_table() {
        let bs = vec! {0 as u8, 1, 128, 0, 0, 0, 3, 0, 1, 0, 0, 0, 133, 0, 0, 0, 66, 128, 0, 9, 3, 4, 5, 6, 8, 22, 9, 25, 11, 0, 0, 0, 2, 84, 49, 0, 0, 0, 2, 84, 50, 0, 0, 0, 2, 84, 51, 0, 0, 0, 2, 84, 52, 0, 0, 0, 2, 84, 53, 0, 0, 0, 2, 84, 54, 0, 0, 0, 2, 84, 55, 0, 0, 0, 2, 84, 56, 0, 0, 0, 2, 84, 57, 0, 0, 0, 1, 0, 0, 0, 55, 128, 128, 0, 128, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 255, 239, 255, 255, 255, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 128, 0, 0, 0, 0, 0, 0, 0};
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let header = table.columns();
        assert_eq!(header.len(), 9);
        assert_eq!(*header.get(0).unwrap(), Column {
            header_name: "T1".to_owned(),
            header_type: TINYINT_COLUMN,
        });
        assert_eq!(*header.get(1).unwrap(), Column {
            header_name: "T2".to_owned(),
            header_type: SHORT_COLUMN,
        });
        assert_eq!(*header.get(2).unwrap(), Column {
            header_name: "T3".to_owned(),
            header_type: INT_COLUMN,
        });
        assert_eq!(*header.get(3).unwrap(), Column {
            header_name: "T4".to_owned(),
            header_type: LONG_COLUMN,
        });
        assert_eq!(*header.get(4).unwrap(), Column {
            header_name: "T5".to_owned(),
            header_type: FLOAT_COLUMN,
        });
        assert_eq!(*header.get(5).unwrap(), Column {
            header_name: "T6".to_owned(),
            header_type: DECIMAL_COLUMN,
        });
        assert_eq!(*header.get(6).unwrap(), Column {
            header_name: "T7".to_owned(),
            header_type: STRING_COLUMN,
        });
        assert_eq!(*header.get(7).unwrap(), Column {
            header_name: "T8".to_owned(),
            header_type: VAR_BIN_COLUMN,
        });
        assert_eq!(*header.get(8).unwrap(), Column {
            header_name: "T9".to_owned(),
            header_type: TIMESTAMP_COLUMN,
        });


        let i1 = table.get_bool_by_idx(0).unwrap();
        let i2 = table.get_i16_by_idx(1).unwrap();
        let i3 = table.get_i32_by_idx(2).unwrap();
        let i4 = table.get_i64_by_idx(3).unwrap();
        let i5 = table.get_f64_by_idx(4).unwrap();
        let i6 = table.get_decimal_by_idx(5).unwrap();
        let i7 = table.get_string_by_idx(6).unwrap();
        let i8 = table.get_bytes_op_by_idx(7).unwrap();
        let i9 = table.get_time_by_idx(8).unwrap();

        assert_eq!(i1, None);
        assert_eq!(i2, None);
        assert_eq!(i3, None);
        assert_eq!(i4, None);
        assert_eq!(i5, None);
        assert_eq!(i6, None);
        assert_eq!(i7, None);
        assert_eq!(i8, None);
        assert_eq!(i9, None);
        let offsets = vec![0, 1, 3, 7, 15, 23, 39, 43, 47, 55];
        assert_eq!(offsets, table.column_offsets);
    }

    #[test]
    fn test_big_decimal() {
        template(vec!("i8", "u8"), "NULL_BYTE_VALUE");
        template(vec!("i16", "u16"), "NULL_SHORT_VALUE");
        template(vec!("i32", "u32"), "NULL_INT_VALUE");
        template(vec!("i64", "u64"), "NULL_LONG_VALUE");
        template(vec!("f32", "f64"), "NULL_FLOAT_VALUE");
    }
}

