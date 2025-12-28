use bytebuffer::ByteBuffer;

use crate::encode::*;
use crate::table::VoltTable;

impl VoltTable {
    pub fn get_i8_by_column(&mut self, column: &str) -> Result<Option<i8>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_i8_by_idx(idx)?);
    }

    pub fn get_i8_by_idx(&mut self, column: i16) -> Result<Option<i8>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i8()?;
        return Ok(Some(value));
    }

    pub fn get_u8_by_column(&mut self, column: &str) -> Result<Option<u8>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_u8_by_idx(idx)?);
    }

    pub fn get_u8_by_idx(&mut self, column: i16) -> Result<Option<u8>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_BIT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u8()?;
        return Ok(Some(value));
    }

    pub fn get_i16_by_column(&mut self, column: &str) -> Result<Option<i16>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_i16_by_idx(idx)?);
    }

    pub fn get_i16_by_idx(&mut self, column: i16) -> Result<Option<i16>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i16()?;
        return Ok(Some(value));
    }

    pub fn get_u16_by_column(&mut self, column: &str) -> Result<Option<u16>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_u16_by_idx(idx)?);
    }

    pub fn get_u16_by_idx(&mut self, column: i16) -> Result<Option<u16>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_SHORT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u16()?;
        return Ok(Some(value));
    }

    pub fn get_i32_by_column(&mut self, column: &str) -> Result<Option<i32>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_i32_by_idx(idx)?);
    }

    pub fn get_i32_by_idx(&mut self, column: i16) -> Result<Option<i32>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i32()?;
        return Ok(Some(value));
    }

    pub fn get_u32_by_column(&mut self, column: &str) -> Result<Option<u32>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_u32_by_idx(idx)?);
    }

    pub fn get_u32_by_idx(&mut self, column: i16) -> Result<Option<u32>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_INT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u32()?;
        return Ok(Some(value));
    }

    pub fn get_i64_by_column(&mut self, column: &str) -> Result<Option<i64>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_i64_by_idx(idx)?);
    }

    pub fn get_i64_by_idx(&mut self, column: i16) -> Result<Option<i64>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_i64()?;
        return Ok(Some(value));
    }

    pub fn get_u64_by_column(&mut self, column: &str) -> Result<Option<u64>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_u64_by_idx(idx)?);
    }

    pub fn get_u64_by_idx(&mut self, column: i16) -> Result<Option<u64>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_LONG_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_u64()?;
        return Ok(Some(value));
    }

    pub fn get_f32_by_column(&mut self, column: &str) -> Result<Option<f32>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_f32_by_idx(idx)?);
    }

    pub fn get_f32_by_idx(&mut self, column: i16) -> Result<Option<f32>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_FLOAT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_f32()?;
        return Ok(Some(value));
    }

    pub fn get_f64_by_column(&mut self, column: &str) -> Result<Option<f64>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_f64_by_idx(idx)?);
    }

    pub fn get_f64_by_idx(&mut self, column: i16) -> Result<Option<f64>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_FLOAT_VALUE {
            return Ok(Option::None);
        }
        let mut buffer = ByteBuffer::from_bytes(&bs);
        let value = buffer.read_f64()?;
        return Ok(Some(value));
    }
}

#[cfg(test)]
mod tests {
    use crate::response::VoltResponseInfo;
    use crate::table::new_volt_table;
    use bytebuffer::ByteBuffer;

    // Test bytes for a table with various numeric columns:
    // Columns: T1(tinyint), T2(short), T3(int), T4(long), T5(float)
    // Values: -128 (NULL), -32768 (NULL), -2147483648 (NULL), -9223372036854775808 (NULL), NULL float
    fn get_null_table_bytes() -> Vec<u8> {
        vec![
            0, 1, 128, 0, 0, 0, 3, 0, 1, 0, 0, 0, 133, 0, 0, 0, 66, 128, 0, 9, 3, 4, 5, 6, 8, 22,
            9, 25, 11, 0, 0, 0, 2, 84, 49, 0, 0, 0, 2, 84, 50, 0, 0, 0, 2, 84, 51, 0, 0, 0, 2, 84,
            52, 0, 0, 0, 2, 84, 53, 0, 0, 0, 2, 84, 54, 0, 0, 0, 2, 84, 55, 0, 0, 0, 2, 84, 56, 0,
            0, 0, 2, 84, 57, 0, 0, 0, 1, 0, 0, 0, 55, 128, // T1: NULL tinyint
            128, 0, // T2: NULL short
            128, 0, 0, 0, // T3: NULL int
            128, 0, 0, 0, 0, 0, 0, 0, // T4: NULL long
            255, 239, 255, 255, 255, 255, 255, 255, // T5: NULL float
            128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // T6: NULL decimal
            255, 255, 255, 255, // T7: NULL string
            255, 255, 255, 255, // T8: NULL varbinary
            128, 0, 0, 0, 0, 0, 0, 0, // T9: NULL timestamp
        ]
    }

    // Test bytes with actual values (non-NULL)
    fn get_value_table_bytes() -> Vec<u8> {
        vec![
            0, 1, 128, 0, 0, 0, 3, 0, 1, 0, 0, 0, 133, 0, 0, 0, 66, 128, 0, 9, 3, 4, 5, 6, 8, 22,
            9, 25, 11, 0, 0, 0, 2, 84, 49, 0, 0, 0, 2, 84, 50, 0, 0, 0, 2, 84, 51, 0, 0, 0, 2, 84,
            52, 0, 0, 0, 2, 84, 53, 0, 0, 0, 2, 84, 54, 0, 0, 0, 2, 84, 55, 0, 0, 0, 2, 84, 56, 0,
            0, 0, 2, 84, 57, 0, 0, 0, 1, 0, 0, 0, 55, 42, // T1: tinyint = 42
            0, 100, // T2: short = 100
            0, 0, 1, 0, // T3: int = 256
            0, 0, 0, 0, 0, 0, 0, 42, // T4: long = 42
            64, 9, 33, 251, 84, 68, 45, 24, // T5: f64 ~ 3.14159
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 232, 212, 165, 16, 0, // T6: decimal
            255, 255, 255, 255, // T7: NULL string
            255, 255, 255, 255, // T8: NULL varbinary
            0, 0, 0, 0, 0, 15, 66, 64, // T9: timestamp
        ]
    }

    #[test]
    fn test_get_i8_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i8_by_idx(0).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_i8_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i8_by_idx(0).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_u8_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u8_by_idx(0).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_u8_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u8_by_idx(0).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_i16_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i16_by_idx(1).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_i16_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i16_by_idx(1).unwrap();
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_get_u16_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u16_by_idx(1).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_u16_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u16_by_idx(1).unwrap();
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_get_i32_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i32_by_idx(2).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_i32_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i32_by_idx(2).unwrap();
        assert_eq!(result, Some(256));
    }

    #[test]
    fn test_get_u32_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u32_by_idx(2).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_u32_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u32_by_idx(2).unwrap();
        assert_eq!(result, Some(256));
    }

    #[test]
    fn test_get_i64_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i64_by_idx(3).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_i64_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i64_by_idx(3).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_u64_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u64_by_idx(3).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_u64_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u64_by_idx(3).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_f32_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_f32_by_idx(4).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_f64_by_idx_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_f64_by_idx(4).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_f64_by_idx_value() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_f64_by_idx(4).unwrap();
        assert!(result.is_some());
        let val = result.unwrap();
        assert!((val - 3.14159).abs() < 0.0001);
    }

    #[test]
    fn test_get_i8_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i8_by_column("T1").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_u8_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u8_by_column("T1").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_i16_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i16_by_column("T2").unwrap();
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_get_u16_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u16_by_column("T2").unwrap();
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_get_i32_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i32_by_column("T3").unwrap();
        assert_eq!(result, Some(256));
    }

    #[test]
    fn test_get_u32_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u32_by_column("T3").unwrap();
        assert_eq!(result, Some(256));
    }

    #[test]
    fn test_get_i64_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_i64_by_column("T4").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_u64_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_u64_by_column("T4").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_f32_by_column_null() {
        let bs = get_null_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_f32_by_column("T5").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_f64_by_column() {
        let bs = get_value_table_bytes();
        let mut b = ByteBuffer::from_bytes(&bs);
        let info = VoltResponseInfo::new(&mut b, 1).unwrap();
        let mut table = new_volt_table(&mut b, info).unwrap();
        table.advance_row();

        let result = table.get_f64_by_column("T5").unwrap();
        assert!(result.is_some());
        let val = result.unwrap();
        assert!((val - 3.14159).abs() < 0.0001);
    }
}
