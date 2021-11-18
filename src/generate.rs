use crate::table::VoltTable;
use crate::encode::{NULL_BYTE_VALUE, VoltError, NULL_SHORT_VALUE, NULL_INT_VALUE, NULL_LONG_VALUE, NULL_FLOAT_VALUE};
use bytebuffer::ByteBuffer;

impl VoltTable {
    pub fn get_i8_by_column(&mut self, column: &str) -> Result<Option<i8>, VoltError> {
        let idx = self.get_column_index(column)?;
        return Ok(self.get_i8_by_idx(idx)?);
    }

    pub fn get_i8_by_idx(&mut self, column: i16) -> Result<Option<i8>, VoltError> {
        let bs = self.get_bytes_by_idx(column)?;
        if bs == NULL_BYTE_VALUE {
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
        if bs == NULL_BYTE_VALUE {
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
