use bytebuffer::ByteBuffer;

use crate::encode::Value;

#[allow(dead_code)]
pub(crate) struct ProcedureInvocation<'a> {
    handle: i64,
    is_query: bool,
    params: &'a Vec<&'a dyn Value>,
    query: String,
    pub(crate) slen: i32, // length of pi once serialized
}

pub(crate) fn new_procedure_invocation<'a>(
    handle: i64,
    is_query: bool,
    params: &'a Vec<&dyn Value>,
    query: &str,
) -> ProcedureInvocation<'a> {
    ProcedureInvocation {
        handle,
        is_query,
        params,
        query: query.to_string(),
        slen: -1,
    }
}

impl<'a> ProcedureInvocation<'a> {
    fn calculate_length(&mut self) -> i32 {
        if self.slen != -1 {
            return self.slen;
        }
        let mut slen: i32 = 15;
        slen += self.query.len() as i32;
        for item in self.params {
            slen += item.get_write_length()
        }
        self.slen = slen;
        slen
    }

    pub(crate) fn bytes(&mut self) -> Vec<u8> {
        let length = self.calculate_length();
        let mut buffer = ByteBuffer::new();
        buffer.write_i32(length);
        buffer.write_u8(0);
        buffer.write_string(self.query.as_str());
        buffer.write_i64(self.handle);
        buffer.write_i16(self.params.len() as i16);
        for item in self.params.iter() {
            item.marshal(&mut buffer)
        }
        buffer.into_vec()
    }
}
