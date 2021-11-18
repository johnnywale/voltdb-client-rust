use bytebuffer::ByteBuffer;

use crate::encode::VoltError;

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseStatus {
    Success,
    UserAbort,
    GracefulFailure,
    UnexpectedFailure,
    ConnectionLost,
    ServerUnavailable,
    ConnectionTimeout,
    ResponseUnknown,
    TXNRestart,
    OperationalFailure,

    // -10 to -12 are not in the client's preview
    UnsupportedDynamicChange,
    UninitializedAppStatusCode,
    Customized(i8),
}

impl From<i8> for ResponseStatus {
    fn from(s: i8) -> Self {
        match s {
            1 => ResponseStatus::Success,
            -1 => ResponseStatus::UserAbort,
            -2 => ResponseStatus::GracefulFailure,
            -3 => ResponseStatus::UnexpectedFailure,
            -4 => ResponseStatus::ConnectionLost,
            -5 => ResponseStatus::ServerUnavailable,
            -6 => ResponseStatus::ConnectionTimeout,
            -7 => ResponseStatus::ResponseUnknown,
            -8 => ResponseStatus::TXNRestart,
            -9 => ResponseStatus::OperationalFailure,
            -13 => ResponseStatus::UnsupportedDynamicChange,
            -128 => ResponseStatus::UninitializedAppStatusCode,
            _ => {
                ResponseStatus::Customized(s)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct VoltResponseInfo {
    handle: i64,
    status: ResponseStatus,
    status_string: String,
    app_status: ResponseStatus,
    app_status_string: String,
    cluster_round_trip_time: i32,
    num_tables: i16,
}

impl VoltResponseInfo {
    #[allow(dead_code)]
    pub fn get_rows_number(&self) -> i16 {
        return self.num_tables;
    }
    pub fn get_status(&self) -> ResponseStatus {
        return self.status.clone();
    }
}

impl VoltResponseInfo {
    pub fn new(bytebuffer: &mut ByteBuffer, handle: i64) -> Result<Self, VoltError> {
        let fields_present = bytebuffer.read_u8()?;
        let status = ResponseStatus::from(bytebuffer.read_i8()?);
        let mut status_string = "".to_owned();
        if fields_present & (1 << 5) != 0 {
            status_string = bytebuffer.read_string()?;
        }
        let app_status = ResponseStatus::from(bytebuffer.read_i8()?);
        let mut app_status_string = "".to_owned();
        if fields_present & (1 << 7) != 0 {
            app_status_string = bytebuffer.read_string()?;
        }
        let cluster_round_trip_time = bytebuffer.read_i32()?;
        let num_tables = bytebuffer.read_i16()?;
        if num_tables < 0 {
            return Err(VoltError::NegativeNumTables(num_tables));
        }
        Ok(VoltResponseInfo {
            handle,
            status,
            status_string,
            app_status,
            app_status_string,
            cluster_round_trip_time,
            num_tables,
        })
    }
}
