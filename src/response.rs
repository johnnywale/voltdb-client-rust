use bytebuffer::ByteBuffer;

use crate::encode::VoltError;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum ResponseStatus {
    #[default]
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
            _ => ResponseStatus::Customized(s),
        }
    }
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
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
        self.num_tables
    }
    pub fn get_status(&self) -> ResponseStatus {
        self.status.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_status_from_success() {
        assert_eq!(ResponseStatus::from(1), ResponseStatus::Success);
    }

    #[test]
    fn test_response_status_from_user_abort() {
        assert_eq!(ResponseStatus::from(-1), ResponseStatus::UserAbort);
    }

    #[test]
    fn test_response_status_from_graceful_failure() {
        assert_eq!(ResponseStatus::from(-2), ResponseStatus::GracefulFailure);
    }

    #[test]
    fn test_response_status_from_unexpected_failure() {
        assert_eq!(ResponseStatus::from(-3), ResponseStatus::UnexpectedFailure);
    }

    #[test]
    fn test_response_status_from_connection_lost() {
        assert_eq!(ResponseStatus::from(-4), ResponseStatus::ConnectionLost);
    }

    #[test]
    fn test_response_status_from_server_unavailable() {
        assert_eq!(ResponseStatus::from(-5), ResponseStatus::ServerUnavailable);
    }

    #[test]
    fn test_response_status_from_connection_timeout() {
        assert_eq!(ResponseStatus::from(-6), ResponseStatus::ConnectionTimeout);
    }

    #[test]
    fn test_response_status_from_response_unknown() {
        assert_eq!(ResponseStatus::from(-7), ResponseStatus::ResponseUnknown);
    }

    #[test]
    fn test_response_status_from_txn_restart() {
        assert_eq!(ResponseStatus::from(-8), ResponseStatus::TXNRestart);
    }

    #[test]
    fn test_response_status_from_operational_failure() {
        assert_eq!(ResponseStatus::from(-9), ResponseStatus::OperationalFailure);
    }

    #[test]
    fn test_response_status_from_unsupported_dynamic_change() {
        assert_eq!(
            ResponseStatus::from(-13),
            ResponseStatus::UnsupportedDynamicChange
        );
    }

    #[test]
    fn test_response_status_from_uninitialized() {
        assert_eq!(
            ResponseStatus::from(-128),
            ResponseStatus::UninitializedAppStatusCode
        );
    }

    #[test]
    fn test_response_status_from_customized() {
        assert_eq!(ResponseStatus::from(99), ResponseStatus::Customized(99));
        assert_eq!(ResponseStatus::from(-50), ResponseStatus::Customized(-50));
    }

    #[test]
    fn test_response_status_default() {
        assert_eq!(ResponseStatus::default(), ResponseStatus::Success);
    }

    #[test]
    fn test_response_status_eq() {
        assert_eq!(ResponseStatus::Success, ResponseStatus::Success);
        assert_ne!(ResponseStatus::Success, ResponseStatus::UserAbort);
    }

    #[test]
    fn test_response_status_clone() {
        let status = ResponseStatus::GracefulFailure;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_volt_response_info_new_success() {
        // Build a minimal response buffer
        let mut buf = ByteBuffer::new();
        buf.write_u8(0); // fields_present (no optional strings)
        buf.write_i8(1); // status = Success
        buf.write_i8(1); // app_status = Success
        buf.write_i32(100); // cluster_round_trip_time
        buf.write_i16(1); // num_tables

        let mut read_buf = ByteBuffer::from_bytes(&buf.into_vec());
        let info = VoltResponseInfo::new(&mut read_buf, 42).unwrap();

        assert_eq!(info.get_status(), ResponseStatus::Success);
        assert_eq!(info.get_rows_number(), 1);
    }

    #[test]
    fn test_volt_response_info_with_status_string() {
        let mut buf = ByteBuffer::new();
        buf.write_u8(1 << 5); // fields_present with status_string
        buf.write_i8(-2); // status = GracefulFailure
        buf.write_string("Error message");
        buf.write_i8(1); // app_status
        buf.write_i32(50); // cluster_round_trip_time
        buf.write_i16(0); // num_tables

        let mut read_buf = ByteBuffer::from_bytes(&buf.into_vec());
        let info = VoltResponseInfo::new(&mut read_buf, 1).unwrap();

        assert_eq!(info.get_status(), ResponseStatus::GracefulFailure);
    }

    #[test]
    fn test_volt_response_info_with_app_status_string() {
        let mut buf = ByteBuffer::new();
        buf.write_u8(1 << 7); // fields_present with app_status_string
        buf.write_i8(1); // status = Success
        buf.write_i8(-1); // app_status = UserAbort
        buf.write_string("App error");
        buf.write_i32(75); // cluster_round_trip_time
        buf.write_i16(2); // num_tables

        let mut read_buf = ByteBuffer::from_bytes(&buf.into_vec());
        let info = VoltResponseInfo::new(&mut read_buf, 99).unwrap();

        assert_eq!(info.get_status(), ResponseStatus::Success);
        assert_eq!(info.get_rows_number(), 2);
    }

    #[test]
    fn test_volt_response_info_negative_tables_error() {
        let mut buf = ByteBuffer::new();
        buf.write_u8(0); // fields_present
        buf.write_i8(1); // status
        buf.write_i8(1); // app_status
        buf.write_i32(0); // cluster_round_trip_time
        buf.write_i16(-1); // negative num_tables

        let mut read_buf = ByteBuffer::from_bytes(&buf.into_vec());
        let result = VoltResponseInfo::new(&mut read_buf, 1);

        assert!(result.is_err());
        match result {
            Err(VoltError::NegativeNumTables(n)) => assert_eq!(n, -1),
            _ => panic!("Expected NegativeNumTables error"),
        }
    }

    #[test]
    fn test_volt_response_info_default() {
        let info = VoltResponseInfo::default();
        assert_eq!(info.get_status(), ResponseStatus::Success);
        assert_eq!(info.get_rows_number(), 0);
    }
}
