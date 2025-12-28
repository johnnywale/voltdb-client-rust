//! Unit tests that don't require a running VoltDB instance

#[allow(unused_imports)]
use voltdb_client_rust::*;

// ============================================================================
// Encode Tests - Primitive Types
// ============================================================================

mod encode_primitive_tests {
    use bytebuffer::ByteBuffer;
    use voltdb_client_rust::*;

    #[test]
    fn test_bool_marshal() {
        let val = true;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(buffer.as_bytes()[0], TINYINT_COLUMN as u8);
        assert_eq!(buffer.as_bytes()[1], 1);

        let val = false;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(buffer.as_bytes()[1], 0);
    }

    #[test]
    fn test_i8_marshal() {
        let val: i8 = 42;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 2);
        assert_eq!(buffer.as_bytes()[0], TINYINT_COLUMN as u8);
    }

    #[test]
    fn test_i16_marshal() {
        let val: i16 = 1000;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 3);
        assert_eq!(buffer.as_bytes()[0], SHORT_COLUMN as u8);
    }

    #[test]
    fn test_i32_marshal() {
        let val: i32 = 100000;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 5);
        assert_eq!(buffer.as_bytes()[0], INT_COLUMN as u8);
    }

    #[test]
    fn test_i64_marshal() {
        let val: i64 = 10000000000;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 9);
        assert_eq!(buffer.as_bytes()[0], LONG_COLUMN as u8);
    }

    #[test]
    fn test_f64_marshal() {
        let val: f64 = 3.14159;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 9);
        assert_eq!(buffer.as_bytes()[0], FLOAT_COLUMN as u8);
    }

    #[test]
    fn test_string_marshal() {
        let val = "hello".to_string();
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 10); // 5 + 5 bytes for "hello"
        assert_eq!(buffer.as_bytes()[0], STRING_COLUMN as u8);
    }

    #[test]
    fn test_str_marshal() {
        let val = "world";
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 10); // 5 + 5 bytes
        assert_eq!(buffer.as_bytes()[0], STRING_COLUMN as u8);
    }

    #[test]
    fn test_vec_u8_marshal() {
        let val: Vec<u8> = vec![1, 2, 3, 4, 5];
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 10); // 5 + 5 bytes
        assert_eq!(buffer.as_bytes()[0], VAR_BIN_COLUMN as u8);
    }

    #[test]
    fn test_bigdecimal_marshal() {
        let val = BigDecimal::from(12345);
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        assert_eq!(val.get_write_length(), 17);
        assert_eq!(buffer.as_bytes()[0], DECIMAL_COLUMN as u8);
    }
}

// ============================================================================
// Encode Tests - Option Types (NULL handling)
// ============================================================================

mod encode_option_tests {
    use bytebuffer::ByteBuffer;
    use voltdb_client_rust::*;

    #[test]
    fn test_option_i8_none() {
        let val: Option<i8> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], TINYINT_COLUMN as u8);
        assert_eq!(bytes[1], NULL_BIT_VALUE[0]);
    }

    #[test]
    fn test_option_i8_some() {
        let val: Option<i8> = Some(42);
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], TINYINT_COLUMN as u8);
        assert_eq!(bytes[1], 42);
    }

    #[test]
    fn test_option_i16_none() {
        let val: Option<i16> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], SHORT_COLUMN as u8);
        assert_eq!(&bytes[1..3], &NULL_SHORT_VALUE);
    }

    #[test]
    fn test_option_i32_none() {
        let val: Option<i32> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], INT_COLUMN as u8);
        assert_eq!(&bytes[1..5], &NULL_INT_VALUE);
    }

    #[test]
    fn test_option_i64_none() {
        let val: Option<i64> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], LONG_COLUMN as u8);
        assert_eq!(&bytes[1..9], &NULL_LONG_VALUE);
    }

    #[test]
    fn test_option_f64_none() {
        let val: Option<f64> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], FLOAT_COLUMN as u8);
        assert_eq!(&bytes[1..9], &NULL_FLOAT_VALUE);
    }

    #[test]
    fn test_option_string_none() {
        let val: Option<String> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], STRING_COLUMN as u8);
        assert_eq!(&bytes[1..5], &NULL_VARCHAR);
    }

    #[test]
    fn test_option_string_some() {
        let val: Option<String> = Some("test".to_string());
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], STRING_COLUMN as u8);
        // String length should be 4
        assert_eq!(&bytes[1..5], &[0, 0, 0, 4]);
    }

    #[test]
    fn test_option_bigdecimal_none() {
        let val: Option<BigDecimal> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], DECIMAL_COLUMN as u8);
        assert_eq!(&bytes[1..17], &NULL_DECIMAL);
    }

    #[test]
    fn test_option_bool_none() {
        let val: Option<bool> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], TINYINT_COLUMN as u8);
        assert_eq!(bytes[1], NULL_BIT_VALUE[0]);
    }

    #[test]
    fn test_option_bool_some_true() {
        let val: Option<bool> = Some(true);
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[1], 1);
    }

    #[test]
    fn test_option_bool_some_false() {
        let val: Option<bool> = Some(false);
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[1], 0);
    }
}

// ============================================================================
// volt_param! Macro Tests
// ============================================================================

mod macro_tests {
    use voltdb_client_rust::*;

    #[test]
    fn test_volt_param_empty() {
        let params: Vec<&dyn Value> = volt_param![];
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_volt_param_single() {
        let val = 42i32;
        let params = volt_param![val];
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_volt_param_multiple() {
        let a = 1i32;
        let b = "hello".to_string();
        let c = 3.14f64;
        let params = volt_param![a, b, c];
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_volt_param_with_options() {
        let a: Option<i32> = Some(42);
        let b: Option<String> = None;
        let params = volt_param![a, b];
        assert_eq!(params.len(), 2);
    }
}

// ============================================================================
// Table Tests
// ============================================================================

mod table_tests {
    use voltdb_client_rust::*;

    #[test]
    fn test_new_table() {
        let types = vec![LONG_COLUMN, STRING_COLUMN, INT_COLUMN];
        let headers: Vec<String> = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let table = VoltTable::new_table(types, headers);
        let columns = table.columns();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].header_name, "id");
        assert_eq!(columns[0].header_type, LONG_COLUMN);
        assert_eq!(columns[1].header_name, "name");
        assert_eq!(columns[1].header_type, STRING_COLUMN);
        assert_eq!(columns[2].header_name, "age");
        assert_eq!(columns[2].header_type, INT_COLUMN);
    }

    #[test]
    fn test_table_add_row() {
        let types = vec![INT_COLUMN, STRING_COLUMN];
        let headers: Vec<String> = vec!["id".to_string(), "name".to_string()];
        let mut table = VoltTable::new_table(types, headers);

        let id = 1i32;
        let name = "Alice".to_string();
        let result = table.add_row(volt_param![id, name]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_table_multiple_rows() {
        let types = vec![INT_COLUMN];
        let headers: Vec<String> = vec!["value".to_string()];
        let mut table = VoltTable::new_table(types, headers);

        for i in 0..5 {
            let val = i as i32;
            table.add_row(volt_param![val]).unwrap();
        }
        // Table should have 5 rows
    }

    #[test]
    fn test_column_struct() {
        let col = Column {
            header_name: "test".to_string(),
            header_type: INT_COLUMN,
        };
        assert_eq!(col.header_name, "test");
        assert_eq!(col.header_type, INT_COLUMN);
    }
}

// ============================================================================
// Node/Connection Config Tests
// ============================================================================

mod node_config_tests {
    use voltdb_client_rust::*;

    #[test]
    fn test_ip_port_new() {
        let _ip_port = IpPort::new("localhost".to_string(), 21211);
        // Just verify it can be created
        assert!(true);
    }

    #[test]
    fn test_opts_new() {
        let hosts = vec![
            IpPort::new("localhost".to_string(), 21211),
            IpPort::new("192.168.1.1".to_string(), 21212),
        ];
        let _opts = Opts::new(hosts);
        // Verify opts can be created with multiple hosts
        assert!(true);
    }

    #[test]
    fn test_node_opt() {
        let opt = NodeOpt {
            ip_port: IpPort::new("localhost".to_string(), 21211),
            user: Some("admin".to_string()),
            pass: Some("password".to_string()),
        };
        assert!(opt.user.is_some());
        assert!(opt.pass.is_some());
    }

    #[test]
    fn test_node_opt_no_auth() {
        let opt = NodeOpt {
            ip_port: IpPort::new("localhost".to_string(), 21211),
            user: None,
            pass: None,
        };
        assert!(opt.user.is_none());
        assert!(opt.pass.is_none());
    }
}

// ============================================================================
// Error Type Tests
// ============================================================================

mod error_tests {
    use voltdb_client_rust::VoltError;

    #[test]
    fn test_volt_error_display() {
        let err = VoltError::AuthFailed;
        let display = format!("{}", err);
        assert!(display.contains("Auth"));

        let err = VoltError::ConnectionNotAvailable;
        let display = format!("{}", err);
        assert!(display.contains("Connection"));

        let err = VoltError::InvalidConfig;
        let display = format!("{}", err);
        assert!(display.contains("Invalid"));
    }

    #[test]
    fn test_volt_error_no_value() {
        let err = VoltError::NoValue("test_column".to_string());
        let display = format!("{}", err);
        assert!(display.contains("test_column"));
    }

    #[test]
    fn test_volt_error_invalid_column_type() {
        let err = VoltError::InvalidColumnType(99);
        let display = format!("{}", err);
        assert!(display.contains("99"));
    }
}

// ============================================================================
// Datetime Tests
// ============================================================================

mod datetime_tests {
    use bytebuffer::ByteBuffer;
    use chrono::{TimeZone, Utc};
    use voltdb_client_rust::*;

    #[test]
    fn test_datetime_marshal() {
        let dt = Utc.with_ymd_and_hms(2023, 6, 15, 12, 30, 0).unwrap();
        let mut buffer = ByteBuffer::new();
        dt.marshal(&mut buffer);
        assert_eq!(dt.get_write_length(), 9);
        assert_eq!(buffer.as_bytes()[0], TIMESTAMP_COLUMN as u8);
    }

    #[test]
    fn test_option_datetime_none() {
        let val: Option<DateTime<Utc>> = None;
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], TIMESTAMP_COLUMN as u8);
        assert_eq!(&bytes[1..9], &NULL_TIMESTAMP);
    }

    #[test]
    fn test_option_datetime_some() {
        let dt = Utc.with_ymd_and_hms(2023, 6, 15, 12, 30, 0).unwrap();
        let val: Option<DateTime<Utc>> = Some(dt);
        let mut buffer = ByteBuffer::new();
        val.marshal(&mut buffer);
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], TIMESTAMP_COLUMN as u8);
        // Should not be NULL_TIMESTAMP
        assert_ne!(&bytes[1..9], &NULL_TIMESTAMP);
    }
}

// ============================================================================
// Value trait from_bytes tests
// ============================================================================

mod from_bytes_tests {
    use voltdb_client_rust::*;

    fn dummy_column() -> Column {
        Column {
            header_name: "test".to_string(),
            header_type: INT_COLUMN,
        }
    }

    #[test]
    fn test_i8_from_bytes() {
        let bytes = vec![42u8];
        let col = dummy_column();
        let result: i8 = i8::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_i16_from_bytes() {
        let bytes = vec![0, 100]; // 100 in big-endian
        let col = dummy_column();
        let result: i16 = i16::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, 100);
    }

    #[test]
    fn test_i32_from_bytes() {
        let bytes = vec![0, 0, 1, 0]; // 256 in big-endian
        let col = dummy_column();
        let result: i32 = i32::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, 256);
    }

    #[test]
    fn test_i64_from_bytes() {
        let bytes = vec![0, 0, 0, 0, 0, 0, 1, 0]; // 256 in big-endian
        let col = dummy_column();
        let result: i64 = i64::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, 256);
    }

    #[test]
    fn test_option_i8_from_bytes_null() {
        let bytes = NULL_BIT_VALUE.to_vec();
        let col = dummy_column();
        let result: Option<i8> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_option_i16_from_bytes_null() {
        let bytes = NULL_SHORT_VALUE.to_vec();
        let col = dummy_column();
        let result: Option<i16> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_option_i32_from_bytes_null() {
        let bytes = NULL_INT_VALUE.to_vec();
        let col = dummy_column();
        let result: Option<i32> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_option_i64_from_bytes_null() {
        let bytes = NULL_LONG_VALUE.to_vec();
        let col = dummy_column();
        let result: Option<i64> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_option_f64_from_bytes_null() {
        let bytes = NULL_FLOAT_VALUE.to_vec();
        let col = dummy_column();
        let result: Option<f64> = Option::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_bool_from_bytes_true() {
        let bytes = vec![1u8];
        let col = dummy_column();
        let result: bool = bool::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, true);
    }

    #[test]
    fn test_bool_from_bytes_false() {
        let bytes = vec![0u8];
        let col = dummy_column();
        let result: bool = bool::from_bytes(bytes, &col).unwrap();
        assert_eq!(result, false);
    }
}

// ============================================================================
// Constants Tests
// ============================================================================

mod constants_tests {
    use voltdb_client_rust::*;

    #[test]
    fn test_column_type_constants() {
        assert_eq!(TINYINT_COLUMN, 3);
        assert_eq!(SHORT_COLUMN, 4);
        assert_eq!(INT_COLUMN, 5);
        assert_eq!(LONG_COLUMN, 6);
        assert_eq!(FLOAT_COLUMN, 8);
        assert_eq!(STRING_COLUMN, 9);
        assert_eq!(TIMESTAMP_COLUMN, 11);
        assert_eq!(DECIMAL_COLUMN, 22);
        assert_eq!(VAR_BIN_COLUMN, 25);
    }

    #[test]
    fn test_null_values() {
        assert_eq!(NULL_BIT_VALUE.len(), 1);
        assert_eq!(NULL_SHORT_VALUE.len(), 2);
        assert_eq!(NULL_INT_VALUE.len(), 4);
        assert_eq!(NULL_LONG_VALUE.len(), 8);
        assert_eq!(NULL_FLOAT_VALUE.len(), 8);
        assert_eq!(NULL_VARCHAR.len(), 4);
        assert_eq!(NULL_DECIMAL.len(), 16);
        assert_eq!(NULL_TIMESTAMP.len(), 8);
    }
}
