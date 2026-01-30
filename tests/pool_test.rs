use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime};

use testcontainers::core::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use voltdb_client_rust::*;

#[allow(dead_code)]
static SETUP: Once = Once::new();
static mut VOLTDB_PORT: u16 = 0;
static TEST_MUTEX: Mutex<()> = Mutex::new(());
static VOLTDB_CONTAINER: OnceLock<Container<GenericImage>> = OnceLock::new();
static CLEANUP_REGISTERED: OnceLock<()> = OnceLock::new();
fn setup_voltdb_once() {
    VOLTDB_CONTAINER.get_or_init(|| {
        // Register cleanup handler once
        CLEANUP_REGISTERED.get_or_init(|| {
            extern "C" fn cleanup() {
                // Force stop the container
                if let Some(container) = VOLTDB_CONTAINER.get() {
                    container.stop().unwrap();
                }
            }
            // SAFETY: registering atexit callback is safe
            #[allow(unused_unsafe)]
            unsafe {
                libc::atexit(cleanup);
            }
        });

        let voltdb = GenericImage::new("basvanbeek/voltdb-community", "9.2.1")
            .with_wait_for(WaitFor::message_on_stdout(
                "Server completed initialization.",
            ))
            .with_env_var("HOST_COUNT", "1");
        let docker = voltdb.start().unwrap();
        let host_port = docker.get_host_port_ipv4(21211).unwrap();
        unsafe {
            VOLTDB_PORT = host_port;
        }
        docker
    });
}
fn get_opts() -> Opts {
    setup_voltdb_once();
    let port = unsafe { VOLTDB_PORT };
    let host_ip = IpPort::new("localhost".to_string(), port);
    Opts::new(vec![host_ip])
}

// ============================================================================
// Basic Pool Creation & Configuration Tests
// ============================================================================

#[test]
fn test_pool_creation_default() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new(get_opts())?;

    let stats = pool.stats();
    assert_eq!(stats.size, 10);
    assert_eq!(stats.healthy, 10);
    assert!(!stats.is_shutdown);

    Ok(())
}

#[test]
fn test_pool_creation_custom_size() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;

    let stats = pool.stats();
    assert_eq!(stats.size, 5);
    assert_eq!(stats.healthy, 5);

    Ok(())
}

#[test]
fn test_pool_creation_with_config() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(3)
        .reconnect_backoff(Duration::from_secs(1))
        .circuit_open_duration(Duration::from_secs(10));

    let pool = Pool::with_config(get_opts(), config)?;

    let stats = pool.stats();
    assert_eq!(stats.size, 3);
    assert_eq!(stats.healthy, 3);

    Ok(())
}

#[test]
fn test_pool_config_builder() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(15)
        .reconnect_backoff(Duration::from_secs(2))
        .circuit_open_duration(Duration::from_secs(20))
        .exhaustion_policy(ExhaustionPolicy::Block {
            timeout: Duration::from_secs(3),
        })
        .validation_mode(ValidationMode::BestEffort)
        .circuit_failure_threshold(5)
        .shutdown_timeout(Duration::from_secs(45));

    assert_eq!(config.size, 15);
    assert_eq!(config.reconnect_backoff, Duration::from_secs(2));
    assert_eq!(config.circuit_open_duration, Duration::from_secs(20));
    assert_eq!(config.circuit_failure_threshold, 5);
    assert_eq!(config.shutdown_timeout, Duration::from_secs(45));
}

// ============================================================================
// Connection Acquisition Tests
// ============================================================================

#[test]
fn test_get_single_connection() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;

    let mut conn = pool.get_conn()?;
    let table = conn.list_procedures()?;
    assert!(table.get_row_count() >= 0);

    Ok(())
}

#[test]
fn test_get_multiple_connections_sequential() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    for i in 0..10 {
        let mut conn = pool.get_conn()?;
        let table = conn.list_procedures()?;
        assert!(table.get_row_count() >= 0, "iteration {}", i);
    }

    Ok(())
}

#[test]
fn test_connection_reuse() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(2, get_opts())?;

    // Get connection and use it
    {
        let mut conn = pool.get_conn()?;
        conn.list_procedures()?;
    }

    // Get another connection - should work (connection was returned)
    {
        let mut conn = pool.get_conn()?;
        conn.list_procedures()?;
    }

    let stats = pool.stats();
    assert_eq!(stats.healthy, 2);

    Ok(())
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_concurrent_connections_low() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Arc::new(Pool::new_manual(5, get_opts())?);
    let mut handles = vec![];

    for _ in 0..10 {
        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            let mut conn = pool_clone.get_conn().unwrap();
            conn.list_procedures().unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = pool.stats();
    assert_eq!(stats.healthy, 5);
    assert_eq!(stats.total_requests, 10);

    Ok(())
}

#[test]
fn test_concurrent_connections_high() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Arc::new(Pool::new_manual(10, get_opts())?);
    let mut handles = vec![];
    let start = SystemTime::now();

    for _ in 0..512 {
        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            let mut conn = pool_clone.get_conn().unwrap();
            let mut table = conn.list_procedures().unwrap();
            table.advance_row();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = SystemTime::now()
        .duration_since(start)
        .expect("Time went backwards");
    println!("512 concurrent requests took: {:?}", duration);

    let stats = pool.stats();
    assert_eq!(stats.size, 10);
    assert_eq!(stats.healthy, 10);
    assert_eq!(stats.total_requests, 512);

    Ok(())
}

#[test]
fn test_concurrent_mixed_operations() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Arc::new(Pool::new_manual(8, get_opts())?);
    let mut handles = vec![];
    let success_count = Arc::new(AtomicUsize::new(0));

    for i in 0..100 {
        let pool_clone = Arc::clone(&pool);
        let counter = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let mut conn = pool_clone.get_conn().unwrap();

            // Alternate between different operations
            let result = if i % 2 == 0 {
                conn.list_procedures()
            } else {
                conn.call_sp("@GetPartitionKeys", volt_param!("integer"))
            };
            match result {
                Ok(_) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => panic!("{:?}", e),
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(success_count.load(Ordering::Relaxed), 100);

    Ok(())
}

// ============================================================================
// Query Execution Tests
// ============================================================================

#[test]
fn test_pool_query_execution() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    let mut conn = pool.get_conn()?;
    let table = conn.call_sp("@GetPartitionKeys", volt_param!("integer"))?;
    assert!(table.get_row_count() > 0);

    Ok(())
}

#[test]
fn test_pool_list_procedures() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    let mut conn = pool.get_conn()?;
    let table = conn.list_procedures()?;

    assert!(table.get_row_count() >= 0);

    Ok(())
}

#[test]
fn test_pool_multiple_queries_same_connection() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(2, get_opts())?;

    let mut conn = pool.get_conn()?;

    for _ in 0..5 {
        let table = conn.call_sp("@GetPartitionKeys", volt_param!("integer"))?;
        assert!(table.get_row_count() > 0);
    }

    Ok(())
}

#[test]
fn test_pool_connection_slot_index() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;

    let conn = pool.get_conn()?;
    let idx = conn.slot_index();

    assert!(idx < 5, "Slot index should be within pool size");

    Ok(())
}

// ============================================================================
// Shutdown Tests
// ============================================================================

#[test]
fn test_pool_shutdown() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    assert!(!pool.is_shutdown());

    pool.shutdown();

    assert!(pool.is_shutdown());

    let stats = pool.stats();
    assert!(stats.is_shutdown);
    assert_eq!(stats.healthy, 0);

    Ok(())
}

#[test]
fn test_pool_shutdown_rejects_new_connections() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    pool.shutdown();

    let result = pool.get_conn();
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_pool_shutdown_graceful() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Arc::new(Pool::new_manual(5, get_opts())?);

    // Start some operations
    let pool_clone = Arc::clone(&pool);
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        pool_clone.shutdown();
    });

    // Try to get connections during shutdown
    thread::sleep(Duration::from_millis(50));
    let conn_result = pool.get_conn();

    handle.join().unwrap();

    // Either got a connection before shutdown, or got shutdown error
    if let Err(e) = conn_result {
        assert!(matches!(e, VoltError::ConnectionNotAvailable));
    }

    assert!(pool.is_shutdown());

    Ok(())
}

// ============================================================================
// Exhaustion Policy Tests
// ============================================================================

#[test]
fn test_exhaustion_policy_fail_fast() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(2)
        .exhaustion_policy(ExhaustionPolicy::FailFast);

    let pool = Pool::with_config(get_opts(), config)?;

    // Should be able to get connections
    let _conn1 = pool.get_conn()?;
    let _conn2 = pool.get_conn()?;

    // Note: VoltDB connections are shared, so this will succeed
    // This test just verifies the pool works with FailFast policy
    let _conn3 = pool.get_conn()?;

    Ok(())
}

#[test]
fn test_exhaustion_policy_block() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(2)
        .exhaustion_policy(ExhaustionPolicy::Block {
            timeout: Duration::from_secs(1),
        });

    let pool = Pool::with_config(get_opts(), config)?;

    let mut conn = pool.get_conn()?;
    conn.list_procedures()?;

    Ok(())
}

// ============================================================================
// Stats and Monitoring Tests
// ============================================================================

#[test]
fn test_pool_stats_initial() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(7, get_opts())?;

    let stats = pool.stats();

    assert_eq!(stats.size, 7);
    assert_eq!(stats.healthy, 7);
    assert_eq!(stats.total_requests, 0);
    assert!(!stats.is_shutdown);

    Ok(())
}

#[test]
fn test_pool_stats_after_requests() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;

    for _ in 0..20 {
        let mut conn = pool.get_conn()?;
        conn.list_procedures()?;
    }

    let stats = pool.stats();
    assert_eq!(stats.size, 5);
    assert_eq!(stats.healthy, 5);
    assert_eq!(stats.total_requests, 20);

    Ok(())
}

#[test]
fn test_pool_stats_debug_format() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    let debug_str = format!("{:?}", pool);
    assert!(debug_str.contains("Pool"));

    Ok(())
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_pool_invalid_query() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    let mut conn = pool.get_conn()?;
    let result = conn.query("INVALID SQL SYNTAX");

    // Should return an error, not panic
    assert!(result.is_err());

    // Pool should still be healthy
    let stats = pool.stats();
    assert_eq!(stats.healthy, 3);

    Ok(())
}

#[test]
fn test_pooled_conn_debug_format() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(3, get_opts())?;

    let conn = pool.get_conn()?;
    let debug_str = format!("{:?}", conn);
    assert!(debug_str.contains("PooledConn"));
    assert!(debug_str.contains("idx"));

    Ok(())
}

// ============================================================================
// Load Testing
// ============================================================================

#[test]
fn test_pool_sustained_load() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Arc::new(Pool::new_manual(10, get_opts())?);
    let mut handles = vec![];
    let iterations = 100;

    for _ in 0..iterations {
        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            for _ in 0..5 {
                let mut conn = pool_clone.get_conn().unwrap();
                conn.list_procedures().unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = pool.stats();
    assert_eq!(stats.healthy, 10);
    assert_eq!(stats.total_requests, iterations * 5);

    Ok(())
}

#[test]
fn test_pool_rapid_acquire_release() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;

    for _ in 0..1000 {
        let _conn = pool.get_conn()?;
        // Connection automatically "returned" when dropped
    }

    let stats = pool.stats();
    assert_eq!(stats.healthy, 5);
    assert_eq!(stats.total_requests, 1000);

    Ok(())
}

// ============================================================================
// Validation Mode Tests
// ============================================================================

#[test]
fn test_validation_mode_fail_fast_success() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(3)
        .validation_mode(ValidationMode::FailFast);

    let pool = Pool::with_config(get_opts(), config)?;

    let stats = pool.stats();
    assert_eq!(stats.healthy, 3);

    Ok(())
}

#[test]
fn test_validation_mode_best_effort() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new()
        .size(3)
        .validation_mode(ValidationMode::BestEffort);

    let pool = Pool::with_config(get_opts(), config)?;

    let stats = pool.stats();
    // With valid connection, all should be healthy
    assert!(stats.healthy > 0);

    Ok(())
}

// ============================================================================
// Round-Robin Connection Selection Tests
// ============================================================================

#[test]
fn test_connection_distribution() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(5, get_opts())?;
    let mut slot_indices = Vec::new();

    // Get multiple connections and track their slots
    for _ in 0..10 {
        let conn = pool.get_conn()?;
        slot_indices.push(conn.slot_index());
    }

    // Should cycle through slots (round-robin)
    // At least 2 different slots should be used
    slot_indices.sort();
    slot_indices.dedup();
    assert!(
        slot_indices.len() >= 2,
        "Should use multiple connection slots"
    );

    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_pool_size_one() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock()?;
    let pool = Pool::new_manual(1, get_opts())?;

    let mut conn = pool.get_conn()?;
    conn.list_procedures()?;

    let stats = pool.stats();
    assert_eq!(stats.size, 1);
    assert_eq!(stats.healthy, 1);

    Ok(())
}

#[test]
fn test_pool_large_size() -> Result<(), VoltError> {
    let _guard = TEST_MUTEX.lock().unwrap();
    let pool = Pool::new_manual(50, get_opts())?;

    let stats = pool.stats();
    assert_eq!(stats.size, 50);
    assert_eq!(stats.healthy, 50);

    Ok(())
}

// ============================================================================
// Configuration Edge Cases
// ============================================================================

#[test]
fn test_config_zero_backoff() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new().reconnect_backoff(Duration::from_secs(0));

    assert_eq!(config.reconnect_backoff, Duration::from_secs(0));
}

#[test]
fn test_config_very_short_circuit_duration() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new().circuit_open_duration(Duration::from_millis(1));

    assert_eq!(config.circuit_open_duration, Duration::from_millis(1));
}

#[test]
fn test_config_high_failure_threshold() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let config = PoolConfig::new().circuit_failure_threshold(1000);

    assert_eq!(config.circuit_failure_threshold, 1000);
}
