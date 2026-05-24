/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Tests using a stateful mock SAL implementation
//!
//! These tests use an in-memory storage backend to simulate actual
//! object store operations without needing a real Ceph environment.
//!
//! Run with:
//! ```bash
//! cargo test --features mock-sal stateful_mock
//! ```

use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use std::os::raw::c_void;
use std::sync::Arc;

use lancedb_rgw_store::RGWObjectStore;

/// Create a test store with mock pointers
fn create_test_store(bucket: &str) -> RGWObjectStore {
    create_test_store_with_prefix(bucket, "")
}

fn create_test_store_with_prefix(bucket: &str, prefix: &str) -> RGWObjectStore {
    let fake_driver = 0x1000usize as *mut c_void;
    let fake_dpp = 0x2000usize as *const c_void;
    unsafe { RGWObjectStore::new(fake_driver, fake_dpp, bucket, prefix) }
}

//=============================================================================
// ObjectStore Integration Tests (using mock-sal feature)
//=============================================================================

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_store_basic_operations() {
        let store = create_test_store("integration-bucket");

        // Put operation
        let location = Path::from("test/data.txt");
        let data = Bytes::from("test data content");
        let payload = PutPayload::from(data.clone());

        let put_result = store.put(&location, payload).await;
        assert!(put_result.is_ok(), "Put should succeed");

        // Delete operation (should always succeed with mock)
        let delete_result = store.delete(&location).await;
        assert!(delete_result.is_ok(), "Delete should succeed");
    }

    #[tokio::test]
    async fn test_store_list_operations() {
        let store = create_test_store("list-bucket");

        // List should return empty for mock
        let stream = store.list(None);
        let results: Vec<_> = stream.collect().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_store_copy_operations() {
        let store = create_test_store("copy-bucket");

        let from = Path::from("source.txt");
        let to = Path::from("dest.txt");

        // Copy should succeed with mock
        let result = store.copy(&from, &to).await;
        assert!(result.is_ok());
    }
}

//=============================================================================
// Stress Tests
//=============================================================================

mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_high_concurrency_puts() {
        let store = Arc::new(create_test_store("stress-bucket"));
        let success_count = Arc::new(AtomicUsize::new(0));
        let total_ops = 100;

        let mut join_set = JoinSet::new();

        for i in 0..total_ops {
            let store = store.clone();
            let success_count = success_count.clone();

            join_set.spawn(async move {
                let location = Path::from(format!("stress/file-{}.txt", i));
                let data = Bytes::from(format!("Data content for file number {}", i));
                let payload = PutPayload::from(data);

                if store.put(&location, payload).await.is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        while join_set.join_next().await.is_some() {}

        assert_eq!(
            success_count.load(Ordering::SeqCst),
            total_ops,
            "All puts should succeed"
        );
    }

    #[tokio::test]
    async fn test_mixed_operations_concurrent() {
        let store = Arc::new(create_test_store("mixed-bucket"));
        let mut join_set = JoinSet::new();

        // Mix of puts, deletes, and lists
        for i in 0..50 {
            let store = store.clone();
            join_set.spawn(async move {
                match i % 3 {
                    0 => {
                        // Put
                        let location = Path::from(format!("mixed/put-{}.txt", i));
                        let payload = PutPayload::from(Bytes::from("put data"));
                        let _ = store.put(&location, payload).await;
                    }
                    1 => {
                        // Delete
                        let location = Path::from(format!("mixed/delete-{}.txt", i));
                        let _ = store.delete(&location).await;
                    }
                    _ => {
                        // List
                        let stream = store.list(Some(&Path::from("mixed/")));
                        let _: Vec<_> = stream.collect().await;
                    }
                }
            });
        }

        // All operations should complete without panic
        while join_set.join_next().await.is_some() {}
    }
}

//=============================================================================
// Edge Case Tests
//=============================================================================

mod edge_case_tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_bucket_name() {
        let store = create_test_store("");
        let location = Path::from("test.txt");
        let payload = PutPayload::from(Bytes::from("data"));

        // Should still work (mock doesn't validate bucket names)
        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_very_long_key() {
        let store = create_test_store("long-key-bucket");
        // Create a very long key (1000 characters)
        let key = "a".repeat(1000);
        let location = Path::from(key);
        let payload = PutPayload::from(Bytes::from("data"));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_binary_data() {
        let store = create_test_store("binary-bucket");
        let location = Path::from("binary.bin");

        // Create binary data with all byte values
        let data: Vec<u8> = (0..=255).collect();
        let payload = PutPayload::from(Bytes::from(data));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_null_bytes_in_data() {
        let store = create_test_store("null-bucket");
        let location = Path::from("null-data.bin");

        // Data with embedded null bytes
        let data = Bytes::from(vec![0x00, 0x01, 0x00, 0x02, 0x00]);
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }
}

//=============================================================================
// Performance Characteristics Tests
//=============================================================================

mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_put_latency_consistency() {
        let store = create_test_store("perf-bucket");
        let mut latencies = Vec::new();

        for i in 0..100 {
            let location = Path::from(format!("perf/file-{}.txt", i));
            let data = Bytes::from("test data");
            let payload = PutPayload::from(data);

            let start = Instant::now();
            let _ = store.put(&location, payload).await;
            latencies.push(start.elapsed());
        }

        // With mock SAL, latencies should be very consistent
        let max_latency = latencies.iter().max().unwrap();
        let min_latency = latencies.iter().min().unwrap();

        // Max should be within 100x of min for mock operations
        // Use a floor of 1000ns to avoid division by zero when min_latency is 0
        let min_nanos = min_latency.as_nanos().max(1000);
        assert!(
            max_latency.as_nanos() < min_nanos * 100,
            "Latencies should be relatively consistent: max={:?}, min={:?}",
            max_latency,
            min_latency
        );
    }

    #[tokio::test]
    async fn test_throughput_many_small_objects() {
        let store = create_test_store("throughput-bucket");
        let start = Instant::now();
        let num_objects = 1000;

        for i in 0..num_objects {
            let location = Path::from(format!("throughput/{}.txt", i));
            let data = Bytes::from("small");
            let payload = PutPayload::from(data);
            let _ = store.put(&location, payload).await;
        }

        let elapsed = start.elapsed();
        let ops_per_sec = num_objects as f64 / elapsed.as_secs_f64();

        // Mock should achieve reasonable throughput, but use a low threshold
        // to avoid flaky failures in CI/debug builds. The main goal is to
        // verify the test completes in a reasonable time (< 60 seconds).
        assert!(
            elapsed.as_secs() < 60,
            "Should complete {} ops in under 60 seconds, took {:?} ({:.1} ops/sec)",
            num_objects,
            elapsed,
            ops_per_sec
        );
    }
}
