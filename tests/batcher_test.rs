use hive_otel_rust_collector::BatcherConfig;

#[test]
fn test_batcher_config_default() {
    let config = BatcherConfig::default();
    assert_eq!(config.max_batch_size, 10_000);
    assert_eq!(config.worker_count, 4);
    assert!(config.mem_buffer_size_bytes >= 64 * 1024 * 1024, "minimum 64MB");
}
