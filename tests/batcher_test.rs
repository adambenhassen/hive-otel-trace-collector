use hive_otel_rust_collector::BatcherConfig;

#[test]
fn test_batcher_config_default() {
    let config = BatcherConfig::default();
    assert_eq!(config.max_batch_size, 10_000);
    assert_eq!(config.channel_capacity, 100_000);
}
