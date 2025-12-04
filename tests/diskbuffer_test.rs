use hive_otel_trace_collector::{BufferConfig, BufferedBatch, MmapRingBuffer, Span};
use tempfile::TempDir;

fn create_test_buffer() -> (MmapRingBuffer, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = BufferConfig {
        dir: temp_dir.path().to_path_buf(),
        max_size: 100 * 1024 * 1024,
        segment_size: 1024 * 1024,
    };
    let buffer = MmapRingBuffer::new(config).unwrap();
    (buffer, temp_dir)
}

#[test]
fn test_write_and_read_batch() {
    let (buffer, _temp_dir) = create_test_buffer();

    let batch = BufferedBatch {
        rows: vec![Span::new()],
        created_at_ns: 12345,
    };

    buffer.write_batch(batch.clone()).unwrap();
    assert!(buffer.has_pending());
    assert_eq!(buffer.pending_entries(), 1);

    let read_batch = buffer.read_batch().unwrap().unwrap();
    assert_eq!(read_batch.rows.len(), 1);
    assert_eq!(read_batch.created_at_ns, 12345);
}

#[test]
fn test_empty_buffer() {
    let (buffer, _temp_dir) = create_test_buffer();
    assert!(!buffer.has_pending());
    assert!(buffer.read_batch().unwrap().is_none());
}
