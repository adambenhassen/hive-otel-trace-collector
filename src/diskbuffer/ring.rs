use crate::proto::span::Span;
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, error, info, warn};

const MAGIC: u32 = 0x48495645; // "HIVE"
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 64;
const ENTRY_HEADER_SIZE: usize = 16;

#[derive(Error, Debug)]
pub enum BufferError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Buffer full")]
    BufferFull,
}

#[derive(Debug, Clone)]
pub struct BufferConfig {
    pub dir: PathBuf,
    pub max_size: u64,
    pub segment_size: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/var/lib/otel-collector/buffer"),
            max_size: 10 * 1024 * 1024 * 1024, // 10GB
            segment_size: 64 * 1024 * 1024,    // 64MB
        }
    }
}

impl BufferConfig {
    pub fn from_env() -> Self {
        let dir = std::env::var("DISK_BUFFER_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/otel-collector/buffer"));

        let max_size = match std::env::var("DISK_BUFFER_MAX_SIZE_MB") {
            Ok(s) => match s.parse::<u64>() {
                Ok(mb) => mb * 1024 * 1024,
                Err(_) => {
                    warn!(value = %s, "Invalid DISK_BUFFER_MAX_SIZE_MB, using default 1024 MB");
                    1024 * 1024 * 1024
                }
            },
            Err(_) => 1024 * 1024 * 1024, // 1GB default
        };

        let segment_size = match std::env::var("DISK_BUFFER_SEGMENT_SIZE_MB") {
            Ok(s) => match s.parse::<usize>() {
                Ok(mb) => mb * 1024 * 1024,
                Err(_) => {
                    warn!(value = %s, "Invalid DISK_BUFFER_SEGMENT_SIZE_MB, using default 64 MB");
                    64 * 1024 * 1024
                }
            },
            Err(_) => 64 * 1024 * 1024,
        };

        Self { dir, max_size, segment_size }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BufferedBatch {
    pub rows: Vec<Span>,
    pub created_at_ns: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct SegmentHeader {
    magic: u32,
    version: u32,
    write_offset: u64,
    read_offset: u64,
    entry_count: u64,
    _reserved: [u8; 32],
}

impl SegmentHeader {
    fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            write_offset: HEADER_SIZE as u64,
            read_offset: HEADER_SIZE as u64,
            entry_count: 0,
            _reserved: [0; 32],
        }
    }

    fn is_valid(&self) -> bool {
        self.magic == MAGIC && self.version == VERSION
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct EntryHeader {
    length: u32,
    crc: u32,
    batch_count: u32,
    _reserved: u32,
}

pub struct MmapRingBuffer {
    config: BufferConfig,
    current_segment: Mutex<Option<ActiveSegment>>,
    read_lock: Mutex<()>,
    total_bytes: AtomicU64,
    total_entries: AtomicU64,
}

struct ActiveSegment {
    #[allow(dead_code)] // Kept to hold fd open while mmap is active
    file: File,
    mmap: MmapMut,
    path: PathBuf,
    write_offset: usize,
    entry_count: usize,
}

impl MmapRingBuffer {
    pub fn new(config: BufferConfig) -> Result<Self, BufferError> {
        std::fs::create_dir_all(&config.dir)?;

        let buffer = Self {
            config,
            current_segment: Mutex::new(None),
            read_lock: Mutex::new(()),
            total_bytes: AtomicU64::new(0),
            total_entries: AtomicU64::new(0),
        };

        buffer.recover()?;
        Ok(buffer)
    }

    /// Recover state from existing segment files
    fn recover(&self) -> Result<(), BufferError> {
        let (total_bytes, total_entries, segments_ok, segments_skipped) =
            self.scan_segments()?;

        if segments_skipped > 0 {
            warn!(
                skipped = segments_skipped,
                ok = segments_ok,
                "Recovery completed with errors"
            );
        }

        self.total_bytes.store(total_bytes, Ordering::SeqCst);
        self.total_entries.store(total_entries, Ordering::SeqCst);

        if total_bytes > 0 {
            debug!(
                bytes = total_bytes,
                entries = total_entries,
                "Recovered disk buffer state"
            );
        }

        Ok(())
    }

    /// Scan segments and count only valid unread entries
    fn scan_segments(&self) -> Result<(u64, u64, u64, u64), BufferError> {
        let entries = std::fs::read_dir(&self.config.dir)?;
        let mut total_bytes = 0u64;
        let mut total_entries = 0u64;
        let mut segments_ok = 0u64;
        let mut segments_skipped = 0u64;

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to read directory entry during recovery, skipping");
                    segments_skipped += 1;
                    continue;
                }
            };

            let path = entry.path();
            if path.extension().map(|e| e == "seg").unwrap_or(false) {
                match File::open(&path) {
                    Ok(file) => {
                        match unsafe { MmapOptions::new().map(&file) } {
                            Ok(mmap) => {
                                if mmap.len() >= HEADER_SIZE {
                                    let header =
                                        unsafe { std::ptr::read(mmap.as_ptr() as *const SegmentHeader) };
                                    if header.is_valid() {
                                        // Count actual unread entries by walking the segment
                                        let (entries, bytes) =
                                            Self::count_unread_entries(&mmap, &header);
                                        total_entries += entries;
                                        total_bytes += bytes;
                                        segments_ok += 1;
                                    } else {
                                        warn!(path = %path.display(), "Invalid segment header during recovery");
                                        segments_skipped += 1;
                                    }
                                } else {
                                    warn!(path = %path.display(), "Segment file too small during recovery");
                                    segments_skipped += 1;
                                }
                            }
                            Err(e) => {
                                warn!(path = %path.display(), error = %e, "Failed to mmap segment during recovery, skipping");
                                segments_skipped += 1;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "Failed to open segment during recovery, skipping");
                        segments_skipped += 1;
                    }
                }
            }
        }

        Ok((total_bytes, total_entries, segments_ok, segments_skipped))
    }

    /// Count unread valid entries in a segment by walking from read_offset to write_offset
    fn count_unread_entries(mmap: &memmap2::Mmap, header: &SegmentHeader) -> (u64, u64) {
        let mut offset = header.read_offset as usize;
        let write_offset = header.write_offset as usize;
        let mut entries = 0u64;
        let mut bytes = 0u64;

        while offset + ENTRY_HEADER_SIZE <= write_offset && offset + ENTRY_HEADER_SIZE <= mmap.len()
        {
            let entry_header =
                unsafe { std::ptr::read(mmap[offset..].as_ptr() as *const EntryHeader) };

            let data_end = offset + ENTRY_HEADER_SIZE + entry_header.length as usize;
            if data_end > write_offset || data_end > mmap.len() {
                break; // Truncated entry
            }

            // Verify CRC - only count valid entries
            let data = &mmap[offset + ENTRY_HEADER_SIZE..data_end];
            let crc = crc32fast::hash(data);
            if crc == entry_header.crc {
                entries += 1;
                bytes += (ENTRY_HEADER_SIZE + entry_header.length as usize) as u64;
            }

            offset = data_end;
        }

        (entries, bytes)
    }

    /// Recompute counters by scanning all segments (call when mismatch detected)
    pub fn recompute_counters(&self) {
        match self.scan_segments() {
            Ok((bytes, entries, _, _)) => {
                let old_entries = self.total_entries.swap(entries, Ordering::SeqCst);
                let old_bytes = self.total_bytes.swap(bytes, Ordering::SeqCst);
                if old_entries != entries || old_bytes != bytes {
                    warn!(
                        old_entries,
                        new_entries = entries,
                        old_bytes,
                        new_bytes = bytes,
                        "Recomputed disk buffer counters"
                    );
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to recompute disk buffer counters");
            }
        }
    }

    fn get_or_create_segment(&self) -> Result<(), BufferError> {
        let mut guard = self.current_segment.lock().unwrap();

        if guard.is_none() || self.needs_rotation(&guard) {
            // Create new segment
            let segment_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = self.config.dir.join(format!("{}.seg", segment_id));

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;

            file.set_len(self.config.segment_size as u64)?;

            let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

            // Write header
            let header = SegmentHeader::new();
            unsafe {
                std::ptr::write(mmap.as_mut_ptr() as *mut SegmentHeader, header);
            }
            mmap.flush()?;

            debug!(path = %path.display(), "Created new buffer segment");

            *guard = Some(ActiveSegment {
                file,
                mmap,
                path,
                write_offset: HEADER_SIZE,
                entry_count: 0,
            });
        }

        Ok(())
    }

    fn needs_rotation(&self, segment: &Option<ActiveSegment>) -> bool {
        segment
            .as_ref()
            .map(|s| s.write_offset + 1024 * 1024 > self.config.segment_size) // 1MB margin
            .unwrap_or(true)
    }

    pub fn write_batch(&self, batch: BufferedBatch) -> Result<(), BufferError> {
        if self.total_bytes.load(Ordering::Relaxed) >= self.config.max_size {
            return Err(BufferError::BufferFull);
        }

        let data =
            bincode::serialize(&batch).map_err(|e| BufferError::Serialization(e.to_string()))?;

        let entry_size = ENTRY_HEADER_SIZE + data.len();

        self.get_or_create_segment()?;

        let mut guard = self.current_segment.lock().unwrap();
        let segment = guard.as_mut().ok_or(BufferError::Io(io::Error::new(
            io::ErrorKind::Other,
            "No active segment",
        )))?;

        // Check if entry fits in current segment
        if segment.write_offset + entry_size > self.config.segment_size {
            drop(guard);
            // Force rotation by clearing current segment
            self.current_segment.lock().unwrap().take();
            self.get_or_create_segment()?;
            guard = self.current_segment.lock().unwrap();
        }

        let segment = guard.as_mut().ok_or(BufferError::Io(io::Error::new(
            io::ErrorKind::Other,
            "No active segment after rotation",
        )))?;

        // Write entry header
        let crc = crc32fast::hash(&data);
        let entry_header = EntryHeader {
            length: data.len() as u32,
            crc,
            batch_count: batch.rows.len() as u32,
            _reserved: 0,
        };

        let header_ptr = segment.mmap[segment.write_offset..].as_mut_ptr() as *mut EntryHeader;
        unsafe {
            std::ptr::write(header_ptr, entry_header);
        }

        // Write data
        let data_offset = segment.write_offset + ENTRY_HEADER_SIZE;
        segment.mmap[data_offset..data_offset + data.len()].copy_from_slice(&data);

        segment.write_offset += entry_size;
        segment.entry_count += 1;

        // Update header
        let header = unsafe { &mut *(segment.mmap.as_mut_ptr() as *mut SegmentHeader) };
        header.write_offset = segment.write_offset as u64;
        header.entry_count = segment.entry_count as u64;

        // Async flush (don't block)
        if let Err(e) = segment.mmap.flush_async() {
            warn!(error = %e, "Async mmap flush failed, data may not be durable");
        }

        self.total_bytes
            .fetch_add(entry_size as u64, Ordering::Relaxed);
        self.total_entries.fetch_add(1, Ordering::Relaxed);

        debug!(
            bytes = entry_size,
            rows = batch.rows.len(),
            "Wrote batch to disk buffer"
        );

        Ok(())
    }

    /// Read the next batch from the buffer (for draining)
    pub fn read_batch(&self) -> Result<Option<BufferedBatch>, BufferError> {
        // Serialize reads to prevent duplicate processing by multiple drain workers
        let _read_guard = self.read_lock.lock().unwrap();

        // Get current segment path to skip it (avoid race with writer)
        let current_segment_path = self
            .current_segment
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.path.clone());

        // Find segments with unread data
        let segments: Vec<PathBuf> = std::fs::read_dir(&self.config.dir)?
            .flatten()
            .filter_map(|e| {
                let path = e.path();
                if path.extension().map(|e| e == "seg").unwrap_or(false) {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        for path in segments {
            // Skip the active segment being written to
            if current_segment_path.as_ref() == Some(&path) {
                continue;
            }
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

            if mmap.len() < HEADER_SIZE {
                continue;
            }

            let header = unsafe { std::ptr::read(mmap.as_ptr() as *const SegmentHeader) };

            if !header.is_valid() {
                warn!(path = %path.display(), "Invalid segment header, removing");
                std::fs::remove_file(&path)?;
                continue;
            }

            let read_offset = header.read_offset as usize;
            let write_offset = header.write_offset as usize;

            if read_offset >= write_offset {
                // Segment fully read, remove it
                debug!(path = %path.display(), "Segment fully read, removing");
                drop(mmap);
                drop(file);
                std::fs::remove_file(&path)?;
                continue;
            }

            // Read entry header
            if read_offset + ENTRY_HEADER_SIZE > mmap.len() {
                warn!(
                    path = %path.display(),
                    read_offset,
                    mmap_len = mmap.len(),
                    "Entry header would exceed segment bounds, skipping"
                );
                continue;
            }

            let entry_header =
                unsafe { std::ptr::read(mmap[read_offset..].as_ptr() as *const EntryHeader) };

            let data_offset = read_offset + ENTRY_HEADER_SIZE;
            let data_end = data_offset + entry_header.length as usize;

            if data_end > mmap.len() {
                warn!(path = %path.display(), "Truncated entry, marking segment as read");
                // Mark entire segment as read to skip it
                let header_mut = unsafe { &mut *(mmap.as_mut_ptr() as *mut SegmentHeader) };
                header_mut.read_offset = header_mut.write_offset;
                mmap.flush()?;
                continue;
            }

            let data = &mmap[data_offset..data_end];

            // verify CRC
            let crc = crc32fast::hash(data);
            if crc != entry_header.crc {
                let entry_size = ENTRY_HEADER_SIZE + entry_header.length as usize;
                warn!(
                    path = %path.display(),
                    expected = entry_header.crc,
                    actual = crc,
                    entry_size,
                    "CRC mismatch, skipping entry"
                );
                // Update read offset to skip corrupted entry
                let new_read_offset = data_end;
                let header_mut = unsafe { &mut *(mmap.as_mut_ptr() as *mut SegmentHeader) };
                header_mut.read_offset = new_read_offset as u64;
                mmap.flush()?;
                // Note: Don't decrement counters - recovery excludes invalid CRC entries
                continue;
            }

            // Deserialize batch
            let batch: BufferedBatch = bincode::deserialize(data)
                .map_err(|e| BufferError::Serialization(e.to_string()))?;

            // Update read offset
            let new_read_offset = data_end;
            let header_mut = unsafe { &mut *(mmap.as_mut_ptr() as *mut SegmentHeader) };
            header_mut.read_offset = new_read_offset as u64;
            mmap.flush()?;

            let entry_size = ENTRY_HEADER_SIZE + entry_header.length as usize;
            self.total_bytes
                .fetch_sub(entry_size as u64, Ordering::Relaxed);
            self.total_entries.fetch_sub(1, Ordering::Relaxed);

            debug!(rows = batch.rows.len(), "Read batch from disk buffer");

            return Ok(Some(batch));
        }

        Ok(None)
    }

    pub fn has_pending(&self) -> bool {
        self.total_entries.load(Ordering::Relaxed) > 0
    }

    pub fn pending_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn pending_entries(&self) -> u64 {
        self.total_entries.load(Ordering::Relaxed)
    }

    // /// Compact: remove fully-read segments
    // pub fn compact(&self) -> Result<usize, BufferError> {
    //     let mut removed = 0;
    //     let entries = std::fs::read_dir(&self.config.dir)?;

    //     for entry in entries.flatten() {
    //         let path = entry.path();
    //         if path.extension().map(|e| e == "seg").unwrap_or(false) {
    //             if let Ok(file) = File::open(&path) {
    //                 if let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
    //                     if mmap.len() >= HEADER_SIZE {
    //                         let header =
    //                             unsafe { std::ptr::read(mmap.as_ptr() as *const SegmentHeader) };
    //                         if header.is_valid() && header.read_offset >= header.write_offset {
    //                             drop(mmap);
    //                             drop(file);
    //                             if std::fs::remove_file(&path).is_ok() {
    //                                 removed += 1;
    //                                 info!(path = %path.display(), "Removed empty segment");
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     Ok(removed)
    // }
}

