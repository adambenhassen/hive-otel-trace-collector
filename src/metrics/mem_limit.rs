const CGROUP_MEM_BUFFER_PERCENT: usize = 90; // 90% from cgroup
const SYSTEM_MEM_BUFFER_PERCENT: usize = 25; // 25% from system
const MIN_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB
const FALLBACK_BUFFER_SIZE: usize = 128 * 1024 * 1024; // 128MB

#[derive(Debug, Clone, Copy)]
pub enum MemorySource {
    Cgroup(usize),
    System(usize),
}

#[derive(Debug, Clone, Copy)]
pub enum BufferSizeSource {
    Env,
    Cgroup,
    System,
    Fallback,
}

impl std::fmt::Display for BufferSizeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferSizeSource::Env => write!(f, "env"),
            BufferSizeSource::Cgroup => write!(f, "cgroup"),
            BufferSizeSource::System => write!(f, "system"),
            BufferSizeSource::Fallback => write!(f, "fallback"),
        }
    }
}

pub fn default_buffer_size_with_source() -> (usize, BufferSizeSource) {
    match detect_memory_source() {
        Some(MemorySource::Cgroup(limit)) => {
            let size = (limit * CGROUP_MEM_BUFFER_PERCENT) / 100;
            (size.max(MIN_BUFFER_SIZE), BufferSizeSource::Cgroup)
        }
        Some(MemorySource::System(available)) => {
            let size = (available * SYSTEM_MEM_BUFFER_PERCENT) / 100;
            (size.max(MIN_BUFFER_SIZE), BufferSizeSource::System)
        }
        None => (FALLBACK_BUFFER_SIZE, BufferSizeSource::Fallback),
    }
}

pub fn detect_memory_source() -> Option<MemorySource> {
    // Try cgroup v2 first
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max" {
            if let Ok(limit) = trimmed.parse::<usize>() {
                return Some(MemorySource::Cgroup(limit));
            }
        }
    }

    // Try cgroup v1
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(limit) = content.trim().parse::<usize>() {
            // cgroup v1 returns a huge number when unlimited, cap at reasonable value
            if limit < 1024 * 1024 * 1024 * 1024 { // Less than 1TB
                return Some(MemorySource::Cgroup(limit)); 
            }
        }
    }

    // Try /proc/meminfo for total system memory (Linux)
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<usize>() {
                        return Some(MemorySource::System(kb * 1024));
                    }
                }
            }
        }
    }

    None
}
