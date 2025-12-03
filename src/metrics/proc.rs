use lazy_static::lazy_static;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use sysinfo::{Pid, ProcessesToUpdate, System};

static CURRENT_CPU_M: AtomicU64 = AtomicU64::new(0);
static CURRENT_MEM_MB: AtomicU64 = AtomicU64::new(0);

lazy_static! {
    static ref SYSTEM: Mutex<System> = Mutex::new(System::new());
}

pub fn update_process_metrics() {
    let pid = Pid::from_u32(std::process::id());

    if let Ok(mut sys) = SYSTEM.lock() {
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);

        if let Some(process) = sys.process(pid) {
            let mem_mb = process.memory() / 1024 / 1024;
            CURRENT_MEM_MB.store(mem_mb, Ordering::Relaxed);

            // CPU usage as percentage (0-100 per core) -> millicores (0-1000 per core)
            // cpu_usage() returns percentage, multiply by 10 to get millicores
            let cpu_m = (process.cpu_usage() * 10.0) as u64;
            CURRENT_CPU_M.store(cpu_m, Ordering::Relaxed);
        }
    }
}

pub fn get_cpu_m() -> u64 {
    CURRENT_CPU_M.load(Ordering::Relaxed)
}

pub fn get_mem_mb() -> u64 {
    CURRENT_MEM_MB.load(Ordering::Relaxed)
}
