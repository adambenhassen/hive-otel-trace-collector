use axum::{
    extract::Query,
    http::{header, StatusCode},
    response::IntoResponse,
};
use pprof::protos::Message;
use serde::Deserialize;
use std::time::Duration;
use tikv_jemalloc_ctl::{epoch, stats};

#[derive(Deserialize)]
pub struct ProfileParams {
    #[serde(default = "default_seconds")]
    seconds: u64, // profile duration in seconds (default: 10)
    #[serde(default = "default_frequency")]
    frequency: i32, // sampling frequency in Hz (default: 99)
}

fn default_seconds() -> u64 {
    10
}

fn default_frequency() -> i32 {
    99
}

pub async fn profile_handler(Query(params): Query<ProfileParams>) -> impl IntoResponse {
    let seconds = params.seconds.min(60); // Cap at 60 seconds
    let frequency = params.frequency.clamp(1, 1000);

    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to start profiler: {}", e),
            )
                .into_response();
        }
    };

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to build report: {}", e),
            )
                .into_response();
        }
    };

    let profile = match report.pprof() {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to generate pprof: {}", e),
            )
                .into_response();
        }
    };

    let body = profile.encode_to_vec();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

pub async fn flamegraph_handler(Query(params): Query<ProfileParams>) -> impl IntoResponse {
    let seconds = params.seconds.min(60);
    let frequency = params.frequency.clamp(1, 1000);

    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to start profiler: {}", e),
            )
                .into_response();
        }
    };

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to build report: {}", e),
            )
                .into_response();
        }
    };

    let mut body = Vec::new();
    if let Err(e) = report.flamegraph(&mut body) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to generate flamegraph: {}", e),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        body,
    )
        .into_response()
}

pub async fn heap_handler() -> impl IntoResponse {
    match jemalloc_pprof::PROF_CTL.as_ref() {
        Some(ctl) => {
            let mut ctl = ctl.lock().await;
            match ctl.dump_pprof() {
                Ok(pprof) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    pprof,
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to dump heap profile: {}", e),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Heap profiling not enabled. Set MALLOC_CONF=prof:true",
        )
            .into_response(),
    }
}

pub async fn stats_handler() -> impl IntoResponse {
    // Advance epoch to get fresh stats
    if let Err(e) = epoch::advance() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to advance jemalloc epoch: {}", e),
        )
            .into_response();
    }

    let allocated = stats::allocated::read().unwrap_or(0);
    let active = stats::active::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    let mapped = stats::mapped::read().unwrap_or(0);
    let retained = stats::retained::read().unwrap_or(0);

    let json = format!(
        r#"{{"allocated":{},"active":{},"resident":{},"mapped":{},"retained":{}}}"#,
        allocated, active, resident, mapped, retained
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json,
    )
        .into_response()
}
