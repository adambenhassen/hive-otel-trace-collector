use axum::{
    extract::Query,
    http::{header, StatusCode},
    response::IntoResponse,
};
use pprof::protos::Message;
use serde::Deserialize;
use std::time::Duration;

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
