use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};
use tikv_jemalloc_ctl::{epoch, stats};

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
