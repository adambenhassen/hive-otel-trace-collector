use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{debug, warn};

const SUCCESS_CACHE_TTL: Duration = Duration::from_secs(30);
const FAILURE_CACHE_TTL: Duration = Duration::from_secs(5);

// ONLY FOR DEV PURPOSES
pub fn is_auth_disabled() -> bool {
    std::env::var("DISABLE_AUTH")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
}

pub async fn extract_and_validate(
    authenticator: &Authenticator,
    headers: &HeaderMap,
) -> Result<String, Response> {
    let target_ref = extract_header(headers, "x-hive-target-ref", "X-Hive-Target-Ref")?;

    // If auth is disabled, use target_ref as target_id (ONLY FOR DEV PURPOSES)
    if is_auth_disabled() {
        debug!("Auth disabled, using target_ref as target_id: {}", target_ref);
        return Ok(target_ref);
    }

    let auth_header = extract_header(headers, "authorization", "Authorization")?;

    match authenticator.authenticate(&auth_header, &target_ref).await {
        Ok(id) => Ok(id),
        Err(e) => {
            warn!("Authentication failed: {}", e);
            Err((StatusCode::UNAUTHORIZED, e).into_response())
        }
    }
}

fn extract_header(headers: &HeaderMap, name: &str, alt_name: &str) -> Result<String, Response> {
    let value = headers
        .get(name)
        .or_else(|| headers.get(alt_name))
        .ok_or_else(|| {
            warn!("Missing {} header", alt_name);
            (
                StatusCode::BAD_REQUEST,
                format!("Missing {} header", alt_name),
            )
                .into_response()
        })?;

    value.to_str().map(|s| s.to_string()).map_err(|_| {
        warn!("Malformed {} header: not valid UTF-8", alt_name);
        (
            StatusCode::BAD_REQUEST,
            format!("Malformed {} header: not valid UTF-8", alt_name),
        )
            .into_response()
    })
}

#[derive(Deserialize)]
struct AuthResponse {
    #[serde(rename = "targetId")]
    target_id: String,
}

type InFlight = Arc<Mutex<HashMap<String, tokio::sync::broadcast::Sender<Result<String, String>>>>>;

fn hash_cache_key(auth_header: &str, target_ref: &str) -> String {
    let mut hasher = DefaultHasher::new();
    auth_header.hash(&mut hasher);
    target_ref.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub struct Authenticator {
    client: Client,
    success_cache: Cache<String, String>,
    failure_cache: Cache<String, ()>,
    endpoint: String,
    in_flight: InFlight,
}

impl Authenticator {
    pub fn new(client: Client, endpoint: String) -> Self {
        let success_cache = Cache::builder()
            .time_to_live(SUCCESS_CACHE_TTL)
            .max_capacity(10_000)
            .build();

        let failure_cache = Cache::builder()
            .time_to_live(FAILURE_CACHE_TTL)
            .max_capacity(10_000)
            .build();

        Self {
            client,
            success_cache,
            failure_cache,
            endpoint,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn authenticate(
        &self,
        auth_header: &str,
        target_ref: &str,
    ) -> Result<String, String> {
        // Hash credentials to avoid storing sensitive data in cache keys
        let cache_key = hash_cache_key(auth_header, target_ref);

        // Check success cache first
        if let Some(target_id) = self.success_cache.get(&cache_key).await {
            debug!("Auth cache hit (success) for target_ref={}", target_ref);
            return Ok(target_id);
        }

        // Check failure cache
        if self.failure_cache.contains_key(&cache_key) {
            debug!("Auth cache hit (failure) for target_ref={}", target_ref);
            return Err("Authentication failed (cached)".to_string());
        }

        // Singleflight: atomically check cache again and register in-flight request
        let receiver = {
            let mut in_flight = self.in_flight.lock().await;

            // Re-check caches while holding lock to prevent race
            if let Some(target_id) = self.success_cache.get(&cache_key).await {
                debug!("Auth cache hit (success, after lock) for target_ref={}", target_ref);
                return Ok(target_id);
            }
            if self.failure_cache.contains_key(&cache_key) {
                debug!("Auth cache hit (failure, after lock) for target_ref={}", target_ref);
                return Err("Authentication failed (cached)".to_string());
            }

            if let Some(sender) = in_flight.get(&cache_key) {
                Some(sender.subscribe())
            } else {
                let (tx, _) = tokio::sync::broadcast::channel(1);
                in_flight.insert(cache_key.clone(), tx);
                None
            }
        };

        if let Some(mut rx) = receiver {
            debug!("Waiting on in-flight auth request for target_ref={}", target_ref);
            return match rx.recv().await {
                Ok(result) => result,
                Err(e) => Err(format!("Authentication request failed: {}", e)),
            };
        }

        let result = self.do_request(auth_header, target_ref).await;

        // Notify waiters with full error context
        {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(sender) = in_flight.remove(&cache_key) {
                if let Err(e) = sender.send(result.clone()) {
                    debug!("No waiters for auth result (all receivers dropped): {:?}", e);
                }
            }
        }

        // Cache result in appropriate cache
        match &result {
            Ok(target_id) => {
                self.success_cache
                    .insert(cache_key, target_id.clone())
                    .await;
            }
            Err(_) => {
                self.failure_cache.insert(cache_key, ()).await;
            }
        }

        result
    }

    async fn do_request(&self, auth_header: &str, target_ref: &str) -> Result<String, String> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 100;

        let mut last_error = String::new();

        for attempt in 0..MAX_RETRIES {
            let response = self
                .client
                .get(&self.endpoint)
                .header("Authorization", auth_header)
                .header("X-Hive-Target-Ref", target_ref)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        match resp.json::<AuthResponse>().await {
                            Ok(auth_resp) => {
                                // Validate target_id is not empty
                                if auth_resp.target_id.trim().is_empty() {
                                    return Err("Auth endpoint returned empty targetId".to_string());
                                }
                                return Ok(auth_resp.target_id);
                            }
                            Err(e) => return Err(format!("Failed to parse auth response: {}", e)),
                        }
                    }

                    if status.is_server_error() {
                        warn!(
                            "Received {} from auth endpoint, attempt {}/{}",
                            status,
                            attempt + 1,
                            MAX_RETRIES
                        );
                        last_error = format!("Server error: {}", status);
                        tokio::time::sleep(Duration::from_millis(
                            RETRY_DELAY_MS * (attempt as u64 + 1),
                        ))
                        .await;
                        continue;
                    }

                    return Err(format!("Authentication failed: {}", status));
                }
                Err(e) => {
                    warn!(
                        "Request failed: {}, attempt {}/{}",
                        e,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    last_error = format!("Request failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(
                        RETRY_DELAY_MS * (attempt as u64 + 1),
                    ))
                    .await;
                }
            }
        }

        Err(format!(
            "Authentication failed after {} retries: {}",
            MAX_RETRIES, last_error
        ))
    }
}
