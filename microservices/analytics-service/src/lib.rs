//! Analytics service: popular items over a hopping window of scans. Owns
//! `analytics.sqlite`. Route: `GET /analytics/popular-items`.
//!
//! Choreography, not orchestration: basket-service never calls this service.
//! It records each accepted scan in its outbox and serves the events at
//! `GET /internal/scan-events`; this service pulls that feed in the background
//! and before every read, so an answer always reflects every acknowledged
//! scan. If basket is unreachable, the last published snapshot is served.
pub mod store;

use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    routing::get,
};
use serde::Deserialize;
use serde_json::Value;
use service_kit::{ApiResult, Error, HttpClient, Result, blocking, finish, query};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};
use store::ScanEvent;
pub use store::{Settings, Store};

pub const NAME: &str = "analytics-service";
const BATCH: usize = 5_000;

pub struct Analytics {
    store: Arc<Store>,
    client: HttpClient,
    basket: String,
    cursor: AtomicI64,
    current: Mutex<Arc<Value>>,
    pull: tokio::sync::Mutex<()>,
    degraded: AtomicBool,
}

impl Analytics {
    pub fn new(store: Store, basket: String, client: HttpClient) -> Result<Self> {
        Ok(Self {
            cursor: AtomicI64::new(store.cursor()?),
            current: Mutex::new(Arc::new(store.snapshot()?)),
            store: Arc::new(store),
            client,
            basket,
            pull: tokio::sync::Mutex::new(()),
            degraded: AtomicBool::new(false),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    fn current(&self) -> Result<Arc<Value>> {
        Ok(self.current.lock().map_err(Error::internal)?.clone())
    }

    /// Pulls every event basket has committed since the cursor, publishing
    /// hops as boundaries are crossed, then returns the current snapshot.
    pub async fn refresh(&self) -> Result<Arc<Value>> {
        let _pull = self.pull.lock().await;
        loop {
            let after = self.cursor.load(Ordering::Acquire);
            let url = format!(
                "{}/internal/scan-events?after={after}&limit={BATCH}",
                self.basket
            );
            let body = match self.client.get(&url).await.and_then(|r| r.ok()) {
                Ok(body) => {
                    if self.degraded.swap(false, Ordering::Relaxed) {
                        println!("{NAME}: scan feed reachable again");
                    }
                    body
                }
                Err(error) => {
                    if !self.degraded.swap(true, Ordering::Relaxed) {
                        eprintln!("{NAME}: scan feed unavailable, serving last snapshot: {error}");
                    }
                    break;
                }
            };
            let events: Vec<_> = body["events"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|e| ScanEvent {
                    sequence: e["sequence"].as_i64().unwrap_or_default(),
                    sku: e["sku"].as_str().unwrap_or_default().to_owned(),
                    name: e["name"].as_str().unwrap_or_default().to_owned(),
                })
                .collect();
            if events.is_empty() {
                break;
            }
            let full = events.len() >= BATCH;
            let store = self.store.clone();
            let (cursor, published) = blocking(move || store.ingest(&events)).await?;
            self.cursor.store(cursor, Ordering::Release);
            if let Some(snapshot) = published {
                *self.current.lock().map_err(Error::internal)? = Arc::new(snapshot);
            }
            if !full {
                break;
            }
        }
        self.current()
    }

    pub async fn popular(&self, limit: usize) -> Result<Value> {
        let mut snapshot = Value::clone(&*self.refresh().await?);
        if let Some(items) = snapshot["items"].as_array_mut() {
            items.truncate(limit);
        }
        Ok(snapshot)
    }
}

/// Pulls the feed in the background so the persisted snapshot keeps up with
/// scans even when nobody is reading.
pub fn spawn_refresher(analytics: Arc<Analytics>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = analytics.refresh().await {
                eprintln!("{NAME}: refresh failed: {error}");
            }
        }
    })
}

pub fn router(analytics: Arc<Analytics>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/analytics/popular-items", get(popular))
            .with_state(analytics),
    )
}

#[derive(Deserialize)]
struct PopularQuery {
    limit: Option<usize>,
}
async fn popular(
    State(analytics): State<Arc<Analytics>>,
    input: std::result::Result<Query<PopularQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let limit = query(input)?.limit.unwrap_or(10);
    analytics.popular(limit).await.map(Json)
}
