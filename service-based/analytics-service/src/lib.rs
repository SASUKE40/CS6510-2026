//! Analytics service: popular items over a hopping window of scans.
//! Route: `GET /analytics/popular-items`.
//!
//! The basket service never calls this service. It appends each accepted scan
//! to the shared `scans` table; this service polls that log, publishes a
//! ranking at every `slideInterval` boundary, and trims scans that no future
//! window can include.
use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    routing::get,
};
use checkout_data::{Error, Result, Store, now, popularity, scans};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{ApiResult, blocking, finish, query};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub const NAME: &str = "analytics-service";

pub struct Analytics {
    store: Store,
    current: Mutex<Arc<Value>>,
}

impl Analytics {
    pub fn new(store: Store) -> Result<Self> {
        let snapshot = store.read(popularity::load)?;
        Ok(Self {
            store,
            current: Mutex::new(Arc::new(snapshot)),
        })
    }

    /// Publishes the newest completed hop if committed scans have crossed a
    /// slide boundary since the last one, then returns the current snapshot.
    /// Every scan acknowledged before this call is committed and therefore
    /// visible, so readers never see a stale window.
    pub fn refresh(&self) -> Result<Arc<Value>> {
        let mut current = self.current.lock().map_err(Error::internal)?;
        let settings = self.store.settings();
        let (size, slide) = (settings.window_size, settings.slide_interval);
        let latest = self.store.read(scans::latest_sequence)?;
        let end = latest / slide * slide;
        if end <= current["windowEnd"].as_i64().unwrap_or(0) {
            return Ok(current.clone());
        }
        let start = (end - size + 1).max(1);
        let snapshot = self.store.write(|db| {
            let items: Vec<_> = scans::ranked_counts(db, start, end)?
                .into_iter()
                .enumerate()
                .map(|(index, (sku, name, count))| {
                    json!({"sku": sku, "name": name, "scanCount": count, "rank": index + 1})
                })
                .collect();
            let snapshot = json!({"windowSize": size, "slideInterval": slide,
                "windowStart": start, "windowEnd": end, "computedAt": now(), "items": items});
            popularity::save(db, &snapshot)?;
            scans::trim_through(db, end - size)?;
            Ok(snapshot)
        })?;
        *current = Arc::new(snapshot);
        Ok(current.clone())
    }

    pub fn popular(&self, limit: usize) -> Result<Value> {
        let mut snapshot = Value::clone(&*self.refresh()?);
        if let Some(items) = snapshot["items"].as_array_mut() {
            items.truncate(limit);
        }
        Ok(snapshot)
    }
}

/// Publishes hops in the background so the persisted snapshot keeps up with
/// scans even when nobody is reading.
pub fn spawn_refresher(analytics: Arc<Analytics>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let analytics = analytics.clone();
            if let Ok(Err(error)) = tokio::task::spawn_blocking(move || analytics.refresh()).await {
                eprintln!("analytics refresh failed: {error}");
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
    blocking(move || analytics.popular(limit).map(Json)).await
}
