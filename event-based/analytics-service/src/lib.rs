//! Analytics service: popular items over a hopping window of scans. Owns
//! `analytics.sqlite`. Route: `GET /analytics/popular-items`.
//!
//! Choreography, not orchestration: no service calls this one, and it calls
//! none. It consumes basket's `ItemScanned` events from Kafka as they
//! arrive. Before answering a read it waits until it has ingested the newest
//! scan in the topic. basket acknowledges a scan once its outbox row commits
//! and relays the event right after, so an answer can trail the newest
//! acknowledged scans by the relay's latency. If Kafka is unreachable, the
//! last published snapshot is served.
pub mod store;

use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    routing::get,
};
use serde::Deserialize;
use serde_json::Value;
use service_kit::{ApiResult, Bus, Error, Result, Topic, blocking, bus, finish, query};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use store::ScanEvent;
pub use store::{Settings, Store};
use tokio::sync::watch;

pub const NAME: &str = "analytics-service";
const BATCH: usize = 5_000;
/// How long a read waits to catch up before serving the last snapshot.
const CATCH_UP: Duration = Duration::from_secs(5);

pub struct Analytics {
    store: Arc<Store>,
    bus: Bus,
    cursor: watch::Sender<i64>,
    current: Mutex<Arc<Value>>,
    degraded: AtomicBool,
}

impl Analytics {
    pub fn new(store: Store, bus: Bus) -> Result<Self> {
        Ok(Self {
            cursor: watch::Sender::new(store.cursor()?),
            current: Mutex::new(Arc::new(store.snapshot()?)),
            store: Arc::new(store),
            bus,
            degraded: AtomicBool::new(false),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// The last scan sequence ingested.
    pub fn cursor(&self) -> i64 {
        *self.cursor.borrow()
    }

    fn current(&self) -> Result<Arc<Value>> {
        Ok(self.current.lock().map_err(Error::internal)?.clone())
    }

    /// Ingests one batch of scan events, publishing hops as boundaries are crossed.
    async fn ingest(&self, events: Vec<ScanEvent>) -> Result<()> {
        let store = self.store.clone();
        let (cursor, published) = blocking(move || store.ingest(&events)).await?;
        if let Some(snapshot) = published {
            *self.current.lock().map_err(Error::internal)? = Arc::new(snapshot);
        }
        self.cursor.send_replace(cursor);
        Ok(())
    }

    /// Waits until the newest scan in the topic is ingested, then returns
    /// the current snapshot. Serves the last snapshot if Kafka does not
    /// answer or the consumer does not catch up within `wait`.
    pub async fn catch_up(&self, wait: Duration) -> Result<Arc<Value>> {
        let caught_up = tokio::time::timeout(wait, async {
            let newest = self
                .bus
                .last(Topic::Scans)
                .await?
                .map_or(0, |e| e.value["sequence"].as_i64().unwrap_or_default());
            let mut cursor = self.cursor.subscribe();
            cursor
                .wait_for(|c| *c >= newest)
                .await
                .map_err(Error::internal)?;
            Ok::<_, Error>(())
        })
        .await;
        match caught_up {
            Ok(Ok(())) => {
                if self.degraded.swap(false, Ordering::Relaxed) {
                    println!("{NAME}: scan events reachable again");
                }
            }
            Ok(Err(error)) => self.degrade(&error.to_string()),
            Err(_) => self.degrade("timed out catching up"),
        }
        self.current()
    }

    fn degrade(&self, reason: &str) {
        if !self.degraded.swap(true, Ordering::Relaxed) {
            eprintln!("{NAME}: serving last snapshot: {reason}");
        }
    }

    pub async fn popular(&self, limit: usize) -> Result<Value> {
        let mut snapshot = Value::clone(&*self.catch_up(CATCH_UP).await?);
        if let Some(items) = snapshot["items"].as_array_mut() {
            items.truncate(limit);
        }
        Ok(snapshot)
    }
}

/// Consumes scan events in batches until aborted, so the persisted snapshot
/// keeps up with scans even when nobody is reading. An offset is stored only
/// after its batch is committed to `analytics.sqlite`.
pub async fn spawn_consumer(analytics: Arc<Analytics>) -> Result<tokio::task::JoinHandle<()>> {
    let consumer = analytics.bus.subscribe(NAME, &[Topic::Scans])?;
    let polled = consumer.clone();
    Ok(bus::start(NAME, &consumer, async move {
        let consumer = polled;
        loop {
            let batch = match consumer.batch(BATCH).await {
                Ok(batch) => batch,
                Err(error) => {
                    eprintln!("{NAME}: consume failed: {error}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let events: Vec<_> = batch
                .iter()
                .filter(|d| d.kind() == "ItemScanned")
                .map(|d| ScanEvent {
                    sequence: d.value["sequence"].as_i64().unwrap_or_default(),
                    sku: d.value["sku"].as_str().unwrap_or_default().to_owned(),
                    name: d.value["name"].as_str().unwrap_or_default().to_owned(),
                })
                .collect();
            while let Err(error) = analytics.ingest(events.clone()).await {
                eprintln!("{NAME}: ingest failed, retrying: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if let Some(last) = batch.last()
                && let Err(error) = consumer.done(last)
            {
                eprintln!("{NAME}: {error}");
            }
        }
    })
    .await)
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
