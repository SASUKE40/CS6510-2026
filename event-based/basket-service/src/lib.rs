//! Basket service: starts transactions, scans items into open baskets, and
//! reports basket state. Owns `basket.sqlite`.
//!
//! Public routes: `POST /transactions`, `GET /transactions/{id}`,
//! `POST /transactions/{id}/items`.
//! Events (no other service calls this one):
//! - reads the latest `CatalogPublished` at startup into its catalog replica;
//! - relays its scan outbox, in order, as `ItemScanned` events;
//! - consumes `Checkout`, `Release`, and `Complete` commands from the payment
//!   saga and answers each, keyed by transaction id, on its event topic.
pub mod store;

use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{
    ApiResult, Bus, Delivery, Error, Outgoing, Result, Topic, blocking, body, bus, finish, money,
    parse_transaction_id,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
pub use store::Store;
use store::{Basket, CatalogItem, ScanEvent};
use tokio::sync::{Notify, mpsc, watch};

pub const NAME: &str = "basket-service";
const RELAY_BATCH: i64 = 5_000;
/// How long a scan waits for its event to reach Kafka before it is
/// acknowledged anyway; the outbox still delivers it later.
const RELAY_WAIT: Duration = Duration::from_secs(5);

/// Rebuilds the catalog replica from the latest `CatalogPublished` event.
/// Waits up to `wait` for one on first start; with a replica already
/// present, a missing catalog is only a warning.
pub async fn sync_catalog(store: &Arc<Store>, bus: &Bus, wait: Duration) -> Result<usize> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let latest = match bus.last(Topic::Catalog).await {
            Ok(Some(event)) if event.kind() == "CatalogPublished" => Ok(event.value),
            Ok(_) => Err(Error::unavailable("no catalog has been published yet")),
            Err(error) => Err(error),
        };
        match latest {
            Ok(event) => {
                let items: Vec<_> = event["items"]
                    .as_array()
                    .ok_or_else(|| Error::internal("catalog event has no items"))?
                    .iter()
                    .map(|i| CatalogItem {
                        sku: i["sku"].as_str().unwrap_or_default().to_owned(),
                        name: i["name"].as_str().unwrap_or_default().to_owned(),
                        cents: i["priceCents"].as_i64().unwrap_or_default(),
                    })
                    .collect();
                let count = items.len();
                let store = store.clone();
                blocking(move || store.replace_catalog(&items)).await?;
                return Ok(count);
            }
            Err(error) => {
                let store_ = store.clone();
                if blocking(move || store_.catalog_size()).await? > 0 {
                    eprintln!("{NAME}: catalog refresh failed, using local replica: {error}");
                    return Ok(0);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// Handle on the outbox relay: wakes it after a scan commits and reports
/// the newest sequence Kafka has stored.
#[derive(Clone)]
pub struct Relay {
    wake: Arc<Notify>,
    published: watch::Receiver<i64>,
}

impl Relay {
    pub fn published(&self) -> i64 {
        *self.published.borrow()
    }

    /// Waits until every event up to `sequence` is in Kafka, or `timeout`.
    pub async fn wait_for(&self, sequence: i64, timeout: Duration) -> bool {
        self.wake.notify_one();
        let mut published = self.published.clone();
        tokio::time::timeout(timeout, published.wait_for(|p| *p >= sequence))
            .await
            .is_ok_and(|r| r.is_ok())
    }
}

/// Starts relaying the outbox from the last sequence already in Kafka.
pub async fn start_relay(store: Arc<Store>, bus: Bus) -> Result<(Relay, bus::AbortOnDrop)> {
    let last = bus
        .last(Topic::Scans)
        .await?
        .map_or(0, |e| e.value["sequence"].as_i64().unwrap_or_default());
    let store_ = store.clone();
    let outbox = blocking(move || store_.last_sequence()).await?;
    if last > outbox {
        return Err(Error::internal(format!(
            "{} already holds scan {last} but the outbox ends at {outbox}; reset the broker too",
            bus.name(Topic::Scans)
        )));
    }
    let wake = Arc::new(Notify::new());
    let (published_tx, published) = watch::channel(last);
    let rewind = Arc::new(Mutex::new(None::<i64>));
    let (pending_tx, mut pending) = mpsc::unbounded_channel();

    // Confirms deliveries in order. A failed delivery rewinds the relay to
    // the last confirmed event, so a gap is re-sent rather than skipped.
    let acker = {
        let (rewind, wake) = (rewind.clone(), wake.clone());
        tokio::spawn(async move {
            while let Some((sequence, delivery)) = pending.recv().await {
                let confirmed = *published_tx.borrow();
                match delivery.await {
                    Ok(()) if sequence == confirmed + 1 => {
                        published_tx.send_replace(sequence);
                    }
                    Ok(()) => {}
                    Err(error) => {
                        eprintln!("{NAME}: scan {sequence} not delivered, re-sending: {error}");
                        if let Ok(mut slot) = rewind.lock() {
                            *slot = Some(confirmed);
                        }
                        wake.notify_one();
                    }
                }
            }
        })
    };
    let relay_wake = wake.clone();
    let task = tokio::spawn(async move {
        let _acker = bus::AbortOnDrop(acker);
        let mut enqueued = last;
        loop {
            if let Some(confirmed) = rewind.lock().ok().and_then(|mut s| s.take()) {
                enqueued = confirmed;
            }
            let store = store.clone();
            let events = match blocking(move || store.events(enqueued, RELAY_BATCH)).await {
                Ok(events) => events,
                Err(error) => {
                    eprintln!("{NAME}: reading the outbox failed: {error}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            if events.is_empty() {
                let _ =
                    tokio::time::timeout(Duration::from_millis(100), relay_wake.notified()).await;
                continue;
            }
            for event in events {
                match bus.send(&scanned(&event)) {
                    Ok(delivery) => {
                        enqueued = event.sequence;
                        if pending_tx.send((event.sequence, delivery)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        eprintln!("{NAME}: enqueueing scan {} failed: {error}", event.sequence);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                }
            }
        }
    });
    Ok((Relay { wake, published }, bus::AbortOnDrop(task)))
}

fn scanned(event: &ScanEvent) -> Outgoing {
    Outgoing::new(
        Topic::Scans,
        event.sequence.to_string(),
        json!({"type": "ItemScanned", "sequence": event.sequence, "sku": event.sku, "name": event.name}),
    )
}

/// Consumes basket commands until aborted.
pub async fn spawn_consumer(store: Arc<Store>, bus: Bus) -> Result<tokio::task::JoinHandle<()>> {
    let consumer = bus.subscribe(NAME, &[Topic::BasketCommands])?;
    let polled = consumer.clone();
    Ok(bus::start(NAME, &consumer, async move {
        bus::run(bus.clone(), polled, NAME, move |delivery| {
            handle(store.clone(), delivery)
        })
        .await
    })
    .await)
}

/// Applies one saga command and returns its reply. Business refusals become
/// rejection replies; only infrastructure failures are errors.
pub async fn handle(store: Arc<Store>, command: Delivery) -> Result<Vec<Outgoing>> {
    let saga = command.value["sagaId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let tx = command.value["transactionId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let event = |kind: &str| json!({"type": kind, "sagaId": saga, "transactionId": tx});
    let rejected = |kind: &str, error: Error| {
        let mut reply = error.to_event();
        reply["type"] = json!(kind);
        reply["sagaId"] = json!(saga);
        reply["transactionId"] = json!(tx);
        reply
    };
    let id = parse_transaction_id(&tx);
    let reply = match command.kind() {
        "Checkout" => {
            let step = id.map(|id| (id, saga.clone()));
            match step {
                Ok((id, s)) => blocking(move || store.checkout(id, &s)).await,
                Err(error) => Err(error),
            }
            .map(|(basket, lines)| {
                let mut reply = event("BasketLocked");
                reply["basket"] = locked(&basket, lines);
                reply
            })
            .or_else(|e| reject(e, |e| rejected("CheckoutRejected", e)))?
        }
        "Release" => match id {
            Ok(id) => {
                let s = saga.clone();
                blocking(move || store.release(id, &s)).await
            }
            Err(error) => Err(error),
        }
        .map(|released| {
            let mut reply = event("BasketReleased");
            reply["released"] = json!(released);
            reply
        })
        .or_else(|e| match e.kind {
            // Nothing to release: the basket never existed.
            service_kit::Kind::NotFound => {
                let mut reply = event("BasketReleased");
                reply["released"] = json!(false);
                Ok(reply)
            }
            _ => reject(e, |e| rejected("ReleaseRejected", e)),
        })?,
        "Complete" => {
            let completed_at = command.value["completedAt"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            match id {
                Ok(id) if !completed_at.is_empty() => {
                    let s = saga.clone();
                    blocking(move || store.complete(id, &s, &completed_at)).await
                }
                Ok(_) => Err(Error::bad("completedAt is required")),
                Err(error) => Err(error),
            }
            .map(|_| event("BasketCompleted"))
            .or_else(|e| reject(e, |e| rejected("CompleteRejected", e)))?
        }
        other => {
            eprintln!("{NAME}: ignoring unknown command {other:?}");
            return Ok(vec![]);
        }
    };
    Ok(vec![Outgoing::new(Topic::BasketEvents, tx, reply)])
}

/// Turns a business refusal into a reply; other errors are retried.
fn reject(error: Error, reply: impl FnOnce(Error) -> Value) -> Result<Value> {
    if error.is_rejection() {
        Ok(reply(error))
    } else {
        Err(error)
    }
}

/// The basket as locked for checkout: what payment needs for the
/// reservation and the receipt.
fn locked(basket: &Basket, lines: Vec<store::Line>) -> Value {
    let lines: Vec<_> = lines
        .into_iter()
        .map(|l| {
            json!({"sku": l.item.sku, "name": l.item.name,
                "unitPriceCents": l.item.cents, "quantity": l.quantity})
        })
        .collect();
    json!({"transactionId": format!("tx-{}", basket.id),
        "stationId": basket.station, "itemCount": basket.count, "totalCents": basket.cents,
        "startedAt": basket.started_at, "lines": lines})
}

#[derive(Clone)]
struct App {
    store: Arc<Store>,
    relay: Relay,
}

pub fn router(store: Arc<Store>, relay: Relay) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions", post(start))
            .route("/transactions/{id}", get(status))
            .route("/transactions/{id}/items", post(scan))
            .with_state(App { store, relay }),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Start {
    station_id: String,
}
async fn start(
    State(app): State<App>,
    input: std::result::Result<Json<Start>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let station = body(input)?.station_id;
    if station.trim().is_empty() {
        return Err(Error::bad("stationId must not be blank"));
    }
    let basket = blocking(move || app.store.create(&station)).await?;
    Ok((StatusCode::CREATED, Json(summary(&basket))))
}

async fn status(State(app): State<App>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_transaction_id(&id)?;
    blocking(move || Ok(Json(summary(&app.store.get(id)?)))).await
}

#[derive(Deserialize)]
struct Scan {
    sku: String,
}
/// A scan is acknowledged once its event is in Kafka, so analytics can
/// count every acknowledged scan.
async fn scan(
    State(app): State<App>,
    Path(id): Path<String>,
    input: std::result::Result<Json<Scan>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let sku = body(input)?.sku;
    let id = parse_transaction_id(&id)?;
    let store = app.store.clone();
    let (basket, item, sequence) = blocking(move || store.scan(id, &sku)).await?;
    if !app.relay.wait_for(sequence, RELAY_WAIT).await {
        eprintln!("{NAME}: scan {sequence} acknowledged before reaching Kafka");
    }
    Ok(Json(
        json!({"transactionId": format!("tx-{id}"), "sku": item.sku, "name": item.name,
            "unitPrice": money(item.cents), "itemCount": basket.count, "runningTotal": money(basket.cents)}),
    ))
}

/// The contract's basket view. A basket held by a saga is still `OPEN` to
/// clients; the spec has no in-between state.
fn summary(basket: &Basket) -> Value {
    let status = if basket.status == "CHECKING_OUT" {
        "OPEN"
    } else {
        &basket.status
    };
    json!({"transactionId": format!("tx-{}", basket.id), "stationId": basket.station,
        "status": status, "itemCount": basket.count, "runningTotal": money(basket.cents),
        "startedAt": basket.started_at})
}
