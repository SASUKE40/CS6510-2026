//! Payment service: completes a basket as an orchestrated saga driven by
//! events. Owns `payment.sqlite` (the saga log and payments). It sends
//! commands to basket and inventory over Kafka and advances each saga when
//! their replies arrive. Route: `POST /transactions/{id}/complete`.
//!
//! ```text
//! STARTED --BasketLocked--> BASKET_LOCKED --StockReserved--> STOCK_RESERVED --BasketCompleted--> COMPLETED
//!    |                          |               (+ payment recorded: the pivot)
//!    | CheckoutRejected         | StockRejected
//!    v                          v
//! ABORTED <--BasketReleased-- RELEASING <--ReservationCancelled-- COMPENSATING
//!                                                  (recovery of a stalled STARTED / BASKET_LOCKED)
//! ```
//!
//! Commands are idempotent and keyed by saga id, and every state change is
//! a compare-and-set, so a redelivered or stale reply changes nothing and a
//! command can always be re-sent. Recovery re-sends the command for each
//! stalled saga's state: STOCK_RESERVED rolls forward, earlier states roll back.
//! The client's request is answered at the pivot with its receipt, or when
//! the saga aborts.
pub mod store;

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use service_kit::{
    ApiResult, Bus, Delivery, Error, Outgoing, Result, Topic, blocking, bus, finish, money, now,
    parse_transaction_id,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
pub use store::{Saga, Store};
use tokio::sync::oneshot;

pub const NAME: &str = "payment-service";
/// Periodic recovery leaves recent sagas alone; their replies may still be on the way.
const RECOVERY_GRACE: Duration = Duration::from_secs(5);

type Outcome = Result<Value>;

pub struct Payment {
    store: Arc<Store>,
    bus: Bus,
    /// Completion requests waiting for their saga to finish, by saga id.
    waiters: Mutex<HashMap<i64, oneshot::Sender<Outcome>>>,
    timeout: Duration,
}

/// Removes a request's waiter when the request ends, however it ends.
struct Waiting<'a>(&'a Mutex<HashMap<i64, oneshot::Sender<Outcome>>>, i64);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Ok(mut waiters) = self.0.lock() {
            waiters.remove(&self.1);
        }
    }
}

fn saga_id(id: i64) -> String {
    format!("saga-{id}")
}

fn command(kind: &str, saga: &Saga) -> Value {
    json!({"type": kind, "sagaId": saga_id(saga.id), "transactionId": saga.transaction_id})
}

fn checkout(saga: &Saga) -> Outgoing {
    Outgoing::new(
        Topic::BasketCommands,
        &saga.transaction_id,
        command("Checkout", saga),
    )
}

fn reserve(saga: &Saga, basket: &Value) -> Outgoing {
    let lines: Vec<_> = basket["lines"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|l| json!({"sku": l["sku"], "quantity": l["quantity"]}))
        .collect();
    let mut value = command("Reserve", saga);
    value["lines"] = json!(lines);
    Outgoing::new(Topic::InventoryCommands, saga_id(saga.id), value)
}

fn complete(saga: &Saga, paid_at: &str) -> Outgoing {
    let mut value = command("Complete", saga);
    value["completedAt"] = json!(paid_at);
    Outgoing::new(Topic::BasketCommands, &saga.transaction_id, value)
}

fn release(saga: &Saga) -> Outgoing {
    Outgoing::new(
        Topic::BasketCommands,
        &saga.transaction_id,
        command("Release", saga),
    )
}

fn cancel(saga: &Saga) -> Outgoing {
    Outgoing::new(
        Topic::InventoryCommands,
        saga_id(saga.id),
        command("Cancel", saga),
    )
}

impl Payment {
    pub fn new(store: Arc<Store>, bus: Bus, timeout: Duration) -> Self {
        Self {
            store,
            bus,
            waiters: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        blocking(move || f(&store)).await
    }

    fn answer(&self, saga: i64, outcome: Outcome) {
        let waiter = self.waiters.lock().ok().and_then(|mut w| w.remove(&saga));
        if let Some(waiter) = waiter {
            let _ = waiter.send(outcome);
        }
    }

    fn waiting(&self, saga: i64) -> bool {
        self.waiters
            .lock()
            .map(|w| w.contains_key(&saga))
            .unwrap_or(false)
    }

    /// Starts a saga for one completion request and waits for its outcome.
    pub async fn complete(&self, transaction: &str) -> Result<Value> {
        let tx = format!("tx-{}", parse_transaction_id(transaction)?);
        let id = self
            .db({
                let tx = tx.clone();
                move |s| s.start(&tx)
            })
            .await?;
        let (sender, outcome) = oneshot::channel();
        self.waiters
            .lock()
            .map_err(Error::internal)?
            .insert(id, sender);
        let _waiting = Waiting(&self.waiters, id);
        let saga = self.db(move |s| s.get(id)).await?;
        if let Err(error) = self.bus.send(&checkout(&saga))?.await {
            eprintln!("{NAME}: saga {id}: Checkout not sent, left for recovery: {error}");
            return Err(Error::unavailable("The event broker is unavailable"));
        }
        match tokio::time::timeout(self.timeout, outcome).await {
            Ok(Ok(outcome)) => outcome,
            _ => {
                eprintln!("{NAME}: saga {id}: no outcome in time, left for recovery");
                Err(Error::unavailable("The checkout did not finish in time"))
            }
        }
    }

    /// Advances the saga a basket or inventory reply belongs to and returns
    /// the next command. Replies that do not fit the saga's state are stale
    /// or duplicates and are ignored.
    pub async fn on_event(&self, event: Delivery) -> Result<Vec<Outgoing>> {
        let Some(id) = event.value["sagaId"]
            .as_str()
            .and_then(|s| s.strip_prefix("saga-"))
            .and_then(|s| s.parse::<i64>().ok())
        else {
            return Ok(vec![]);
        };
        let saga = match self.db(move |s| s.get(id)).await {
            Ok(saga) => saga,
            Err(error) if error.is_rejection() => return Ok(vec![]),
            Err(error) => return Err(error),
        };
        let rejection = || Error::from_event(&event.value);
        let next = match (saga.state.as_str(), event.kind()) {
            ("STARTED", "BasketLocked") => {
                let basket = event.value["basket"].clone();
                let stored = basket.clone();
                self.db(move |s| s.locked(id, &stored))
                    .await?
                    .then(|| reserve(&saga, &basket))
            }
            ("STARTED", "CheckoutRejected") => {
                let error = rejection();
                let failure = error.clone();
                if self
                    .db(move |s| s.aborted(id, "STARTED", Some(&failure)))
                    .await?
                {
                    self.answer(id, Err(error));
                }
                None
            }
            ("BASKET_LOCKED", "StockReserved") => {
                let paid_at = now();
                let amount = saga
                    .basket
                    .as_ref()
                    .and_then(|b| b["totalCents"].as_i64())
                    .unwrap_or_default();
                let at = paid_at.clone();
                if !self.db(move |s| s.reserved(id, amount, &at)).await? {
                    return Ok(vec![]);
                }
                // Past the pivot the saga only rolls forward, so the client
                // gets its receipt now; recovery re-sends Complete if needed.
                let basket = saga.basket.clone().unwrap_or_default();
                self.answer(id, Ok(receipt(&basket, &paid_at)));
                Some(complete(&saga, &paid_at))
            }
            ("BASKET_LOCKED", "StockRejected") => {
                let failure = rejection();
                self.db(move |s| s.releasing(id, "BASKET_LOCKED", Some(&failure)))
                    .await?
                    .then(|| release(&saga))
            }
            ("STOCK_RESERVED", "BasketCompleted") => {
                self.db(move |s| s.completed(id)).await?;
                None
            }
            ("COMPENSATING", "ReservationCancelled") => self
                .db(move |s| s.releasing(id, "COMPENSATING", None))
                .await?
                .then(|| release(&saga)),
            ("RELEASING", "BasketReleased") => {
                if self.db(move |s| s.aborted(id, "RELEASING", None)).await? {
                    let failure = saga
                        .failure
                        .unwrap_or_else(|| Error::unavailable("The checkout was undone"));
                    self.answer(id, Err(failure));
                }
                None
            }
            (state, kind @ ("CompleteRejected" | "ReleaseRejected")) => {
                eprintln!("{NAME}: saga {id} in {state}: {kind} {}", rejection());
                None
            }
            _ => None,
        };
        Ok(next.into_iter().collect())
    }

    /// Re-sends the command for every stalled saga's state; STARTED and
    /// BASKET_LOCKED are rolled back first. With `all`, includes recent sagas
    /// (use only when no request can be in flight, i.e. at startup). Returns
    /// how many sagas were retried.
    pub async fn recover(&self, all: bool) -> Result<usize> {
        let before = if all {
            "9999".to_owned()
        } else {
            (Utc::now() - RECOVERY_GRACE).to_rfc3339_opts(SecondsFormat::Millis, true)
        };
        let sagas = self.db(move |s| s.unfinished(&before)).await?;
        let mut retried = 0;
        for saga in sagas {
            if self.waiting(saga.id) {
                continue;
            }
            let id = saga.id;
            let next = match (saga.state.as_str(), &saga.paid_at) {
                (state @ ("STARTED" | "BASKET_LOCKED"), _) => {
                    let from = state.to_owned();
                    if !self.db(move |s| s.compensating(id, &from)).await? {
                        continue;
                    }
                    cancel(&saga)
                }
                ("COMPENSATING", _) => cancel(&saga),
                ("RELEASING", _) => release(&saga),
                ("STOCK_RESERVED", Some(paid_at)) => complete(&saga, paid_at),
                _ => continue,
            };
            self.db(move |s| s.touch(id)).await?;
            self.bus.send(&next)?.await?;
            println!(
                "{NAME}: recovery re-sent {} for saga {id} in {}",
                next.value["type"], saga.state
            );
            retried += 1;
        }
        Ok(retried)
    }
}

fn receipt(basket: &Value, completed_at: &str) -> Value {
    let lines: Vec<_> = basket["lines"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|l| {
            json!({"sku": l["sku"], "name": l["name"],
                "unitPrice": money(l["unitPriceCents"].as_i64().unwrap_or_default()),
                "quantity": l["quantity"]})
        })
        .collect();
    json!({"transactionId": basket["transactionId"], "stationId": basket["stationId"],
        "itemCount": basket["itemCount"],
        "totalAmount": money(basket["totalCents"].as_i64().unwrap_or_default()),
        "startedAt": basket["startedAt"], "completedAt": completed_at, "lines": lines})
}

/// Consumes basket and inventory replies until aborted.
pub async fn spawn_consumer(payment: Arc<Payment>) -> Result<tokio::task::JoinHandle<()>> {
    let consumer = payment
        .bus
        .subscribe(NAME, &[Topic::BasketEvents, Topic::InventoryEvents])?;
    let polled = consumer.clone();
    Ok(bus::start(NAME, &consumer, async move {
        let bus = payment.bus.clone();
        bus::run(bus, polled, NAME, move |event| {
            let payment = payment.clone();
            async move { payment.on_event(event).await }
        })
        .await
    })
    .await)
}

/// Retries stalled sagas in the background (e.g. after a service outage).
pub fn spawn_recovery(payment: Arc<Payment>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = payment.recover(false).await {
                eprintln!("{NAME}: recovery pass failed: {error}");
            }
        }
    })
}

pub fn router(payment: Arc<Payment>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions/{id}/complete", post(complete_route))
            .with_state(payment),
    )
}

async fn complete_route(
    State(payment): State<Arc<Payment>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    payment.complete(&id).await.map(Json)
}
