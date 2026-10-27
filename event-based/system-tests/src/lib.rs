//! Test harness: gives each microservice its own temporary database file and
//! each test its own Kafka topics (a unique topic prefix), starts each
//! service's HTTP server on an ephemeral port and its consumers against the
//! broker (as the separate processes are), and exposes the gateway router in
//! front of them. Requires Kafka at `KAFKA_BOOTSTRAP` (default
//! `127.0.0.1:9092`): `docker compose -f event-based/docker-compose.yml up -d --wait`.
use analytics_service::Analytics;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use checkout_gateway::Upstreams;
use payment_service::Payment;
use serde_json::Value;
use service_kit::{Bus, Delivery, Result, Topic, bus::AbortOnDrop};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

/// Every service's settings in one place for tests.
#[derive(Clone, Debug)]
pub struct Config {
    pub catalog_size: i64,
    pub initial_stock: i64,
    pub threshold: i64,
    pub window_size: i64,
    pub slide_interval: i64,
    pub saga_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            catalog_size: 2000,
            initial_stock: 10000,
            threshold: 50,
            window_size: 1000,
            slide_interval: 500,
            saga_timeout: Duration::from_secs(10),
        }
    }
}
impl Config {
    pub fn inventory(&self) -> inventory_service::Settings {
        inventory_service::Settings {
            catalog_size: self.catalog_size,
            initial_stock: self.initial_stock,
            threshold: self.threshold,
        }
    }
    pub fn analytics(&self) -> analytics_service::Settings {
        analytics_service::Settings {
            window_size: self.window_size,
            slide_interval: self.slide_interval,
        }
    }
}

/// A temporary directory holding one database file per service, and the
/// topic prefix that goes with them.
pub struct Dbs {
    dir: PathBuf,
    prefix: Mutex<String>,
}
impl Dbs {
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("checkout-events-{}", unique()));
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            dir,
            prefix: Mutex::new(format!("test-{}", unique())),
        }
    }
    pub fn path(&self, service: &str) -> String {
        self.dir
            .join(format!("{service}.sqlite"))
            .to_str()
            .unwrap()
            .to_owned()
    }
    pub fn prefix(&self) -> String {
        self.prefix.lock().unwrap().clone()
    }
    /// Fresh topics, as resetting a deployment resets its broker.
    fn reset_topics(&self) {
        *self.prefix.lock().unwrap() = format!("test-{}", unique());
    }
}
impl Default for Dbs {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for Dbs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn unique() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}-{}-{nanos}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

pub async fn connect(dbs: &Dbs) -> Bus {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_owned());
    Bus::connect(&bootstrap, &dbs.prefix(), Duration::from_secs(5))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Kafka must be running at {bootstrap} \
                 (docker compose -f event-based/docker-compose.yml up -d --wait): {e}"
            )
        })
}

/// A consumed record built by hand, for calling a handler directly.
pub fn delivery(topic: Topic, value: Value) -> Delivery {
    Delivery {
        topic,
        key: String::new(),
        value,
        partition: 0,
        offset: 0,
    }
}

pub struct Urls {
    pub inventory: String,
    pub basket: String,
    pub payment: String,
    pub analytics: String,
}

pub struct System {
    pub gateway: Router,
    pub urls: Urls,
    pub bus: Bus,
    pub inventory: Arc<inventory_service::Store>,
    pub basket: Arc<basket_service::Store>,
    pub analytics: Arc<Analytics>,
    pub payment: Arc<Payment>,
    consumers: Mutex<HashMap<&'static str, tokio::task::JoinHandle<()>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _relay: AbortOnDrop,
}

impl Drop for System {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        if let Ok(consumers) = self.consumers.lock() {
            for consumer in consumers.values() {
                consumer.abort();
            }
        }
    }
}

pub async fn listen(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}

/// Starts every service in dependency order, each on its own database
/// (dropping and reseeding it, on fresh topics, with `reset`), then the gateway.
pub async fn launch(dbs: &Dbs, config: &Config, reset: bool) -> System {
    if reset {
        dbs.reset_topics();
    }
    let bus = connect(dbs).await;
    let inventory_store = Arc::new(
        inventory_service::Store::open(&dbs.path("inventory"), &config.inventory(), reset).unwrap(),
    );
    inventory_service::publish_catalog(&inventory_store, &bus)
        .await
        .unwrap();
    let (inventory, a) = listen(inventory_service::router(inventory_store.clone())).await;

    let basket_store = Arc::new(basket_service::Store::open(&dbs.path("basket"), reset).unwrap());
    basket_service::sync_catalog(&basket_store, &bus, Duration::from_secs(5))
        .await
        .unwrap();
    let (relay, relay_task) = basket_service::start_relay(basket_store.clone(), bus.clone())
        .await
        .unwrap();
    let (basket, b) = listen(basket_service::router(basket_store.clone(), relay)).await;

    if reset {
        payment_service::Store::open(&dbs.path("payment"), true).unwrap();
        analytics_service::Store::open(&dbs.path("analytics"), &config.analytics(), true).unwrap();
    }
    let payment = Arc::new(Payment::new(
        Arc::new(payment_service::Store::open(&dbs.path("payment"), false).unwrap()),
        bus.clone(),
        config.saga_timeout,
    ));
    payment.recover(true).await.unwrap();
    let (payment_url, c) = listen(payment_service::router(payment.clone())).await;

    let analytics_store =
        analytics_service::Store::open(&dbs.path("analytics"), &config.analytics(), false).unwrap();
    let analytics = Arc::new(Analytics::new(analytics_store, bus.clone()).unwrap());
    let (analytics_url, d) = listen(analytics_service::router(analytics.clone())).await;

    let urls = Urls {
        inventory,
        basket,
        payment: payment_url,
        analytics: analytics_url,
    };
    let system = System {
        gateway: checkout_gateway::router(Upstreams {
            inventory: urls.inventory.clone(),
            basket: urls.basket.clone(),
            payment: urls.payment.clone(),
            analytics: urls.analytics.clone(),
        }),
        urls,
        bus,
        inventory: inventory_store,
        basket: basket_store,
        analytics,
        payment,
        consumers: Mutex::new(HashMap::new()),
        tasks: vec![a, b, c, d],
        _relay: relay_task,
    };
    for service in ["inventory", "basket", "payment", "analytics"] {
        system.resume(service).await;
    }
    system
}

impl System {
    /// Stops one service's consumer, as if the service were down: its
    /// commands or events wait in Kafka.
    pub async fn pause(&self, service: &'static str) {
        let consumer = self.consumers.lock().unwrap().remove(service);
        if let Some(consumer) = consumer {
            consumer.abort();
            let _ = consumer.await;
        }
    }

    /// (Re)starts one service's consumer; it resumes from its group's
    /// committed offset.
    pub async fn resume(&self, service: &'static str) {
        let consumer = match service {
            "inventory" => {
                inventory_service::spawn_consumer(self.inventory.clone(), self.bus.clone()).await
            }
            "basket" => basket_service::spawn_consumer(self.basket.clone(), self.bus.clone()).await,
            "payment" => payment_service::spawn_consumer(self.payment.clone()).await,
            "analytics" => analytics_service::spawn_consumer(self.analytics.clone()).await,
            other => panic!("no consumer for {other}"),
        }
        .unwrap();
        self.consumers.lock().unwrap().insert(service, consumer);
    }

    /// Runs one inventory command through its handler, bypassing Kafka, and
    /// returns the reply.
    pub async fn inventory_command(&self, command: Value) -> Value {
        let replies = inventory_service::handle(
            self.inventory.clone(),
            delivery(Topic::InventoryCommands, command),
        )
        .await
        .unwrap();
        replies.into_iter().next().unwrap().value
    }

    /// Runs one basket command through its handler, bypassing Kafka, and
    /// returns the reply.
    pub async fn basket_command(&self, command: Value) -> Value {
        let replies = basket_service::handle(
            self.basket.clone(),
            delivery(Topic::BasketCommands, command),
        )
        .await
        .unwrap();
        replies.into_iter().next().unwrap().value
    }

    pub async fn raw(&self, method: &str, path: &str, body: &str) -> (u16, Value) {
        let response = self
            .gateway
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    pub async fn request(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        self.raw(method, path, &body.to_string()).await
    }
    pub async fn get(&self, path: &str) -> Value {
        let (code, data) = self.request("GET", path, Value::Null).await;
        assert_eq!(code, 200, "GET {path}: {data}");
        data
    }
    pub async fn start(&self) -> String {
        let (code, result) = self
            .request(
                "POST",
                "/transactions",
                serde_json::json!({"stationId": "station-1"}),
            )
            .await;
        assert_eq!(code, 201);
        assert_eq!(result["status"], "OPEN");
        result["transactionId"].as_str().unwrap().to_owned()
    }
    pub async fn scan(&self, id: &str, sku: &str) -> (u16, Value) {
        self.request(
            "POST",
            &format!("/transactions/{id}/items"),
            serde_json::json!({"sku": sku}),
        )
        .await
    }
    pub async fn complete(&self, id: &str) -> (u16, Value) {
        self.request(
            "POST",
            &format!("/transactions/{id}/complete"),
            serde_json::json!({}),
        )
        .await
    }
    /// Current stock of one SKU, read through the public low-stock API.
    pub async fn stock(&self, sku: &str) -> i64 {
        let alerts = self.get("/inventory/low-stock?threshold=1000000000").await;
        alerts["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["sku"] == sku)
            .map(|a| a["currentStock"].as_i64().unwrap())
            .unwrap()
    }
    /// The state of one saga in the payment log.
    pub fn saga(&self, id: i64) -> payment_service::Saga {
        self.payment.store().get(id).unwrap()
    }
}

/// Polls `check` until it holds, for up to 15 s.
pub async fn eventually(what: &str, mut check: impl FnMut() -> Result<bool>) {
    for _ in 0..1500 {
        if check().unwrap_or(false) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}
