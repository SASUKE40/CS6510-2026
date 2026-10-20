//! Test harness: gives each microservice its own temporary database file,
//! starts each on an ephemeral TCP port wired to the others by URL (as the
//! separate processes are), and exposes the gateway router in front of them.
use analytics_service::Analytics;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use checkout_gateway::Upstreams;
use payment_service::Payment;
use serde_json::Value;
use service_kit::HttpClient;
use std::{
    path::PathBuf,
    sync::{
        Arc,
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
}
impl Default for Config {
    fn default() -> Self {
        Self {
            catalog_size: 2000,
            initial_stock: 10000,
            threshold: 50,
            window_size: 1000,
            slide_interval: 500,
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

/// A temporary directory holding one database file per service.
pub struct Dbs(PathBuf);
impl Dbs {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "checkout-microservices-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
    pub fn path(&self, service: &str) -> String {
        self.0
            .join(format!("{service}.sqlite"))
            .to_str()
            .unwrap()
            .to_owned()
    }
}
impl Default for Dbs {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for Dbs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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
    pub analytics: Arc<Analytics>,
    pub payment: Arc<Payment>,
    pub client: HttpClient,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for System {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
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

pub fn client() -> HttpClient {
    HttpClient::new(Duration::from_secs(5))
}

/// Opens the payment orchestrator on its database against the given services.
pub fn payment(dbs: &Dbs, basket: &str, inventory: &str, timeout: Duration) -> Payment {
    Payment::new(
        Arc::new(payment_service::Store::open(&dbs.path("payment"), false).unwrap()),
        payment_service::Upstreams {
            basket: basket.to_owned(),
            inventory: inventory.to_owned(),
        },
        HttpClient::new(timeout),
    )
}

/// Opens an analytics instance on its database that consumes `basket`'s feed.
pub fn analytics(dbs: &Dbs, config: &Config, basket: &str) -> Analytics {
    let store =
        analytics_service::Store::open(&dbs.path("analytics"), &config.analytics(), false).unwrap();
    Analytics::new(store, basket.to_owned(), client()).unwrap()
}

/// Starts every service in dependency order, each on its own database
/// (dropping and reseeding it with `reset`), then the gateway.
pub async fn launch(dbs: &Dbs, config: &Config, reset: bool) -> System {
    let client = client();
    let inventory_store =
        inventory_service::Store::open(&dbs.path("inventory"), &config.inventory(), reset).unwrap();
    let (inventory, a) = listen(inventory_service::router(Arc::new(inventory_store))).await;

    let basket_store = Arc::new(basket_service::Store::open(&dbs.path("basket"), reset).unwrap());
    basket_service::sync_catalog(&basket_store, &client, &inventory, Duration::from_secs(5))
        .await
        .unwrap();
    let (basket, b) = listen(basket_service::router(basket_store)).await;

    if reset {
        payment_service::Store::open(&dbs.path("payment"), true).unwrap();
        analytics_service::Store::open(&dbs.path("analytics"), &config.analytics(), true).unwrap();
    }
    let payment_ = Arc::new(payment(dbs, &basket, &inventory, Duration::from_secs(5)));
    payment_.recover(true).await.unwrap();
    let (payment_url, c) = listen(payment_service::router(payment_.clone())).await;

    let analytics_ = Arc::new(analytics(dbs, config, &basket));
    let (analytics_url, d) = listen(analytics_service::router(analytics_.clone())).await;

    let urls = Urls {
        inventory,
        basket,
        payment: payment_url,
        analytics: analytics_url,
    };
    System {
        gateway: checkout_gateway::router(Upstreams {
            inventory: urls.inventory.clone(),
            basket: urls.basket.clone(),
            payment: urls.payment.clone(),
            analytics: urls.analytics.clone(),
        }),
        urls,
        analytics: analytics_,
        payment: payment_,
        client,
        tasks: vec![a, b, c, d],
    }
}

impl System {
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
    /// POSTs directly to a service URL, bypassing the gateway.
    pub async fn internal(&self, url: &str, body: Value) -> (u16, Value) {
        let reply = self.client.post(url, &body).await.unwrap();
        (reply.status.as_u16(), reply.body)
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
}
