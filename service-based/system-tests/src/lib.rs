//! Test harness: initializes a temporary database file, starts each domain
//! service with its own connection on an ephemeral port (as separate processes
//! would), and exposes the gateway router that talks to them over TCP.
use analytics_service::Analytics;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use checkout_data::{Settings, Store, admin};
use checkout_gateway::Upstreams;
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tower::ServiceExt;

pub struct System {
    pub gateway: Router,
    pub analytics: Arc<Analytics>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for System {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub fn temp_db() -> TempDb {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    TempDb(std::env::temp_dir().join(format!(
        "checkout-services-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )))
}

pub struct TempDb(PathBuf);
impl TempDb {
    pub fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path()));
        }
    }
}

async fn listen(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}

/// Starts all services on an already-initialized database.
pub async fn start(db: &TempDb) -> System {
    let open = || Arc::new(Store::open(db.path()).unwrap());
    let analytics = Arc::new(Analytics::new(Store::open(db.path()).unwrap()).unwrap());
    let (inventory, a) = listen(inventory_service::router(open())).await;
    let (basket, b) = listen(basket_service::router(open())).await;
    let (payment, c) = listen(payment_service::router(open())).await;
    let (analytics_url, d) = listen(analytics_service::router(analytics.clone())).await;
    System {
        gateway: checkout_gateway::router(Upstreams {
            inventory,
            basket,
            payment,
            analytics: analytics_url,
        }),
        analytics,
        tasks: vec![a, b, c, d],
    }
}

/// Initializes (or resets) the database, then starts all services.
pub async fn launch(db: &TempDb, settings: Settings, reset: bool) -> System {
    admin::initialize(db.path(), &settings, reset).unwrap();
    start(db).await
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
}
