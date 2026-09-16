use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
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

pub struct Store {
    connection: Connection,
    config: Config,
}
pub type SharedStore = Arc<Mutex<Store>>;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}
type Result<T> = std::result::Result<T, ApiError>;
impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
    fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "INVALID_REQUEST", message)
    }
    fn conflict(code: &'static str, message: &str) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }
    fn missing(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        eprintln!("Internal error: {error}");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Internal server error",
        )
    }
}
impl From<rusqlite::Error> for ApiError {
    fn from(error: rusqlite::Error) -> Self {
        Self::internal(error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": self.code, "message": self.message})),
        )
            .into_response()
    }
}
fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
fn money(cents: i64) -> f64 {
    cents as f64 / 100.0
}
fn tx_id(id: &str) -> Result<i64> {
    id.strip_prefix("tx-")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| ApiError::missing("No such transaction"))
}

impl Store {
    pub fn open(
        path: &str,
        config: Config,
        reset: bool,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        if config.catalog_size <= 0
            || config.initial_stock < 0
            || config.threshold < 0
            || config.window_size <= 0
            || config.slide_interval <= 0
        {
            return Err(
                "Catalog/window/slide must be positive; stock/threshold must be nonnegative".into(),
            );
        }
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let db = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if reset {
            db.execute_batch("DROP TABLE IF EXISTS lines; DROP TABLE IF EXISTS scans;
                DROP TABLE IF EXISTS stock_changes; DROP TABLE IF EXISTS transactions;
                DROP TABLE IF EXISTS items; DROP TABLE IF EXISTS settings; DROP TABLE IF EXISTS popularity;")?;
        }
        db.execute_batch(include_str!("schema.sql"))?;
        let existing = db.query_row("SELECT catalog_size, initial_stock, threshold, window_size, slide_interval FROM settings WHERE id=1", [], |r| {
            Ok(Config { catalog_size: r.get(0)?, initial_stock: r.get(1)?, threshold: r.get(2)?, window_size: r.get(3)?, slide_interval: r.get(4)? })
        }).optional()?;
        if let Some(existing) = existing {
            if existing != config {
                return Err(
                    "Database configuration differs; use the original settings or --reset".into(),
                );
            }
        } else {
            db.execute(
                "INSERT INTO settings VALUES (1, ?1, ?2, ?3, ?4, ?5)",
                params![
                    config.catalog_size,
                    config.initial_stock,
                    config.threshold,
                    config.window_size,
                    config.slide_interval
                ],
            )?;
            let timestamp = now();
            for i in 1..=config.catalog_size {
                let sku = format!("SKU-{i:06}");
                db.execute(
                    "INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?4)",
                    params![
                        sku,
                        format!("Item {i}"),
                        50 + (i % 47) * 35,
                        config.initial_stock
                    ],
                )?;
                db.execute(
                    "INSERT INTO stock_changes VALUES (?1, ?2, ?3)",
                    params![sku, config.initial_stock, timestamp],
                )?;
            }
            let snapshot = json!({"windowSize": config.window_size, "slideInterval": config.slide_interval,
                "windowStart": 0, "windowEnd": 0, "computedAt": timestamp, "items": []});
            db.execute(
                "INSERT INTO popularity VALUES (1, ?1)",
                [snapshot.to_string()],
            )?;
        }
        db.commit()?;
        Ok(Self { connection, config })
    }

    fn catalog(&self) -> Result<Value> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT sku, name, price_cents FROM items ORDER BY sku")?;
        let items = stmt.query_map([], |r| Ok(json!({"sku": r.get::<_, String>(0)?, "name": r.get::<_, String>(1)?, "price": money(r.get(2)?)})))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(json!({"items": items}))
    }

    fn start(&mut self, station: &str) -> Result<Value> {
        if station.trim().is_empty() {
            return Err(ApiError::bad("stationId must not be blank"));
        }
        let timestamp = now();
        self.connection.execute(
            "INSERT INTO transactions(station_id, started_at) VALUES (?1, ?2)",
            params![station, timestamp],
        )?;
        status(&self.connection, self.connection.last_insert_rowid())
    }

    fn scan(&mut self, id: i64, sku: &str) -> Result<Value> {
        let db = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = status(&db, id)?;
        require_open(&current)?;
        let item = db
            .query_row(
                "SELECT name, price_cents FROM items WHERE sku=?1",
                [sku],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "UNKNOWN_SKU", "No such SKU"))?;
        db.execute("INSERT INTO lines VALUES (?1, ?2, 1) ON CONFLICT(transaction_id, sku) DO UPDATE SET quantity=quantity+1", params![id, sku])?;
        db.execute("UPDATE transactions SET item_count=item_count+1, total_cents=total_cents+?1 WHERE id=?2", params![item.1, id])?;
        db.execute("INSERT INTO scans(sku) VALUES (?1)", [sku])?;
        let sequence = db.last_insert_rowid();
        db.execute(
            "DELETE FROM scans WHERE sequence <= ?1",
            [sequence - self.config.window_size],
        )?;
        if sequence % self.config.slide_interval == 0 {
            let mut stmt = db.prepare("SELECT s.sku, i.name, COUNT(*) AS count FROM scans s JOIN items i ON i.sku=s.sku GROUP BY s.sku ORDER BY count DESC, s.sku ASC")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let items: Vec<_> = rows.into_iter().enumerate().map(|(i, (sku, name, count))| json!({"sku": sku, "name": name, "scanCount": count, "rank": i+1})).collect();
            let snapshot = json!({"windowSize": self.config.window_size, "slideInterval": self.config.slide_interval,
                "windowStart": (sequence-self.config.window_size+1).max(1), "windowEnd": sequence, "computedAt": now(), "items": items});
            db.execute(
                "UPDATE popularity SET snapshot=?1 WHERE id=1",
                [snapshot.to_string()],
            )?;
        }
        let updated = status(&db, id)?;
        db.commit()?;
        Ok(
            json!({"transactionId": format!("tx-{id}"), "sku": sku, "name": item.0, "unitPrice": money(item.1),
            "itemCount": updated["itemCount"], "runningTotal": updated["runningTotal"]}),
        )
    }

    fn complete(&mut self, id: i64) -> Result<Value> {
        // The write lock spans validation, all decrements, and finalization. Any
        // failure rolls the entire basket back, including earlier SKU updates.
        let db = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = status(&db, id)?;
        require_open(&current)?;
        if current["itemCount"] == 0 {
            return Err(ApiError::conflict(
                "EMPTY_BASKET",
                "Cannot complete an empty transaction",
            ));
        }
        let lines = {
            let mut stmt = db.prepare("SELECT l.sku, i.name, i.price_cents, l.quantity FROM lines l JOIN items i ON i.sku=l.sku WHERE l.transaction_id=?1 ORDER BY l.sku")?;
            stmt.query_map([id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        let completed_at = now();
        for (sku, _, _, quantity) in &lines {
            let changed = db.execute(
                "UPDATE items SET stock=stock-?1 WHERE sku=?2 AND stock>=?1",
                params![quantity, sku],
            )?;
            if changed != 1 {
                return Err(ApiError::conflict(
                    "INSUFFICIENT_STOCK",
                    "Insufficient stock to complete the whole basket",
                ));
            }
            db.execute(
                "INSERT INTO stock_changes SELECT sku, stock, ?1 FROM items WHERE sku=?2",
                params![completed_at, sku],
            )?;
        }
        db.execute(
            "UPDATE transactions SET status='COMPLETED', completed_at=?1 WHERE id=?2",
            params![completed_at, id],
        )?;
        let receipt_lines: Vec<_> = lines.into_iter().map(|(sku, name, cents, quantity)| json!({"sku": sku, "name": name, "unitPrice": money(cents), "quantity": quantity})).collect();
        let receipt = json!({"transactionId": current["transactionId"], "stationId": current["stationId"],
            "itemCount": current["itemCount"], "totalAmount": current["runningTotal"], "startedAt": current["startedAt"],
            "completedAt": completed_at, "lines": receipt_lines});
        db.commit()?;
        Ok(receipt)
    }

    fn low_stock(&self, threshold: i64) -> Result<Value> {
        if threshold < 0 {
            return Err(ApiError::bad("threshold must be nonnegative"));
        }
        let mut stmt = self.connection.prepare_cached("SELECT i.sku, i.name, i.stock,
            (SELECT changed_at FROM stock_changes s WHERE s.sku=i.sku AND s.stock<?1 ORDER BY s.stock DESC LIMIT 1)
            FROM items i WHERE i.stock<?1 ORDER BY i.sku")?;
        let alerts = stmt.query_map([threshold], |r| Ok(json!({"sku": r.get::<_, String>(0)?, "name": r.get::<_, String>(1)?,
            "currentStock": r.get::<_, i64>(2)?, "threshold": threshold, "triggeredAt": r.get::<_, String>(3)?})))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(json!({"threshold": threshold, "generatedAt": now(), "alerts": alerts}))
    }

    fn popular(&self, limit: usize) -> Result<Value> {
        let raw: String =
            self.connection
                .query_row("SELECT snapshot FROM popularity WHERE id=1", [], |r| {
                    r.get(0)
                })?;
        let mut snapshot: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;
        if let Some(items) = snapshot["items"].as_array_mut() {
            items.truncate(limit);
        }
        Ok(snapshot)
    }
}

fn status(db: &Connection, id: i64) -> Result<Value> {
    db.query_row("SELECT station_id, status, item_count, total_cents, started_at FROM transactions WHERE id=?1", [id], |r| {
        Ok(json!({"transactionId": format!("tx-{id}"), "stationId": r.get::<_, String>(0)?, "status": r.get::<_, String>(1)?,
            "itemCount": r.get::<_, i64>(2)?, "runningTotal": money(r.get(3)?), "startedAt": r.get::<_, String>(4)?}))
    }).optional()?.ok_or_else(|| ApiError::missing("No such transaction"))
}
fn require_open(current: &Value) -> Result<()> {
    if current["status"] != "OPEN" {
        Err(ApiError::conflict(
            "TRANSACTION_NOT_OPEN",
            "Transaction is not open",
        ))
    } else {
        Ok(())
    }
}

// SQLite is synchronous: never hold its lock or perform disk I/O on a Tokio
// executor thread. Requests queue on the process's single database connection.
async fn access<T: Send + 'static>(
    store: SharedStore,
    f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || {
        let mut db = store.lock().map_err(ApiError::internal)?;
        f(&mut db)
    })
    .await
    .map_err(ApiError::internal)?
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Start {
    station_id: String,
}
#[derive(Deserialize)]
struct Scan {
    sku: String,
}
#[derive(Deserialize)]
struct LowStockQuery {
    threshold: Option<i64>,
}
#[derive(Deserialize)]
struct PopularQuery {
    limit: Option<usize>,
}
fn body<T>(input: std::result::Result<Json<T>, JsonRejection>) -> Result<T> {
    input
        .map(|Json(v)| v)
        .map_err(|e| ApiError::bad(e.body_text()))
}
async fn items(State(s): State<SharedStore>) -> Result<Json<Value>> {
    access(s, |db| db.catalog()).await.map(Json)
}
async fn start(
    State(s): State<SharedStore>,
    input: std::result::Result<Json<Start>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>)> {
    let request = body(input)?;
    Ok((
        StatusCode::CREATED,
        Json(access(s, move |db| db.start(&request.station_id)).await?),
    ))
}
async fn scan(
    State(s): State<SharedStore>,
    Path(id): Path<String>,
    input: std::result::Result<Json<Scan>, JsonRejection>,
) -> Result<Json<Value>> {
    let request = body(input)?;
    let id = tx_id(&id)?;
    access(s, move |db| db.scan(id, &request.sku))
        .await
        .map(Json)
}
async fn complete(State(s): State<SharedStore>, Path(id): Path<String>) -> Result<Json<Value>> {
    let id = tx_id(&id)?;
    access(s, move |db| db.complete(id)).await.map(Json)
}
async fn get_status(State(s): State<SharedStore>, Path(id): Path<String>) -> Result<Json<Value>> {
    let id = tx_id(&id)?;
    access(s, move |db| status(&db.connection, id))
        .await
        .map(Json)
}
async fn low_stock(
    State(s): State<SharedStore>,
    query: std::result::Result<Query<LowStockQuery>, QueryRejection>,
) -> Result<Json<Value>> {
    let Query(query) = query.map_err(|e| ApiError::bad(e.body_text()))?;
    access(s, move |db| {
        db.low_stock(query.threshold.unwrap_or(db.config.threshold))
    })
    .await
    .map(Json)
}
async fn popular(
    State(s): State<SharedStore>,
    query: std::result::Result<Query<PopularQuery>, QueryRejection>,
) -> Result<Json<Value>> {
    let Query(query) = query.map_err(|e| ApiError::bad(e.body_text()))?;
    access(s, move |db| db.popular(query.limit.unwrap_or(10)))
        .await
        .map(Json)
}
async fn docs() -> Html<&'static str> {
    Html(include_str!("docs.html"))
}

async fn openapi() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/yaml; charset=utf-8",
        )],
        include_str!("../../spec/self-checkout-openapi.yaml"),
    )
}

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/docs", get(docs))
        .route("/docs/", get(docs))
        .route("/openapi.yaml", get(openapi))
        .route("/items", get(items))
        .route("/transactions", post(start))
        .route("/transactions/{id}", get(get_status))
        .route("/transactions/{id}/items", post(scan))
        .route("/transactions/{id}/complete", post(complete))
        .route("/inventory/low-stock", get(low_stock))
        .route("/analytics/popular-items", get(popular))
        .fallback(|| async { ApiError::missing("No such route") })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "METHOD_NOT_ALLOWED",
                "Method not allowed",
            )
        })
        .with_state(Arc::new(Mutex::new(store)))
}
