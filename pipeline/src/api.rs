use crate::{
    Services,
    error::{Error, Kind, Result},
};
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
use serde::Deserialize;
use serde_json::{Value, json};

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self.kind {
            Kind::Invalid => StatusCode::BAD_REQUEST,
            Kind::NotFound => StatusCode::NOT_FOUND,
            Kind::Conflict => StatusCode::CONFLICT,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(json!({"error": self.code, "message": self.message})),
        )
            .into_response()
    }
}
// Service/database calls are synchronous; keep them off async I/O workers.
async fn access<T: Send + 'static>(
    services: Services,
    f: impl FnOnce(Services) -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || f(services))
        .await
        .map_err(Error::internal)?
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
        .map_err(|e| Error::bad(e.body_text()))
}
async fn items(State(s): State<Services>) -> Result<Json<Value>> {
    access(s, |services| services.inventory.catalog())
        .await
        .map(Json)
}
async fn start(
    State(s): State<Services>,
    input: std::result::Result<Json<Start>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>)> {
    let request = body(input)?;
    Ok((
        StatusCode::CREATED,
        Json(
            access(s, move |services| {
                services.transactions.start(&request.station_id)
            })
            .await?,
        ),
    ))
}
async fn scan(
    State(s): State<Services>,
    Path(id): Path<String>,
    input: std::result::Result<Json<Scan>, JsonRejection>,
) -> Result<Json<Value>> {
    let request = body(input)?;
    access(s, move |services| {
        services.transactions.scan(&id, &request.sku)
    })
    .await
    .map(Json)
}
async fn complete(State(s): State<Services>, Path(id): Path<String>) -> Result<Json<Value>> {
    access(s, move |services| services.transactions.complete(&id))
        .await
        .map(Json)
}
async fn get_status(State(s): State<Services>, Path(id): Path<String>) -> Result<Json<Value>> {
    access(s, move |services| services.transactions.status(&id))
        .await
        .map(Json)
}
async fn low_stock(
    State(s): State<Services>,
    query: std::result::Result<Query<LowStockQuery>, QueryRejection>,
) -> Result<Json<Value>> {
    let Query(query) = query.map_err(|e| Error::bad(e.body_text()))?;
    access(s, move |services| {
        services.inventory.low_stock(query.threshold)
    })
    .await
    .map(Json)
}
async fn popular(
    State(s): State<Services>,
    query: std::result::Result<Query<PopularQuery>, QueryRejection>,
) -> Result<Json<Value>> {
    let Query(query) = query.map_err(|e| Error::bad(e.body_text()))?;
    access(s, move |services| {
        services.analytics.popular(query.limit.unwrap_or(10))
    })
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

pub(crate) fn router(services: Services) -> Router {
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
        .fallback(|| async { Error::missing("No such route") })
        .method_not_allowed_fallback(|| async {
            (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(json!({"error": "METHOD_NOT_ALLOWED", "message": "Method not allowed"})),
            )
        })
        .with_state(services)
}
