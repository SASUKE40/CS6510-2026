use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use checkout_monolith::{Config, Store, router};
use serde_json::{Value, json};
use tower::ServiceExt;

fn app(config: Config) -> Router {
    router(Store::open(":memory:", config, false).unwrap())
}
async fn request(app: &Router, method: &str, path: &str, body: Value) -> (u16, Value) {
    raw(app, method, path, &body.to_string()).await
}
async fn raw(app: &Router, method: &str, path: &str, body: &str) -> (u16, Value) {
    let response = app
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
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn start(app: &Router) -> String {
    let (code, result) = request(
        app,
        "POST",
        "/transactions",
        json!({"stationId":"station-1"}),
    )
    .await;
    assert_eq!(code, 201);
    assert_eq!(result["status"], "OPEN");
    result["transactionId"].as_str().unwrap().to_owned()
}
async fn scan(app: &Router, id: &str, sku: &str) -> (u16, Value) {
    request(
        app,
        "POST",
        &format!("/transactions/{id}/items"),
        json!({"sku":sku}),
    )
    .await
}
async fn complete(app: &Router, id: &str) -> (u16, Value) {
    request(
        app,
        "POST",
        &format!("/transactions/{id}/complete"),
        json!({}),
    )
    .await
}
async fn get(app: &Router, path: &str) -> Value {
    let (code, data) = request(app, "GET", path, json!(null)).await;
    assert_eq!(code, 200);
    data
}

#[tokio::test]
async fn catalog_receipt_validation_and_stock() {
    let app = app(Config {
        initial_stock: 2,
        ..Config::default()
    });
    let catalog = get(&app, "/items").await;
    assert_eq!(catalog["items"].as_array().unwrap().len(), 2000);
    assert_eq!(
        catalog["items"][0],
        json!({"sku":"SKU-000001", "name":"Item 1", "price":0.85})
    );
    for body in ["{", "{}", "{\"stationId\":123}", "{\"stationId\":\" \"}"] {
        assert_eq!(raw(&app, "POST", "/transactions", body).await.0, 400);
    }
    let id = start(&app).await;
    assert_eq!(complete(&app, &id).await.1["error"], "EMPTY_BASKET");
    assert_eq!(scan(&app, &id, "missing").await.0, 404);
    assert_eq!(scan(&app, "missing", "SKU-000001").await.0, 404);
    assert_eq!(scan(&app, &id, "SKU-000001").await.0, 200);
    let (code, second) = scan(&app, &id, "SKU-000001").await;
    assert_eq!(code, 200);
    assert_eq!(second["itemCount"], 2);
    assert_eq!(second["runningTotal"], 1.70);
    assert_eq!(
        get(&app, "/inventory/low-stock?threshold=2").await["alerts"],
        json!([])
    );
    let (code, receipt) = complete(&app, &id).await;
    assert_eq!(code, 200);
    assert_eq!(
        receipt["lines"],
        json!([{"sku":"SKU-000001","name":"Item 1","unitPrice":0.85,"quantity":2}])
    );
    assert_eq!(receipt["totalAmount"], 1.70);
    chrono::DateTime::parse_from_rfc3339(receipt["completedAt"].as_str().unwrap()).unwrap();
    assert_eq!(complete(&app, &id).await.0, 409);
    assert_eq!(scan(&app, &id, "SKU-000001").await.0, 409);
    assert_eq!(
        get(&app, &format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );
    let alerts = get(&app, "/inventory/low-stock?threshold=2").await;
    assert_eq!(alerts["alerts"][0]["currentStock"], 0);
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(
        get(&app, "/inventory/low-stock?threshold=2").await["alerts"],
        alerts["alerts"]
    );
    for path in [
        "/inventory/low-stock?threshold=-1",
        "/inventory/low-stock?threshold=x",
        "/analytics/popular-items?limit=-1",
    ] {
        assert_eq!(request(&app, "GET", path, json!(null)).await.0, 400);
    }
    assert_eq!(request(&app, "POST", "/items", json!({})).await.0, 405);
    assert_eq!(request(&app, "GET", "/missing", json!(null)).await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_last_unit_and_duplicate_completion_are_atomic() {
    let app = app(Config {
        initial_stock: 1,
        ..Config::default()
    });
    let mut ids = Vec::new();
    for _ in 0..20 {
        let id = start(&app).await;
        assert_eq!(scan(&app, &id, "SKU-000001").await.0, 200);
        ids.push(id);
    }
    let mut jobs = Vec::new();
    for id in ids {
        let app = app.clone();
        jobs.push(tokio::spawn(async move { complete(&app, &id).await.0 }));
    }
    let mut winners = 0;
    for job in jobs {
        match job.await.unwrap() {
            200 => winners += 1,
            409 => {}
            code => panic!("unexpected {code}"),
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(
        get(&app, "/inventory/low-stock?threshold=1").await["alerts"][0]["currentStock"],
        0
    );

    let id = start(&app).await;
    scan(&app, &id, "SKU-000002").await;
    let (a, b) = tokio::join!(complete(&app, &id), complete(&app, &id));
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [200, 409]);
}

#[tokio::test]
async fn out_of_stock_rolls_back_earlier_lines() {
    let app = app(Config {
        initial_stock: 1,
        ..Config::default()
    });
    let empty_stock = start(&app).await;
    scan(&app, &empty_stock, "SKU-000002").await;
    complete(&app, &empty_stock).await;
    let id = start(&app).await;
    scan(&app, &id, "SKU-000001").await;
    scan(&app, &id, "SKU-000002").await;
    assert_eq!(complete(&app, &id).await.1["error"], "INSUFFICIENT_STOCK");
    let alerts = get(&app, "/inventory/low-stock?threshold=1").await;
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(alerts["alerts"][0]["sku"], "SKU-000002");
    assert_eq!(
        get(&app, &format!("/transactions/{id}")).await["status"],
        "OPEN"
    );
}

#[tokio::test]
async fn hopping_windows_evict_old_scans_and_remain_stable_between_hops() {
    let app = app(Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    });
    let id = start(&app).await;
    assert_eq!(get(&app, "/analytics/popular-items").await["windowEnd"], 0);
    scan(&app, &id, "SKU-000001").await;
    assert_eq!(
        get(&app, "/analytics/popular-items").await["items"],
        json!([])
    );
    scan(&app, &id, "SKU-000001").await;
    let first = get(&app, "/analytics/popular-items").await;
    assert_eq!(first["windowStart"], 1);
    assert_eq!(first["windowEnd"], 2);
    assert_eq!(first["items"][0]["scanCount"], 2);
    scan(&app, &id, "SKU-000002").await;
    assert_eq!(get(&app, "/analytics/popular-items").await, first);
    scan(&app, &id, "SKU-000002").await;
    let tie = get(&app, "/analytics/popular-items").await;
    assert_eq!(tie["items"][0]["sku"], "SKU-000001");
    scan(&app, &id, "SKU-000003").await;
    scan(&app, &id, "SKU-000003").await;
    let latest = get(&app, "/analytics/popular-items?limit=1").await;
    assert_eq!(latest["windowStart"], 3);
    assert_eq!(latest["windowEnd"], 6);
    assert_eq!(latest["items"].as_array().unwrap().len(), 1);
    assert_eq!(latest["items"][0]["sku"], "SKU-000002");
    assert_eq!(
        get(&app, "/analytics/popular-items?limit=0").await["items"],
        json!([])
    );
}

#[tokio::test]
async fn file_database_recovers_state_and_reset_reseeds() {
    let path = std::env::temp_dir().join(format!(
        "checkout-test-{}-{}.sqlite",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let path = path.to_str().unwrap();
    let config = Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    };
    let app = router(Store::open(path, config.clone(), false).unwrap());
    let id = start(&app).await;
    scan(&app, &id, "SKU-000001").await;
    scan(&app, &id, "SKU-000001").await;
    let saved = get(&app, "/analytics/popular-items").await;
    drop(app);
    let app = router(Store::open(path, config.clone(), false).unwrap());
    assert_eq!(get(&app, "/analytics/popular-items").await, saved);
    assert_eq!(complete(&app, &id).await.0, 200);
    drop(app);
    let app = router(Store::open(path, config.clone(), false).unwrap());
    assert_eq!(
        get(&app, &format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );
    assert_eq!(
        get(&app, "/inventory/low-stock?threshold=10000").await["alerts"][0]["currentStock"],
        9998
    );
    drop(app);
    assert!(Store::open(path, Config::default(), false).is_err());
    let app = router(Store::open(path, config, true).unwrap());
    assert_eq!(
        get(&app, "/inventory/low-stock?threshold=10000").await["alerts"],
        json!([])
    );
    assert_eq!(get(&app, "/analytics/popular-items").await["windowEnd"], 0);
    assert_eq!(
        request(&app, "GET", &format!("/transactions/{id}"), json!(null))
            .await
            .0,
        404
    );
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_racing_completion_cannot_change_a_paid_basket() {
    let app = app(Config::default());
    for _ in 0..25 {
        let id = start(&app).await;
        scan(&app, &id, "SKU-000001").await;
        let (scanned, paid) = tokio::join!(scan(&app, &id, "SKU-000002"), complete(&app, &id));
        assert_eq!(paid.0, 200);
        let expected_count = match scanned.0 {
            200 => 2,
            409 => 1,
            code => panic!("unexpected scan response {code}"),
        };
        assert_eq!(paid.1["itemCount"], expected_count);
        let state = get(&app, &format!("/transactions/{id}")).await;
        assert_eq!(state["status"], "COMPLETED");
        assert_eq!(state["itemCount"], paid.1["itemCount"]);
        assert_eq!(state["runningTotal"], paid.1["totalAmount"]);
    }
    let alerts = get(&app, "/inventory/low-stock?threshold=10000").await;
    assert_eq!(alerts["alerts"][0]["currentStock"], 9975);
}

#[tokio::test]
async fn default_window_boundaries_are_based_on_scans_not_transactions() {
    let app = app(Config::default());
    let id = start(&app).await;
    for (sku, end) in [
        ("SKU-000001", 500),
        ("SKU-000002", 1000),
        ("SKU-000003", 1500),
    ] {
        for _ in 0..500 {
            assert_eq!(scan(&app, &id, sku).await.0, 200);
        }
        let snapshot = get(&app, "/analytics/popular-items").await;
        assert_eq!(snapshot["windowEnd"], end);
        assert_eq!(snapshot["windowStart"], (end - 999).max(1));
        assert_eq!(snapshot["items"][0]["scanCount"], 500);
        let counts: i64 = snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["scanCount"].as_i64().unwrap())
            .sum();
        assert_eq!(counts, end.min(1000));
    }
    let latest = get(&app, "/analytics/popular-items").await;
    assert_eq!(latest["items"][0]["sku"], "SKU-000002");
    assert_eq!(latest["items"][1]["sku"], "SKU-000003");
}
