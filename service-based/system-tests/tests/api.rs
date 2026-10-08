use checkout_data::{Settings, Store, admin, popularity, scans};
use checkout_gateway::Upstreams;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use system_tests::{System, launch, start, temp_db};

async fn system(settings: Settings) -> (System, system_tests::TempDb) {
    let db = temp_db();
    (launch(&db, settings, false).await, db)
}

#[tokio::test]
async fn catalog_receipt_validation_and_stock() {
    let (app, _db) = system(Settings {
        initial_stock: 2,
        ..Settings::default()
    })
    .await;
    let catalog = app.get("/items").await;
    assert_eq!(catalog["items"].as_array().unwrap().len(), 2000);
    assert_eq!(
        catalog["items"][0],
        json!({"sku":"SKU-000001", "name":"Item 1", "price":0.85})
    );
    for body in ["{", "{}", "{\"stationId\":123}", "{\"stationId\":\" \"}"] {
        assert_eq!(app.raw("POST", "/transactions", body).await.0, 400);
    }
    let id = app.start().await;
    assert_eq!(app.complete(&id).await.1["error"], "EMPTY_BASKET");
    assert_eq!(app.scan(&id, "missing").await.0, 404);
    assert_eq!(app.scan("missing", "SKU-000001").await.0, 404);
    assert_eq!(app.scan(&id, "SKU-000001").await.0, 200);
    let (code, second) = app.scan(&id, "SKU-000001").await;
    assert_eq!(code, 200);
    assert_eq!(second["itemCount"], 2);
    assert_eq!(second["runningTotal"], 1.70);
    assert_eq!(
        app.get("/inventory/low-stock?threshold=2").await["alerts"],
        json!([])
    );
    let (code, receipt) = app.complete(&id).await;
    assert_eq!(code, 200);
    assert_eq!(
        receipt["lines"],
        json!([{"sku":"SKU-000001","name":"Item 1","unitPrice":0.85,"quantity":2}])
    );
    assert_eq!(receipt["totalAmount"], 1.70);
    chrono::DateTime::parse_from_rfc3339(receipt["completedAt"].as_str().unwrap()).unwrap();
    assert_eq!(app.complete(&id).await.0, 409);
    assert_eq!(app.scan(&id, "SKU-000001").await.0, 409);
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );
    let alerts = app.get("/inventory/low-stock?threshold=2").await;
    assert_eq!(alerts["alerts"][0]["currentStock"], 0);
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(
        app.get("/inventory/low-stock?threshold=2").await["alerts"],
        alerts["alerts"]
    );
    for path in [
        "/inventory/low-stock?threshold=-1",
        "/inventory/low-stock?threshold=x",
        "/analytics/popular-items?limit=-1",
    ] {
        assert_eq!(app.request("GET", path, json!(null)).await.0, 400);
    }
    assert_eq!(app.request("POST", "/items", json!({})).await.0, 405);
    assert_eq!(
        app.request("GET", &format!("/transactions/{id}/complete"), json!(null))
            .await
            .0,
        405
    );
    assert_eq!(app.request("GET", "/missing", json!(null)).await.0, 404);
    assert_eq!(
        app.request("GET", "/transactions/1/x/y", json!(null))
            .await
            .0,
        404
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_last_unit_and_duplicate_completion_are_atomic() {
    let (app, _db) = system(Settings {
        initial_stock: 1,
        ..Settings::default()
    })
    .await;
    let app = Arc::new(app);
    let mut ids = Vec::new();
    for _ in 0..20 {
        let id = app.start().await;
        assert_eq!(app.scan(&id, "SKU-000001").await.0, 200);
        ids.push(id);
    }
    let mut jobs = Vec::new();
    for id in ids {
        let app = app.clone();
        jobs.push(tokio::spawn(async move { app.complete(&id).await.0 }));
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
        app.get("/inventory/low-stock?threshold=1").await["alerts"][0]["currentStock"],
        0
    );

    let id = app.start().await;
    app.scan(&id, "SKU-000002").await;
    let (a, b) = tokio::join!(app.complete(&id), app.complete(&id));
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [200, 409]);
}

#[tokio::test]
async fn out_of_stock_rolls_back_earlier_lines() {
    let (app, _db) = system(Settings {
        initial_stock: 1,
        ..Settings::default()
    })
    .await;
    let empty_stock = app.start().await;
    app.scan(&empty_stock, "SKU-000002").await;
    app.complete(&empty_stock).await;
    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    app.scan(&id, "SKU-000002").await;
    assert_eq!(app.complete(&id).await.1["error"], "INSUFFICIENT_STOCK");
    let alerts = app.get("/inventory/low-stock?threshold=1").await;
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(alerts["alerts"][0]["sku"], "SKU-000002");
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "OPEN"
    );
}

#[tokio::test]
async fn hopping_windows_evict_old_scans_and_remain_stable_between_hops() {
    let (app, _db) = system(Settings {
        window_size: 4,
        slide_interval: 2,
        ..Settings::default()
    })
    .await;
    let id = app.start().await;
    assert_eq!(app.get("/analytics/popular-items").await["windowEnd"], 0);
    app.scan(&id, "SKU-000001").await;
    assert_eq!(
        app.get("/analytics/popular-items").await["items"],
        json!([])
    );
    app.scan(&id, "SKU-000001").await;
    let first = app.get("/analytics/popular-items").await;
    assert_eq!(first["windowStart"], 1);
    assert_eq!(first["windowEnd"], 2);
    assert_eq!(first["items"][0]["scanCount"], 2);
    app.scan(&id, "SKU-000002").await;
    assert_eq!(app.get("/analytics/popular-items").await, first);
    app.scan(&id, "SKU-000002").await;
    let tie = app.get("/analytics/popular-items").await;
    assert_eq!(tie["items"][0]["sku"], "SKU-000001");
    app.scan(&id, "SKU-000003").await;
    app.scan(&id, "SKU-000003").await;
    let latest = app.get("/analytics/popular-items?limit=1").await;
    assert_eq!(latest["windowStart"], 3);
    assert_eq!(latest["windowEnd"], 6);
    assert_eq!(latest["items"].as_array().unwrap().len(), 1);
    assert_eq!(latest["items"][0]["sku"], "SKU-000002");
    assert_eq!(
        app.get("/analytics/popular-items?limit=0").await["items"],
        json!([])
    );
}

#[tokio::test]
async fn restarted_services_recover_state_and_reset_reseeds() {
    let db = temp_db();
    let settings = Settings {
        window_size: 4,
        slide_interval: 2,
        ..Settings::default()
    };
    assert!(Store::open(db.path()).is_err(), "services require db-admin");
    let app = launch(&db, settings.clone(), false).await;
    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    app.scan(&id, "SKU-000001").await;
    let saved = app.get("/analytics/popular-items").await;
    drop(app);
    let app = start(&db).await;
    assert_eq!(app.get("/analytics/popular-items").await, saved);
    assert_eq!(app.complete(&id).await.0, 200);
    drop(app);
    let app = launch(&db, settings.clone(), false).await;
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );
    assert_eq!(
        app.get("/inventory/low-stock?threshold=10000").await["alerts"][0]["currentStock"],
        9998
    );
    drop(app);
    assert!(admin::initialize(db.path(), &Settings::default(), false).is_err());
    let app = launch(&db, settings, true).await;
    assert_eq!(
        app.get("/inventory/low-stock?threshold=10000").await["alerts"],
        json!([])
    );
    assert_eq!(app.get("/analytics/popular-items").await["windowEnd"], 0);
    assert_eq!(
        app.request("GET", &format!("/transactions/{id}"), json!(null))
            .await
            .0,
        404
    );
}

#[tokio::test]
async fn analytics_restart_resumes_the_window_from_the_shared_scan_log() {
    let db = temp_db();
    let app = launch(
        &db,
        Settings {
            window_size: 4,
            slide_interval: 2,
            ..Settings::default()
        },
        false,
    )
    .await;
    let id = app.start().await;
    // Scans committed while nobody reads analytics are still in the log.
    for sku in ["SKU-000001", "SKU-000001", "SKU-000002"] {
        assert_eq!(app.scan(&id, sku).await.0, 200);
    }
    drop(app);

    let app = start(&db).await;
    let restored = app.get("/analytics/popular-items").await;
    assert_eq!(restored["windowEnd"], 2);
    assert_eq!(restored["items"][0]["scanCount"], 2);
    for sku in ["SKU-000002", "SKU-000003", "SKU-000003"] {
        assert_eq!(app.scan(&id, sku).await.0, 200);
    }
    let resumed = app.get("/analytics/popular-items").await;
    assert_eq!(resumed["windowStart"], 3);
    assert_eq!(resumed["windowEnd"], 6);
    assert_eq!(
        resumed["items"],
        json!([
            {"sku":"SKU-000002","name":"Item 2","scanCount":2,"rank":1},
            {"sku":"SKU-000003","name":"Item 3","scanCount":2,"rank":2}
        ])
    );
}

#[tokio::test]
async fn background_refresher_publishes_hops_without_readers() {
    let (app, db) = system(Settings {
        window_size: 4,
        slide_interval: 2,
        ..Settings::default()
    })
    .await;
    let refresher =
        analytics_service::spawn_refresher(app.analytics.clone(), Duration::from_millis(10));
    let id = app.start().await;
    for sku in [
        "SKU-000001",
        "SKU-000002",
        "SKU-000002",
        "SKU-000003",
        "SKU-000003",
        "SKU-000003",
    ] {
        app.scan(&id, sku).await;
    }
    // Nobody has read analytics, so only the refresher can have published.
    let store = Store::open(db.path()).unwrap();
    let mut persisted = json!(null);
    for _ in 0..200 {
        persisted = store.read(popularity::load).unwrap();
        if persisted["windowEnd"] == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    refresher.abort();
    assert_eq!(persisted["windowEnd"], 6);
    let retained = store
        .read(|db| scans::ranked_counts(db, 0, i64::MAX))
        .unwrap();
    assert_eq!(
        retained.iter().map(|r| r.2).sum::<i64>(),
        4,
        "scans 1-2 trimmed"
    );
    let snapshot = app.get("/analytics/popular-items").await;
    assert_eq!(snapshot["windowStart"], 3);
    assert_eq!(snapshot["windowEnd"], 6);
    assert_eq!(
        snapshot["items"][0],
        json!({"sku":"SKU-000003","name":"Item 3","scanCount":3,"rank":1})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scans_are_each_counted_exactly_once() {
    let (app, _db) = system(Settings {
        window_size: 1000,
        slide_interval: 100,
        ..Settings::default()
    })
    .await;
    let app = Arc::new(app);
    let mut jobs = Vec::new();
    for station in 0..20 {
        let app = app.clone();
        jobs.push(tokio::spawn(async move {
            let id = app.start().await;
            for n in 0..40 {
                let sku = format!("SKU-{:06}", 1 + (station + n) % 5);
                assert_eq!(app.scan(&id, &sku).await.0, 200);
            }
        }));
    }
    for job in jobs {
        job.await.unwrap();
    }
    let snapshot = app.get("/analytics/popular-items").await;
    assert_eq!(snapshot["windowStart"], 1);
    assert_eq!(snapshot["windowEnd"], 800);
    let counts: Vec<i64> = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["scanCount"].as_i64().unwrap())
        .collect();
    assert_eq!(counts, vec![160; 5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_racing_completion_across_services_cannot_change_a_paid_basket() {
    let (app, _db) = system(Settings::default()).await;
    for _ in 0..25 {
        let id = app.start().await;
        app.scan(&id, "SKU-000001").await;
        let (scanned, paid) = tokio::join!(app.scan(&id, "SKU-000002"), app.complete(&id));
        assert_eq!(paid.0, 200);
        let expected_count = match scanned.0 {
            200 => 2,
            409 => 1,
            code => panic!("unexpected scan response {code}"),
        };
        assert_eq!(paid.1["itemCount"], expected_count);
        let state = app.get(&format!("/transactions/{id}")).await;
        assert_eq!(state["status"], "COMPLETED");
        assert_eq!(state["itemCount"], paid.1["itemCount"]);
        assert_eq!(state["runningTotal"], paid.1["totalAmount"]);
    }
    let alerts = app.get("/inventory/low-stock?threshold=10000").await;
    assert_eq!(alerts["alerts"][0]["currentStock"], 9975);
}

#[tokio::test]
async fn default_window_boundaries_are_based_on_scans_not_transactions() {
    let (app, _db) = system(Settings::default()).await;
    let id = app.start().await;
    for (sku, end) in [
        ("SKU-000001", 500),
        ("SKU-000002", 1000),
        ("SKU-000003", 1500),
    ] {
        for _ in 0..500 {
            assert_eq!(app.scan(&id, sku).await.0, 200);
        }
        let snapshot = app.get("/analytics/popular-items").await;
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
    let latest = app.get("/analytics/popular-items").await;
    assert_eq!(latest["items"][0]["sku"], "SKU-000002");
    assert_eq!(latest["items"][1]["sku"], "SKU-000003");
}

#[tokio::test]
async fn gateway_routes_by_path_and_reports_unavailable_services() {
    let upstreams = Upstreams {
        inventory: "http://127.0.0.1:1".into(),
        basket: "http://127.0.0.1:2".into(),
        payment: "http://127.0.0.1:3".into(),
        analytics: "http://127.0.0.1:4".into(),
    };
    for (path, service) in [
        ("/items", Some("inventory")),
        ("/inventory/low-stock", Some("inventory")),
        ("/transactions", Some("basket")),
        ("/transactions/tx-1", Some("basket")),
        ("/transactions/tx-1/items", Some("basket")),
        ("/transactions/tx-1/complete", Some("payment")),
        ("/analytics/popular-items", Some("analytics")),
        ("/transactions/tx-1/refund", None),
        ("/", None),
    ] {
        assert_eq!(upstreams.route(path).map(|r| r.0), service, "{path}");
    }
    let db = temp_db();
    let mut app = launch(&db, Settings::default(), false).await;
    app.gateway = checkout_gateway::router(upstreams);
    let (code, body) = app.request("GET", "/items", json!(null)).await;
    assert_eq!(code, 502);
    assert_eq!(body["error"], "SERVICE_UNAVAILABLE");
    assert_eq!(app.get("/health").await["status"], "UP");
}
