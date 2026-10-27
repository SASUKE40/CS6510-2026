use checkout_gateway::Upstreams;
use serde_json::{Value, json};
use service_kit::Topic;
use std::{sync::Arc, time::Duration};
use system_tests::{Config, Dbs, System, delivery, eventually, launch};

async fn system(config: Config) -> (System, Dbs) {
    let dbs = Dbs::new();
    (launch(&dbs, &config, false).await, dbs)
}

#[tokio::test]
async fn catalog_receipt_validation_and_stock() {
    let (app, _dbs) = system(Config {
        initial_stock: 2,
        ..Config::default()
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
    assert_eq!(app.complete("tx-999999").await.0, 404);
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
    assert_eq!(receipt["transactionId"], id);
    chrono::DateTime::parse_from_rfc3339(receipt["completedAt"].as_str().unwrap()).unwrap();
    assert_eq!(app.complete(&id).await.1["error"], "TRANSACTION_NOT_OPEN");
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
    let (app, _dbs) = system(Config {
        initial_stock: 1,
        ..Config::default()
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
    assert_eq!(app.stock("SKU-000001").await, 0);

    let id = app.start().await;
    app.scan(&id, "SKU-000002").await;
    let (a, b) = tokio::join!(app.complete(&id), app.complete(&id));
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [200, 409]);
    assert_eq!(app.stock("SKU-000002").await, 0);
}

#[tokio::test]
async fn out_of_stock_rolls_back_every_service() {
    let (app, _dbs) = system(Config {
        initial_stock: 1,
        ..Config::default()
    })
    .await;
    let empty_stock = app.start().await;
    app.scan(&empty_stock, "SKU-000002").await;
    app.complete(&empty_stock).await;
    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    app.scan(&id, "SKU-000002").await;
    let (code, error) = app.complete(&id).await;
    assert_eq!(code, 409);
    assert_eq!(error["error"], "INSUFFICIENT_STOCK");
    // Inventory: the first line's decrement was rolled back.
    let alerts = app.get("/inventory/low-stock?threshold=1").await;
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(alerts["alerts"][0]["sku"], "SKU-000002");
    // Basket: released, so it is open and scannable again.
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "OPEN"
    );
    assert_eq!(app.scan(&id, "SKU-000003").await.0, 200);
    // Payment: the saga is aborted and nothing was charged.
    let saga = app.payment.store().get(2).unwrap();
    assert_eq!(saga.state, "ABORTED");
    assert_eq!(
        saga.failure.map(|e| e.code).as_deref(),
        Some("INSUFFICIENT_STOCK")
    );
    assert_eq!(saga.paid_at, None);
}

#[tokio::test]
async fn hopping_windows_evict_old_scans_and_remain_stable_between_hops() {
    let (app, _dbs) = system(Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
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
    let dbs = Dbs::new();
    let config = Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    };
    let app = launch(&dbs, &config, false).await;
    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    app.scan(&id, "SKU-000001").await;
    let saved = app.get("/analytics/popular-items").await;
    drop(app);
    let app = launch(&dbs, &config, false).await;
    assert_eq!(app.get("/analytics/popular-items").await, saved);
    assert_eq!(app.complete(&id).await.0, 200);
    drop(app);
    let app = launch(&dbs, &config, false).await;
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );
    assert_eq!(app.stock("SKU-000001").await, 9998);
    drop(app);
    // Each service validates its own settings against its own database.
    let other = Config::default();
    assert!(
        inventory_service::Store::open(
            &dbs.path("inventory"),
            &Config {
                initial_stock: 5,
                ..other.clone()
            }
            .inventory(),
            false
        )
        .is_err()
    );
    assert!(
        analytics_service::Store::open(&dbs.path("analytics"), &other.analytics(), false).is_err()
    );
    let app = launch(&dbs, &config, true).await;
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
async fn analytics_restart_resumes_the_window_from_the_scan_topic() {
    let dbs = Dbs::new();
    let config = Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    };
    let app = launch(&dbs, &config, false).await;
    let id = app.start().await;
    // Scans accepted while nobody reads analytics are kept in the scan topic.
    for sku in ["SKU-000001", "SKU-000001", "SKU-000002"] {
        assert_eq!(app.scan(&id, sku).await.0, 200);
    }
    drop(app);

    let app = launch(&dbs, &config, false).await;
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
async fn consumer_publishes_hops_without_readers() {
    let (app, _dbs) = system(Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    })
    .await;
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
    // Nobody has read analytics, so only its consumer can have published.
    let store = app.analytics.store().clone();
    let mut persisted = json!(null);
    for _ in 0..1500 {
        persisted = store.snapshot().unwrap();
        if persisted["windowEnd"] == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(persisted["windowEnd"], 6);
    assert_eq!(store.retained().unwrap(), 4, "scans 1-2 trimmed");
    assert_eq!(store.cursor().unwrap(), 6);
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
    let (app, _dbs) = system(Config {
        window_size: 1000,
        slide_interval: 100,
        ..Config::default()
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
    let (app, _dbs) = system(Config::default()).await;
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
    assert_eq!(app.stock("SKU-000001").await, 9975);
}

#[tokio::test]
async fn default_window_boundaries_are_based_on_scans_not_transactions() {
    let (app, _dbs) = system(Config::default()).await;
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
async fn gateway_routes_public_paths_only_and_reports_unavailable_services() {
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
        ("/internal/catalog", None),
        ("/internal/reservations", None),
        ("/internal/baskets/tx-1/checkout", None),
        ("/internal/scan-events", None),
        ("/", None),
    ] {
        assert_eq!(upstreams.route(path).map(|r| r.0), service, "{path}");
    }
    let dbs = Dbs::new();
    let mut app = launch(&dbs, &Config::default(), false).await;
    for path in ["/internal/catalog", "/internal/scan-events"] {
        assert_eq!(app.request("GET", path, json!(null)).await.0, 404, "{path}");
    }
    assert_eq!(
        app.request(
            "POST",
            "/internal/reservations",
            json!({"id": "x", "lines": []})
        )
        .await
        .0,
        404
    );
    app.gateway = checkout_gateway::router(upstreams);
    let (code, body) = app.request("GET", "/items", json!(null)).await;
    assert_eq!(code, 502);
    assert_eq!(body["error"], "SERVICE_UNAVAILABLE");
    assert_eq!(app.get("/health").await["status"], "UP");
}

fn command(kind: &str, saga: &str, tx: &str) -> Value {
    json!({"type": kind, "sagaId": saga, "transactionId": tx})
}

fn reserve(saga: &str, lines: Value) -> Value {
    json!({"type": "Reserve", "sagaId": saga, "lines": lines})
}

#[tokio::test]
async fn inventory_outage_mid_saga_is_compensated_by_recovery() {
    let (app, _dbs) = system(Config {
        saga_timeout: Duration::from_millis(500),
        ..Config::default()
    })
    .await;
    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    app.scan(&id, "SKU-000002").await;
    // Inventory is down: the basket gets locked, then the Reserve command
    // waits in Kafka and the client gives up.
    app.pause("inventory").await;
    let (code, error) = app.complete(&id).await;
    assert_eq!(
        (code, error["error"].as_str()),
        (503, Some("SERVICE_UNAVAILABLE"))
    );
    assert_eq!(app.saga(1).state, "BASKET_LOCKED");
    // The basket stays held: clients see OPEN, but scans are refused.
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "OPEN"
    );
    assert_eq!(
        app.scan(&id, "SKU-000003").await.1["error"],
        "TRANSACTION_NOT_OPEN"
    );

    // Recovery gives up on the saga and queues a Cancel behind the Reserve.
    assert_eq!(
        app.payment.recover(false).await.unwrap(),
        0,
        "within grace period"
    );
    assert_eq!(app.payment.recover(true).await.unwrap(), 1);
    assert_eq!(app.saga(1).state, "COMPENSATING");
    // Inventory is back: it reserves, then cancels; the late StockReserved
    // is ignored, and the saga releases the basket and aborts.
    app.resume("inventory").await;
    let store = app.payment.store().clone();
    eventually("the saga to abort", || Ok(store.get(1)?.state == "ABORTED")).await;
    assert_eq!(
        app.saga(1).failure.map(|e| e.code).as_deref(),
        Some("RECOVERED")
    );
    assert_eq!(app.stock("SKU-000001").await, 10000);
    // The cancel left a tombstone, so a redelivered Reserve is refused.
    let late = app
        .inventory_command(reserve(
            "saga-1",
            json!([{"sku": "SKU-000001", "quantity": 1}]),
        ))
        .await;
    assert_eq!(late["type"], "StockRejected");
    assert_eq!(late["error"], "RESERVATION_CANCELLED");
    assert_eq!(app.stock("SKU-000001").await, 10000);

    assert_eq!(app.scan(&id, "SKU-000003").await.0, 200);
    let (code, receipt) = app.complete(&id).await;
    assert_eq!(code, 200);
    assert_eq!(receipt["itemCount"], 3);
    assert_eq!(app.stock("SKU-000001").await, 9999);
    assert_eq!(app.payment.recover(true).await.unwrap(), 0);
}

#[tokio::test]
async fn stalled_sagas_roll_forward_after_the_pivot_and_back_before_it() {
    let (app, _dbs) = system(Config::default()).await;
    let store = app.payment.store().clone();
    // Each saga below stops where a crashed orchestrator would leave it:
    // the commands ran (handlers called directly), but some replies were lost.

    // Crashed after the pivot: stock reserved and payment recorded.
    let paid = app.start().await;
    app.scan(&paid, "SKU-000001").await;
    app.scan(&paid, "SKU-000001").await;
    let saga = store.start(&paid).unwrap();
    let locked = app
        .basket_command(command("Checkout", &format!("saga-{saga}"), &paid))
        .await;
    assert_eq!(locked["type"], "BasketLocked");
    assert!(store.locked(saga, &locked["basket"]).unwrap());
    let reply = app
        .inventory_command(reserve(
            &format!("saga-{saga}"),
            json!([{"sku": "SKU-000001", "quantity": 2}]),
        ))
        .await;
    assert_eq!(reply["type"], "StockReserved");
    assert!(
        store
            .reserved(saga, 170, "2026-10-19T00:00:00.000Z")
            .unwrap()
    );

    // Crashed after inventory reserved, before the saga log recorded it.
    let reserved = app.start().await;
    app.scan(&reserved, "SKU-000002").await;
    let saga2 = store.start(&reserved).unwrap();
    let locked = app
        .basket_command(command("Checkout", &format!("saga-{saga2}"), &reserved))
        .await;
    assert!(store.locked(saga2, &locked["basket"]).unwrap());
    let reply = app
        .inventory_command(reserve(
            &format!("saga-{saga2}"),
            json!([{"sku": "SKU-000002", "quantity": 1}]),
        ))
        .await;
    assert_eq!(reply["type"], "StockReserved");
    assert_eq!(app.stock("SKU-000002").await, 9999);

    // Crashed after basket locked, before the saga log recorded it.
    let held = app.start().await;
    app.scan(&held, "SKU-000003").await;
    let saga3 = store.start(&held).unwrap();
    app.basket_command(command("Checkout", &format!("saga-{saga3}"), &held))
        .await;

    // A restarting orchestrator re-sends one command per stalled saga.
    assert_eq!(app.payment.recover(true).await.unwrap(), 3);
    eventually("every saga to finish", || {
        Ok([saga, saga2, saga3].iter().all(|&s| {
            matches!(
                store.get(s).map(|s| s.state).as_deref(),
                Ok("COMPLETED" | "ABORTED")
            )
        }))
    })
    .await;
    assert_eq!(app.payment.recover(true).await.unwrap(), 0);

    let forward = store.get(saga).unwrap();
    assert_eq!(forward.state, "COMPLETED");
    assert_eq!(forward.paid_at.as_deref(), Some("2026-10-19T00:00:00.000Z"));
    assert_eq!(
        app.get(&format!("/transactions/{paid}")).await["status"],
        "COMPLETED"
    );
    // A late duplicate checkout from a rolled-back saga cannot re-lock.
    let stale = app
        .basket_command(command("Checkout", &format!("saga-{saga3}"), &held))
        .await;
    assert_eq!(stale["type"], "CheckoutRejected");
    assert_eq!(stale["error"], "SAGA_RELEASED");
    assert_eq!(app.stock("SKU-000001").await, 9998);

    for (saga, tx) in [(saga2, &reserved), (saga3, &held)] {
        assert_eq!(store.get(saga).unwrap().state, "ABORTED");
        assert_eq!(
            app.get(&format!("/transactions/{tx}")).await["status"],
            "OPEN"
        );
        assert_eq!(
            app.complete(tx).await.0,
            200,
            "released basket can be paid again"
        );
    }
    assert_eq!(
        app.stock("SKU-000002").await,
        9999,
        "restocked, then sold once"
    );
}

#[tokio::test]
async fn saga_commands_and_replies_are_idempotent() {
    let (app, _dbs) = system(Config {
        initial_stock: 3,
        ..Config::default()
    })
    .await;
    let line = |sku: &str, quantity: i64| json!({"sku": sku, "quantity": quantity});
    let cancel = |saga: &str| json!({"type": "Cancel", "sagaId": saga});

    let request = reserve("r1", json!([line("SKU-000001", 2)]));
    let first = app.inventory_command(request.clone()).await;
    assert_eq!(
        (&first["type"], &first["replayed"]),
        (&json!("StockReserved"), &json!(false))
    );
    let again = app.inventory_command(request).await;
    assert_eq!(
        (&again["type"], &again["replayed"]),
        (&json!("StockReserved"), &json!(true))
    );
    assert_eq!(app.stock("SKU-000001").await, 1);
    assert_eq!(app.inventory_command(cancel("r1")).await["restocked"], true);
    assert_eq!(
        app.inventory_command(cancel("r1")).await["restocked"],
        false
    );
    assert_eq!(app.stock("SKU-000001").await, 3);

    // A rejection is recorded, so a redelivery is rejected again even
    // after stock has been put back.
    let short = reserve("r2", json!([line("SKU-000001", 1), line("SKU-000002", 4)]));
    let rejected = app.inventory_command(short.clone()).await;
    assert_eq!(
        (&rejected["type"], &rejected["error"], &rejected["kind"]),
        (
            &json!("StockRejected"),
            &json!("INSUFFICIENT_STOCK"),
            &json!("CONFLICT")
        )
    );
    assert_eq!(app.stock("SKU-000001").await, 3);
    assert_eq!(app.inventory_command(short).await, rejected);
    assert_eq!(
        app.inventory_command(cancel("r2")).await["restocked"],
        false
    );
    for (lines, code) in [
        (json!([line("nope", 1)]), "UNKNOWN_SKU"),
        (json!([]), "INVALID_REQUEST"),
        (
            json!([line("SKU-000001", 1), line("SKU-000001", 1)]),
            "INVALID_REQUEST",
        ),
    ] {
        let reply = app.inventory_command(reserve("r3", lines)).await;
        assert_eq!(reply["error"], code);
    }

    let id = app.start().await;
    app.scan(&id, "SKU-000001").await;
    let locked = app.basket_command(command("Checkout", "s1", &id)).await;
    assert_eq!(locked["type"], "BasketLocked");
    assert_eq!(
        locked["basket"]["lines"],
        json!([{"sku":"SKU-000001","name":"Item 1","unitPriceCents":85,"quantity":1}])
    );
    assert_eq!(
        app.basket_command(command("Checkout", "s1", &id)).await,
        locked
    );
    let other = app.basket_command(command("Checkout", "s2", &id)).await;
    assert_eq!(
        (&other["type"], &other["error"]),
        (&json!("CheckoutRejected"), &json!("TRANSACTION_NOT_OPEN"))
    );
    let released = app.basket_command(command("Release", "s2", &id)).await;
    assert_eq!(
        (&released["type"], &released["released"]),
        (&json!("BasketReleased"), &json!(false))
    );
    let mut done = command("Complete", "s2", &id);
    done["completedAt"] = json!("t");
    assert_eq!(app.basket_command(done).await["type"], "CompleteRejected");
    let mut done = command("Complete", "s1", &id);
    done["completedAt"] = json!("2026-10-19T00:00:00.000Z");
    assert_eq!(
        app.basket_command(done.clone()).await["type"],
        "BasketCompleted"
    );
    assert_eq!(app.basket_command(done).await["type"], "BasketCompleted");
    let late = app.basket_command(command("Release", "s1", &id)).await;
    assert_eq!(
        (&late["type"], &late["error"]),
        (&json!("ReleaseRejected"), &json!("TRANSACTION_COMPLETED"))
    );
    assert_eq!(
        app.get(&format!("/transactions/{id}")).await["status"],
        "COMPLETED"
    );

    // A completed saga ignores redelivered replies.
    let paid = app.start().await;
    app.scan(&paid, "SKU-000002").await;
    assert_eq!(app.complete(&paid).await.0, 200);
    for kind in [
        "BasketLocked",
        "StockReserved",
        "BasketCompleted",
        "BasketReleased",
    ] {
        let replay = delivery(
            Topic::BasketEvents,
            json!({"type": kind, "sagaId": "saga-1"}),
        );
        assert!(
            app.payment.on_event(replay).await.unwrap().is_empty(),
            "{kind}"
        );
    }
    assert_eq!(app.saga(1).state, "COMPLETED");

    // Every accepted scan was relayed to the scan topic, in order.
    let newest = app.bus.last(Topic::Scans).await.unwrap().unwrap();
    assert_eq!(
        newest.value,
        json!({"type": "ItemScanned", "sequence": 2, "sku": "SKU-000002", "name": "Item 2"})
    );
}

#[tokio::test]
async fn analytics_outage_serves_last_snapshot_and_catches_up_exactly_once() {
    let config = Config {
        window_size: 4,
        slide_interval: 2,
        ..Config::default()
    };
    let (app, _dbs) = system(config).await;
    let id = app.start().await;
    for sku in ["SKU-000001", "SKU-000001", "SKU-000002"] {
        app.scan(&id, sku).await;
    }
    let first = app.get("/analytics/popular-items").await;
    assert_eq!(first["windowEnd"], 2);
    // Analytics' consumer is down; basket keeps accepting scans.
    app.pause("analytics").await;
    for sku in ["SKU-000002", "SKU-000003", "SKU-000003"] {
        assert_eq!(app.scan(&id, sku).await.0, 200);
    }
    assert_eq!(app.analytics.store().cursor().unwrap(), 3);
    let stale = app
        .analytics
        .catch_up(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(stale["windowEnd"], first["windowEnd"]);
    assert_eq!(stale["items"], first["items"]);
    // Back up: the backlog is consumed exactly once.
    app.resume("analytics").await;
    let caught_up = app.get("/analytics/popular-items").await;
    assert_eq!(caught_up["windowStart"], 3);
    assert_eq!(caught_up["windowEnd"], 6);
    assert_eq!(app.analytics.store().cursor().unwrap(), 6);
    assert_eq!(app.analytics.store().retained().unwrap(), 4);
    assert_eq!(
        app.get("/analytics/popular-items").await["items"],
        caught_up["items"]
    );
}
