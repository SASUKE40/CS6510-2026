use analytics_service::{Analytics, NAME, Settings, Store, router, spawn_consumer};
use service_kit::{Args, Bus};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "analytics-service [--port=8084] [--db=analytics.sqlite] [--reset=false]\n  [--window-size=1000] [--slide-interval=500]\n  [--kafka=127.0.0.1:9092] [--topic-prefix=checkout]",
        &[
            "--port",
            "--db",
            "--reset",
            "--window-size",
            "--slide-interval",
            "--kafka",
            "--topic-prefix",
        ],
    )?;
    let defaults = Settings::default();
    let settings = Settings {
        window_size: args.number("--window-size", defaults.window_size)?,
        slide_interval: args.number("--slide-interval", defaults.slide_interval)?,
    };
    let store = Store::open(
        &args.string("--db", "analytics.sqlite"),
        &settings,
        args.number("--reset", false)?,
    )?;
    let bus = Bus::connect(
        &args.string("--kafka", "127.0.0.1:9092"),
        &args.string("--topic-prefix", "checkout"),
        Duration::from_secs(30),
    )
    .await?;
    println!(
        "{NAME}: {settings:?}, consuming {}",
        bus.name(service_kit::Topic::Scans)
    );
    let analytics = Arc::new(Analytics::new(store, bus)?);
    let consumer = spawn_consumer(analytics.clone()).await?;
    service_kit::serve(
        NAME,
        args.number("--port", 8084)?,
        router(analytics.clone()),
    )
    .await?;
    let snapshot = analytics.catch_up(Duration::from_secs(10)).await?;
    consumer.abort();
    let _ = consumer.await;
    println!(
        "{NAME}: final window {}..{} published",
        snapshot["windowStart"], snapshot["windowEnd"]
    );
    Ok(())
}
