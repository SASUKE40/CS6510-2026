use analytics_service::{Analytics, NAME, Settings, Store, router, spawn_refresher};
use service_kit::{Args, HttpClient};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "analytics-service [--port=8084] [--db=analytics.sqlite] [--reset=false]\n  [--basket=http://127.0.0.1:8082] [--window-size=1000] [--slide-interval=500] [--refresh-ms=100]",
        &[
            "--port",
            "--db",
            "--reset",
            "--basket",
            "--window-size",
            "--slide-interval",
            "--refresh-ms",
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
    let basket = args.string("--basket", "http://127.0.0.1:8082");
    println!("{NAME}: {settings:?}, consuming scan events from {basket}");
    let analytics = Arc::new(Analytics::new(
        store,
        basket,
        HttpClient::new(Duration::from_secs(5)),
    )?);
    let refresher = spawn_refresher(
        analytics.clone(),
        Duration::from_millis(args.number("--refresh-ms", 100)?),
    );
    service_kit::serve(
        NAME,
        args.number("--port", 8084)?,
        router(analytics.clone()),
    )
    .await?;
    refresher.abort();
    let snapshot = analytics.refresh().await?;
    println!(
        "{NAME}: final window {}..{} published",
        snapshot["windowStart"], snapshot["windowEnd"]
    );
    Ok(())
}
