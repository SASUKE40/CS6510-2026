use analytics_service::{Analytics, NAME, router, spawn_refresher};
use checkout_data::Store;
use service_kit::Args;
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "analytics-service [--port=8084] [--db=checkout.sqlite] [--refresh-ms=100]",
        &["--port", "--db", "--refresh-ms"],
    )?;
    let analytics = Arc::new(Analytics::new(Store::open(
        &args.string("--db", "checkout.sqlite"),
    )?)?);
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
    analytics.refresh()?;
    println!("{NAME}: final window published");
    Ok(())
}
