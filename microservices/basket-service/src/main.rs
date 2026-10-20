use basket_service::{NAME, Store, router, sync_catalog};
use service_kit::{Args, HttpClient};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "basket-service [--port=8082] [--db=basket.sqlite] [--reset=false]\n  [--inventory=http://127.0.0.1:8081]",
        &["--port", "--db", "--reset", "--inventory"],
    )?;
    let store = Arc::new(Store::open(
        &args.string("--db", "basket.sqlite"),
        args.number("--reset", false)?,
    )?);
    let inventory = args.string("--inventory", "http://127.0.0.1:8081");
    let client = HttpClient::new(Duration::from_secs(10));
    let synced = sync_catalog(&store, &client, &inventory, Duration::from_secs(30)).await?;
    println!("{NAME}: catalog replica refreshed with {synced} items from {inventory}");
    service_kit::serve(NAME, args.number("--port", 8082)?, router(store)).await?;
    Ok(())
}
