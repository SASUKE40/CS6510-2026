use inventory_service::{NAME, Settings, Store, publish_catalog, router, spawn_consumer};
use service_kit::{Args, Bus};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "inventory-service [--port=8081] [--db=inventory.sqlite] [--reset=false]\n  [--catalog-size=2000] [--stock=10000] [--threshold=50]\n  [--kafka=127.0.0.1:9092] [--topic-prefix=checkout]",
        &[
            "--port",
            "--db",
            "--reset",
            "--catalog-size",
            "--stock",
            "--threshold",
            "--kafka",
            "--topic-prefix",
        ],
    )?;
    let defaults = Settings::default();
    let settings = Settings {
        catalog_size: args.number("--catalog-size", defaults.catalog_size)?,
        initial_stock: args.number("--stock", defaults.initial_stock)?,
        threshold: args.number("--threshold", defaults.threshold)?,
    };
    let path = args.string("--db", "inventory.sqlite");
    let store = Arc::new(Store::open(
        &path,
        &settings,
        args.number("--reset", false)?,
    )?);
    println!("{NAME}: {path} {settings:?}");
    let bus = Bus::connect(
        &args.string("--kafka", "127.0.0.1:9092"),
        &args.string("--topic-prefix", "checkout"),
        Duration::from_secs(30),
    )
    .await?;
    let published = publish_catalog(&store, &bus).await?;
    println!("{NAME}: published a catalog of {published} items");
    let consumer = spawn_consumer(store.clone(), bus.clone()).await?;
    service_kit::serve(NAME, args.number("--port", 8081)?, router(store)).await?;
    consumer.abort();
    let _ = consumer.await;
    bus.flush().await?;
    Ok(())
}
