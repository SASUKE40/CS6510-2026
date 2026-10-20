use inventory_service::{NAME, Settings, Store, router};
use service_kit::Args;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "inventory-service [--port=8081] [--db=inventory.sqlite] [--reset=false]\n  [--catalog-size=2000] [--stock=10000] [--threshold=50]",
        &[
            "--port",
            "--db",
            "--reset",
            "--catalog-size",
            "--stock",
            "--threshold",
        ],
    )?;
    let defaults = Settings::default();
    let settings = Settings {
        catalog_size: args.number("--catalog-size", defaults.catalog_size)?,
        initial_stock: args.number("--stock", defaults.initial_stock)?,
        threshold: args.number("--threshold", defaults.threshold)?,
    };
    let path = args.string("--db", "inventory.sqlite");
    let store = Store::open(&path, &settings, args.number("--reset", false)?)?;
    println!("{NAME}: {path} {settings:?}");
    service_kit::serve(NAME, args.number("--port", 8081)?, router(Arc::new(store))).await?;
    Ok(())
}
