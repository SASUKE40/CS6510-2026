use checkout_data::Store;
use service_kit::Args;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "inventory-service [--port=8081] [--db=checkout.sqlite]",
        &["--port", "--db"],
    )?;
    let store = Store::open(&args.string("--db", "checkout.sqlite"))?;
    let router = inventory_service::router(Arc::new(store));
    service_kit::serve(
        inventory_service::NAME,
        args.number("--port", 8081)?,
        router,
    )
    .await?;
    Ok(())
}
