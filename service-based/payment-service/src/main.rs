use checkout_data::Store;
use service_kit::Args;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "payment-service [--port=8083] [--db=checkout.sqlite]",
        &["--port", "--db"],
    )?;
    let store = Store::open(&args.string("--db", "checkout.sqlite"))?;
    let router = payment_service::router(Arc::new(store));
    service_kit::serve(payment_service::NAME, args.number("--port", 8083)?, router).await?;
    Ok(())
}
