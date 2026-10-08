use checkout_data::Store;
use service_kit::Args;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "basket-service [--port=8082] [--db=checkout.sqlite]",
        &["--port", "--db"],
    )?;
    let store = Store::open(&args.string("--db", "checkout.sqlite"))?;
    let router = basket_service::router(Arc::new(store));
    service_kit::serve(basket_service::NAME, args.number("--port", 8082)?, router).await?;
    Ok(())
}
