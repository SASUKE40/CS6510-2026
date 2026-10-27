use basket_service::{NAME, Store, router, spawn_consumer, start_relay, sync_catalog};
use service_kit::{Args, Bus, blocking};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "basket-service [--port=8082] [--db=basket.sqlite] [--reset=false]\n  [--kafka=127.0.0.1:9092] [--topic-prefix=checkout]",
        &["--port", "--db", "--reset", "--kafka", "--topic-prefix"],
    )?;
    let store = Arc::new(Store::open(
        &args.string("--db", "basket.sqlite"),
        args.number("--reset", false)?,
    )?);
    let bus = Bus::connect(
        &args.string("--kafka", "127.0.0.1:9092"),
        &args.string("--topic-prefix", "checkout"),
        Duration::from_secs(30),
    )
    .await?;
    let synced = sync_catalog(&store, &bus, Duration::from_secs(30)).await?;
    println!("{NAME}: catalog replica refreshed with {synced} items");
    let (relay, relay_task) = start_relay(store.clone(), bus.clone()).await?;
    println!("{NAME}: relaying scans after #{}", relay.published());
    let consumer = spawn_consumer(store.clone(), bus.clone()).await?;
    service_kit::serve(
        NAME,
        args.number("--port", 8082)?,
        router(store.clone(), relay.clone()),
    )
    .await?;
    consumer.abort();
    let _ = consumer.await;
    let last = blocking(move || store.last_sequence()).await?;
    if !relay.wait_for(last, Duration::from_secs(10)).await {
        eprintln!(
            "{NAME}: stopping with scans after #{} still in the outbox",
            relay.published()
        );
    }
    drop(relay_task);
    bus.flush().await?;
    Ok(())
}
