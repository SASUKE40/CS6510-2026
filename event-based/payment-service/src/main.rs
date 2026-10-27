use payment_service::{NAME, Payment, Store, router, spawn_consumer, spawn_recovery};
use service_kit::{Args, Bus};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "payment-service [--port=8083] [--db=payment.sqlite] [--reset=false]\n  [--kafka=127.0.0.1:9092] [--topic-prefix=checkout]\n  [--saga-timeout-ms=10000] [--recovery-ms=1000]",
        &[
            "--port",
            "--db",
            "--reset",
            "--kafka",
            "--topic-prefix",
            "--saga-timeout-ms",
            "--recovery-ms",
        ],
    )?;
    let store = Store::open(
        &args.string("--db", "payment.sqlite"),
        args.number("--reset", false)?,
    )?;
    let bus = Bus::connect(
        &args.string("--kafka", "127.0.0.1:9092"),
        &args.string("--topic-prefix", "checkout"),
        Duration::from_secs(30),
    )
    .await?;
    let timeout = Duration::from_millis(args.number("--saga-timeout-ms", 10_000)?);
    let payment = Arc::new(Payment::new(Arc::new(store), bus.clone(), timeout));
    // Nothing is in flight yet, so every unfinished saga belongs to a
    // previous run; the replies to the re-sent commands finish them.
    let retried = payment.recover(true).await?;
    println!("{NAME}: startup recovery re-sent commands for {retried} saga(s)");
    let consumer = spawn_consumer(payment.clone()).await?;
    let recovery = spawn_recovery(
        payment.clone(),
        Duration::from_millis(args.number("--recovery-ms", 1000)?),
    );
    // Graceful shutdown waits for in-flight completions, whose replies the
    // consumer is still handling.
    service_kit::serve(NAME, args.number("--port", 8083)?, router(payment.clone())).await?;
    recovery.abort();
    consumer.abort();
    let _ = consumer.await;
    bus.flush().await?;
    Ok(())
}
