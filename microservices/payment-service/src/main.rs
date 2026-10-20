use payment_service::{NAME, Payment, Store, Upstreams, router, spawn_recovery};
use service_kit::{Args, HttpClient};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "payment-service [--port=8083] [--db=payment.sqlite] [--reset=false]\n  [--basket=http://127.0.0.1:8082] [--inventory=http://127.0.0.1:8081]\n  [--call-timeout-ms=5000] [--recovery-ms=1000]",
        &[
            "--port",
            "--db",
            "--reset",
            "--basket",
            "--inventory",
            "--call-timeout-ms",
            "--recovery-ms",
        ],
    )?;
    let store = Store::open(
        &args.string("--db", "payment.sqlite"),
        args.number("--reset", false)?,
    )?;
    let upstreams = Upstreams {
        basket: args.string("--basket", "http://127.0.0.1:8082"),
        inventory: args.string("--inventory", "http://127.0.0.1:8081"),
    };
    println!("{NAME} orchestrates: {upstreams:?}");
    let client = HttpClient::new(Duration::from_millis(
        args.number("--call-timeout-ms", 5000)?,
    ));
    let payment = Arc::new(Payment::new(Arc::new(store), upstreams, client));
    // Nothing is in flight yet, so every unfinished saga belongs to a
    // previous run. Downstream services may still be down; the periodic
    // pass retries whatever this one cannot settle.
    let settled = payment.recover(true).await?;
    println!("{NAME}: startup recovery settled {settled} saga(s)");
    let recovery = spawn_recovery(
        payment.clone(),
        Duration::from_millis(args.number("--recovery-ms", 1000)?),
    );
    service_kit::serve(NAME, args.number("--port", 8083)?, router(payment.clone())).await?;
    recovery.abort();
    Ok(())
}
