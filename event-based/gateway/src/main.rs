use checkout_gateway::{NAME, Upstreams, router};
use service_kit::Args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "checkout-gateway [--port=8080]\n  [--inventory=http://127.0.0.1:8081] [--basket=http://127.0.0.1:8082]\n  [--payment=http://127.0.0.1:8083] [--analytics=http://127.0.0.1:8084]",
        &[
            "--port",
            "--inventory",
            "--basket",
            "--payment",
            "--analytics",
        ],
    )?;
    let upstreams = Upstreams {
        inventory: args.string("--inventory", "http://127.0.0.1:8081"),
        basket: args.string("--basket", "http://127.0.0.1:8082"),
        payment: args.string("--payment", "http://127.0.0.1:8083"),
        analytics: args.string("--analytics", "http://127.0.0.1:8084"),
    };
    println!("{NAME} routes: {upstreams:?}");
    let port = args.number("--port", 8080)?;
    println!("API docs: http://localhost:{port}/docs");
    service_kit::serve(NAME, port, router(upstreams)).await?;
    Ok(())
}
