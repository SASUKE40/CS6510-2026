use checkout_pipeline::{Config, Database, PIPELINE_STAGES, app};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    let mut path = String::from("pipeline.sqlite");
    let mut port: u16 = 8080;
    let mut reset = false;
    for arg in std::env::args().skip(1) {
        if arg == "--help" {
            println!(
                "checkout-pipeline [--port=8080] [--db=pipeline.sqlite] [--reset]\n  [--catalog-size=2000] [--stock=10000] [--threshold=50]\n  [--window-size=1000] [--slide-interval=500]\n--reset clears this application's database tables before seeding."
            );
            return Ok(());
        }
        if arg == "--reset" {
            reset = true;
            continue;
        }
        let (key, value) = arg
            .split_once('=')
            .ok_or("Expected --key=value; see --help")?;
        match key {
            "--db" => path = value.to_owned(),
            "--port" => port = value.parse()?,
            "--catalog-size" => config.catalog_size = value.parse()?,
            "--stock" => config.initial_stock = value.parse()?,
            "--threshold" => config.threshold = value.parse()?,
            "--window-size" => config.window_size = value.parse()?,
            "--slide-interval" => config.slide_interval = value.parse()?,
            _ => return Err(format!("Unknown option {key}").into()),
        }
    }
    let store = Database::open(&path, config.clone(), reset)?;
    let (router, pipeline) = app(store)?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let addr = listener.local_addr()?;
    println!("Rust pipeline checkout: http://{addr} database={path} {config:?}");
    println!("Analytics pipeline: {PIPELINE_STAGES}");
    println!("API docs: http://{addr}/docs");
    let served = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    pipeline.shutdown();
    println!("Analytics pipeline drained and stopped");
    Ok(served?)
}
