//! One-shot schema owner for the shared database. Run it before starting the
//! services; run it with `--reset=true` only while every service is stopped.
use checkout_data::{Settings, admin};
use service_kit::Args;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(
        "checkout-db-admin [--db=checkout.sqlite] [--reset=false]\n  [--catalog-size=2000] [--stock=10000] [--threshold=50]\n  [--window-size=1000] [--slide-interval=500]\nCreates and seeds the shared database; --reset=true drops all application tables first.",
        &[
            "--db",
            "--reset",
            "--catalog-size",
            "--stock",
            "--threshold",
            "--window-size",
            "--slide-interval",
        ],
    )?;
    let defaults = Settings::default();
    let settings = Settings {
        catalog_size: args.number("--catalog-size", defaults.catalog_size)?,
        initial_stock: args.number("--stock", defaults.initial_stock)?,
        threshold: args.number("--threshold", defaults.threshold)?,
        window_size: args.number("--window-size", defaults.window_size)?,
        slide_interval: args.number("--slide-interval", defaults.slide_interval)?,
    };
    let path = args.string("--db", "checkout.sqlite");
    let reset = args.number("--reset", false)?;
    match admin::initialize(&path, &settings, reset)? {
        admin::Outcome::Seeded => println!("Seeded {path} {settings:?}"),
        admin::Outcome::AlreadyInitialized => println!("{path} already initialized {settings:?}"),
    }
    Ok(())
}
