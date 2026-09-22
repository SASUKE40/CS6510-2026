//! Scan-window policy and ranking. Storage is accessed only through Repository.
use crate::{
    config::Config,
    database::{Database, Repository},
    domain::now,
    error::Result,
};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Analytics {
    database: Arc<Database>,
    config: Config,
}
impl Analytics {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            config: database.config().clone(),
            database,
        }
    }
    // Invoked inside the transaction service's existing write unit of work:
    // basket updates and analytics either commit together or roll back together.
    pub fn record_scan(&self, repository: &Repository<'_>, sku: &str) -> Result<()> {
        let sequence = repository.insert_scan(sku)?;
        repository.remove_scans_through(sequence - self.config.window_size)?;
        if sequence % self.config.slide_interval == 0 {
            let mut counts = repository.scan_counts()?;
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.sku.cmp(&b.sku)));
            let items: Vec<_> = counts.into_iter().enumerate().map(|(index, item)|
                json!({"sku": item.sku, "name": item.name, "scanCount": item.count, "rank": index+1})).collect();
            repository.save_popularity(&json!({"windowSize": self.config.window_size,
                "slideInterval": self.config.slide_interval, "windowStart": (sequence-self.config.window_size+1).max(1),
                "windowEnd": sequence, "computedAt": now(), "items": items}))?;
        }
        Ok(())
    }
    pub fn popular(&self, limit: usize) -> Result<Value> {
        self.database.read(|repo| {
            let mut snapshot = repo.popularity()?;
            if let Some(items) = snapshot["items"].as_array_mut() {
                items.truncate(limit);
            }
            Ok(snapshot)
        })
    }
}
