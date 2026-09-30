//! Popular-items analytics as a pipes-and-filters pipeline. Checkout requests
//! only enqueue accepted scans; the filters compute and persist the windows.
mod filters;
mod pipes;

use crate::{
    database::Database,
    error::{Error, Result},
};
use filters::{Journal, Publish, Rank, Sequence, Window};
use pipes::{Inlet, Message, pipe, spawn_filter, spawn_sink};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, mpsc::sync_channel},
    thread::JoinHandle,
};

pub(crate) const STAGES: &str = "sequence -> journal -> window -> rank -> publish";

/// Entry to the pipeline, shared by request handlers.
#[derive(Clone)]
pub(crate) struct Analytics {
    input: Inlet<String>,
}

/// Owns the filter threads so the server can drain them on shutdown.
pub struct Pipeline {
    input: Inlet<String>,
    filters: Vec<JoinHandle<()>>,
}

impl Pipeline {
    /// Waits until every scan accepted so far has been journaled and
    /// published, then stops all filters.
    pub fn shutdown(self) {
        let _ = self.input.send(Message::Stop);
        for filter in self.filters {
            let _ = filter.join();
        }
    }
}

pub(crate) fn start(database: Arc<Database>) -> Result<(Analytics, Pipeline)> {
    let config = database.config().clone();
    let (recent, catalog, snapshot) = database.read(|repo| {
        Ok((
            repo.recent_scans(config.window_size)?,
            repo.catalog()?,
            repo.popularity()?,
        ))
    })?;
    let last = recent.last().map_or(0, |scan| scan.sequence);
    let names: HashMap<_, _> = catalog
        .into_iter()
        .map(|item| (item.sku, item.name))
        .collect();

    let (source, sequence_in) = pipe();
    let (sequence_out, journal_in) = pipe();
    let (journal_out, window_in) = pipe();
    let (window_out, rank_in) = pipe();
    let (rank_out, publish_in) = pipe();
    let filters = vec![
        spawn_filter("sequence", Sequence { last }, sequence_in, sequence_out),
        spawn_filter(
            "journal",
            Journal {
                database: database.clone(),
                window_size: config.window_size,
            },
            journal_in,
            journal_out,
        ),
        spawn_filter(
            "window",
            Window::restore(&config, recent),
            window_in,
            window_out,
        ),
        spawn_filter(
            "rank",
            Rank {
                names,
                window_size: config.window_size,
                slide_interval: config.slide_interval,
            },
            rank_in,
            rank_out,
        ),
        spawn_sink(
            "publish",
            Publish {
                database,
                current: Arc::new(snapshot),
            },
            publish_in,
        ),
    ];
    Ok((
        Analytics {
            input: source.clone(),
        },
        Pipeline {
            input: source,
            filters,
        },
    ))
}

impl Analytics {
    /// Called only after the scan has committed; never blocks on analytics
    /// work beyond waiting for room in the first pipe.
    pub fn record_scan(&self, sku: String) {
        if self.input.send(Message::Data(sku)).is_err() {
            eprintln!("analytics pipeline is not running; scan was not counted");
        }
    }

    /// Returns the latest hopping-window snapshot that includes every scan
    /// acknowledged before this call.
    pub fn popular(&self, limit: usize) -> Result<Value> {
        let (reply, answer) = sync_channel(1);
        self.input
            .send(Message::Barrier(reply))
            .map_err(Error::internal)?;
        let snapshot = answer.recv().map_err(Error::internal)?;
        let mut snapshot = Value::clone(&snapshot);
        if let Some(items) = snapshot["items"].as_array_mut() {
            items.truncate(limit);
        }
        Ok(snapshot)
    }
}
