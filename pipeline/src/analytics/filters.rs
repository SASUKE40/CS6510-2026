//! The five stages of the popular-items pipeline:
//! sequence -> journal -> window -> rank -> publish.
use super::pipes::{Filter, Sink, Snapshot};
use crate::{
    config::Config,
    database::Database,
    domain::{Scan, now},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

/// Stamps each accepted scan with the next global scan sequence number.
pub(crate) struct Sequence {
    pub last: i64,
}
impl Filter for Sequence {
    type In = String;
    type Out = Scan;
    fn process(&mut self, batch: Vec<String>) -> Vec<Scan> {
        batch
            .into_iter()
            .map(|sku| {
                self.last += 1;
                Scan {
                    sequence: self.last,
                    sku,
                }
            })
            .collect()
    }
}

/// Appends each batch of scans to SQLite in one transaction and trims scans
/// that can no longer be in any window, so a restart can rebuild the window.
pub(crate) struct Journal {
    pub database: Arc<Database>,
    pub window_size: i64,
}
impl Filter for Journal {
    type In = Scan;
    type Out = Scan;
    fn process(&mut self, batch: Vec<Scan>) -> Vec<Scan> {
        let last = batch.last().map_or(0, |scan| scan.sequence);
        let saved = self.database.write(|repo| {
            repo.append_scans(&batch)?;
            repo.remove_scans_through(last - self.window_size)
        });
        if saved.is_err() {
            eprintln!("journal filter: scans through {last} were not persisted");
        }
        batch
    }
}

pub(crate) struct WindowCounts {
    pub start: i64,
    pub end: i64,
    pub counts: Vec<(String, i64)>,
}

/// Keeps the most recent `window_size` scans with running per-SKU counts and
/// emits the counts every `slide_interval` scans (a hopping window).
pub(crate) struct Window {
    size: i64,
    slide: i64,
    scans: VecDeque<String>,
    counts: HashMap<String, i64>,
}
impl Window {
    pub fn restore(config: &Config, recent: Vec<Scan>) -> Self {
        let mut window = Self {
            size: config.window_size,
            slide: config.slide_interval,
            scans: VecDeque::new(),
            counts: HashMap::new(),
        };
        for scan in recent {
            window.push(scan.sku);
        }
        window
    }
    fn push(&mut self, sku: String) {
        *self.counts.entry(sku.clone()).or_default() += 1;
        self.scans.push_back(sku);
        if self.scans.len() as i64 > self.size {
            let evicted = self.scans.pop_front().expect("window is not empty");
            let count = self
                .counts
                .get_mut(&evicted)
                .expect("evicted SKU is counted");
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&evicted);
            }
        }
    }
}
impl Filter for Window {
    type In = Scan;
    type Out = WindowCounts;
    fn process(&mut self, batch: Vec<Scan>) -> Vec<WindowCounts> {
        let mut hops = Vec::new();
        for scan in batch {
            self.push(scan.sku);
            if scan.sequence % self.slide == 0 {
                hops.push(WindowCounts {
                    start: (scan.sequence - self.size + 1).max(1),
                    end: scan.sequence,
                    counts: self
                        .counts
                        .iter()
                        .map(|(sku, n)| (sku.clone(), *n))
                        .collect(),
                });
            }
        }
        hops
    }
}

/// Orders a window's counts (most scans first, ties by SKU), assigns ranks,
/// and attaches item names to form the API's popular-items snapshot.
pub(crate) struct Rank {
    pub names: HashMap<String, String>,
    pub window_size: i64,
    pub slide_interval: i64,
}
impl Filter for Rank {
    type In = WindowCounts;
    type Out = Value;
    fn process(&mut self, batch: Vec<WindowCounts>) -> Vec<Value> {
        batch
            .into_iter()
            .map(|mut window| {
                window.counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                let items: Vec<_> = window.counts.into_iter().enumerate().map(|(index, (sku, count))| {
                    let name = self.names.get(&sku).cloned().unwrap_or_default();
                    json!({"sku": sku, "name": name, "scanCount": count, "rank": index + 1})
                }).collect();
                json!({"windowSize": self.window_size, "slideInterval": self.slide_interval,
                    "windowStart": window.start, "windowEnd": window.end, "computedAt": now(), "items": items})
            })
            .collect()
    }
}

/// Persists the newest snapshot and serves it to readers.
pub(crate) struct Publish {
    pub database: Arc<Database>,
    pub current: Snapshot,
}
impl Sink for Publish {
    type In = Value;
    fn consume(&mut self, batch: Vec<Value>) {
        let Some(latest) = batch.into_iter().last() else {
            return;
        };
        if self
            .database
            .write(|repo| repo.save_popularity(&latest))
            .is_err()
        {
            eprintln!(
                "publish filter: window ending {} was not persisted",
                latest["windowEnd"]
            );
        }
        self.current = Arc::new(latest);
    }
    fn current(&self) -> Snapshot {
        self.current.clone()
    }
}
