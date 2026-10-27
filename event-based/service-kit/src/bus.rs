//! The event bus every service uses to reach the others: Kafka topics with
//! JSON values. Each topic has one partition, so its events are consumed in
//! the order they were produced.
//!
//! Delivery is at least once. A publish resolves only after Kafka has stored
//! the record (`acks=all`, idempotent producer). A consumer stores a
//! message's offset only after handling it and delivering its replies, so a
//! crash redelivers the message; every handler must therefore be idempotent.
use crate::error::{Error, Result};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer as _, StreamConsumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord, Producer as _},
};
use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot, oneshot::error::TryRecvError},
    task::JoinHandle,
    time::Instant,
};

/// The topics of the event contract, named `<prefix>.<suffix>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Topic {
    /// `CatalogPublished` snapshots from inventory (compacted, key `catalog`).
    Catalog,
    /// `ItemScanned` events relayed from basket's outbox.
    Scans,
    /// `Checkout`, `Release`, `Complete` commands for basket.
    BasketCommands,
    /// basket's replies to its commands.
    BasketEvents,
    /// `Reserve`, `Cancel` commands for inventory.
    InventoryCommands,
    /// inventory's replies to its commands.
    InventoryEvents,
}

impl Topic {
    pub const ALL: [Topic; 6] = [
        Topic::Catalog,
        Topic::Scans,
        Topic::BasketCommands,
        Topic::BasketEvents,
        Topic::InventoryCommands,
        Topic::InventoryEvents,
    ];

    pub fn suffix(self) -> &'static str {
        match self {
            Topic::Catalog => "inventory.catalog",
            Topic::Scans => "basket.scans",
            Topic::BasketCommands => "basket.commands",
            Topic::BasketEvents => "basket.events",
            Topic::InventoryCommands => "inventory.commands",
            Topic::InventoryEvents => "inventory.events",
        }
    }
}

/// One consumed record. `value` is `Null` if the payload was not JSON.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub topic: Topic,
    pub key: String,
    pub value: Value,
    pub partition: i32,
    pub offset: i64,
}

impl Delivery {
    /// The event or command name in the value's `type` field.
    pub fn kind(&self) -> &str {
        self.value["type"].as_str().unwrap_or_default()
    }
}

/// A record to publish.
pub struct Outgoing {
    pub topic: Topic,
    pub key: String,
    pub value: Value,
}

impl Outgoing {
    pub fn new(topic: Topic, key: impl Into<String>, value: Value) -> Self {
        Self {
            topic,
            key: key.into(),
            value,
        }
    }
}

/// Resolves once Kafka has stored a sent record.
pub type Pending = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How many records [`run`] handles at once.
pub const IN_FLIGHT: usize = 64;

fn kafka(error: impl std::fmt::Display) -> Error {
    Error::unavailable(format!("Kafka: {error}"))
}

#[derive(Clone)]
pub struct Bus {
    producer: FutureProducer,
    bootstrap: String,
    prefix: String,
}

impl Bus {
    /// Connects to `bootstrap` and creates any missing topic under `prefix`,
    /// waiting up to `wait` for the broker to answer.
    pub async fn connect(bootstrap: &str, prefix: &str, wait: Duration) -> Result<Self> {
        let producer = client_config(bootstrap)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "1")
            .set("message.timeout.ms", "60000")
            .create()
            .map_err(kafka)?;
        let bus = Self {
            producer,
            bootstrap: bootstrap.to_owned(),
            prefix: prefix.to_owned(),
        };
        bus.create_topics(wait).await?;
        Ok(bus)
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn name(&self, topic: Topic) -> String {
        format!("{}.{}", self.prefix, topic.suffix())
    }

    fn topic_of(&self, name: &str) -> Option<Topic> {
        Topic::ALL.into_iter().find(|t| self.name(*t) == name)
    }

    async fn create_topics(&self, wait: Duration) -> Result<()> {
        let admin: AdminClient<DefaultClientContext> =
            client_config(&self.bootstrap).create().map_err(kafka)?;
        let names: Vec<_> = Topic::ALL.iter().map(|t| self.name(*t)).collect();
        let topics: Vec<_> = Topic::ALL
            .iter()
            .zip(&names)
            .map(|(topic, name)| {
                let new = NewTopic::new(name, 1, TopicReplication::Fixed(1));
                if *topic == Topic::Catalog {
                    new.set("cleanup.policy", "compact")
                } else {
                    new
                }
            })
            .collect();
        let options = AdminOptions::new().operation_timeout(Some(REQUEST_TIMEOUT));
        let deadline = Instant::now() + wait;
        loop {
            match admin.create_topics(&topics, &options).await {
                Ok(results) => {
                    for result in results {
                        match result {
                            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                            Err((name, code)) => {
                                return Err(kafka(format!("creating {name}: {code}")));
                            }
                        }
                    }
                    return Ok(());
                }
                Err(error) if Instant::now() < deadline => {
                    eprintln!("waiting for Kafka at {}: {error}", self.bootstrap);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => return Err(kafka(error)),
            }
        }
    }

    /// Enqueues a record behind everything enqueued before it. The returned
    /// future resolves once Kafka has stored the record.
    pub fn send(&self, record: &Outgoing) -> Result<Pending> {
        let name = self.name(record.topic);
        let payload = record.value.to_string();
        let delivery = self
            .producer
            .send_result(FutureRecord::to(&name).key(&record.key).payload(&payload))
            .map_err(|(error, _)| kafka(error))?;
        Ok(Box::pin(async move {
            match delivery.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err((error, _))) => Err(kafka(error)),
                Err(_) => Err(kafka("delivery cancelled")),
            }
        }))
    }

    /// Publishes one record and waits until Kafka has stored it.
    pub async fn publish(&self, topic: Topic, key: &str, value: Value) -> Result<()> {
        self.send(&Outgoing::new(topic, key, value))?.await
    }

    /// Waits until every enqueued record is delivered (used at shutdown).
    pub async fn flush(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || producer.flush(REQUEST_TIMEOUT))
            .await
            .map_err(Error::internal)?
            .map_err(kafka)
    }

    /// The offset the next record of `topic` will get.
    pub async fn end_offset(&self, topic: Topic) -> Result<i64> {
        let producer = self.producer.clone();
        let name = self.name(topic);
        tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_watermarks(&name, 0, REQUEST_TIMEOUT)
        })
        .await
        .map_err(Error::internal)?
        .map(|(_, high)| high)
        .map_err(kafka)
    }

    /// The newest record of `topic`, read without a consumer group.
    pub async fn last(&self, topic: Topic) -> Result<Option<Delivery>> {
        let end = self.end_offset(topic).await?;
        if end == 0 {
            return Ok(None);
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let reader: StreamConsumer = self
            .consumer_config(&format!(
                "reader-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .set("enable.auto.commit", "false")
            .create()
            .map_err(kafka)?;
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&self.name(topic), 0, Offset::Offset(end - 1))
            .map_err(kafka)?;
        reader.assign(&assignment).map_err(kafka)?;
        loop {
            let message = tokio::time::timeout(REQUEST_TIMEOUT, reader.recv())
                .await
                .map_err(|_| kafka(format!("reading the end of {}", self.name(topic))))?
                .map_err(kafka)?;
            if message.offset() >= end - 1 {
                return self.delivery(&message).map(Some);
            }
        }
    }

    /// Joins consumer group `<prefix>.<group>` on `topics`. A new group
    /// starts from the beginning of each topic.
    pub fn subscribe(&self, group: &str, topics: &[Topic]) -> Result<Arc<Consumer>> {
        let inner: StreamConsumer = self
            .consumer_config(group)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "200")
            .set("enable.auto.offset.store", "false")
            .create()
            .map_err(kafka)?;
        let consumer = Consumer {
            inner,
            bus: self.clone(),
            topics: topics.iter().map(|t| self.name(*t)).collect(),
        };
        consumer.subscribe()?;
        Ok(Arc::new(consumer))
    }

    fn consumer_config(&self, group: &str) -> ClientConfig {
        let mut config = client_config(&self.bootstrap);
        config
            .set("group.id", format!("{}.{group}", self.prefix))
            .set("auto.offset.reset", "earliest")
            .set("fetch.wait.max.ms", "100");
        config
    }

    fn delivery(&self, message: &BorrowedMessage<'_>) -> Result<Delivery> {
        let topic = self
            .topic_of(message.topic())
            .ok_or_else(|| Error::internal(format!("unexpected topic {}", message.topic())))?;
        Ok(Delivery {
            topic,
            key: message
                .key()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .unwrap_or_default(),
            value: message
                .payload()
                .and_then(|p| serde_json::from_slice(p).ok())
                .unwrap_or(Value::Null),
            partition: message.partition(),
            offset: message.offset(),
        })
    }
}

fn client_config(bootstrap: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("socket.nagle.disable", "true")
        .set("log_level", "3");
    config
}

pub struct Consumer {
    inner: StreamConsumer,
    bus: Bus,
    topics: Vec<String>,
}

impl Consumer {
    fn subscribe(&self) -> Result<()> {
        let topics: Vec<&str> = self.topics.iter().map(String::as_str).collect();
        self.inner.subscribe(&topics).map_err(kafka)
    }

    /// A consume error can leave a partition assigned but idle; e.g. when
    /// the group coordinator fails the offset fetch, librdkafka reports it
    /// once and never fetches. Rejoining the group fetches the offsets again.
    fn rejoin(&self, error: KafkaError) -> Error {
        self.inner.unsubscribe();
        if let Err(again) = self.subscribe() {
            eprintln!("resubscribing after a consume error failed: {again}");
        }
        kafka(error)
    }

    pub async fn recv(&self) -> Result<Delivery> {
        let message = self.inner.recv().await.map_err(|e| self.rejoin(e))?;
        self.bus.delivery(&message)
    }

    /// Waits for one record, then takes those already fetched, up to `max`.
    pub async fn batch(&self, max: usize) -> Result<Vec<Delivery>> {
        let mut batch = vec![self.recv().await?];
        while batch.len() < max {
            match tokio::time::timeout(Duration::ZERO, self.inner.recv()).await {
                Ok(message) => {
                    let message = message.map_err(|e| self.rejoin(e))?;
                    batch.push(self.bus.delivery(&message)?);
                }
                Err(_) => break,
            }
        }
        Ok(batch)
    }

    /// Waits until the group has assigned this consumer its partitions, so
    /// a service reports ready only once it can receive. The consumer must
    /// be polled meanwhile (e.g. by [`run`] on another task).
    pub async fn joined(&self, wait: Duration) -> Result<()> {
        let deadline = Instant::now() + wait;
        loop {
            if self.inner.assignment().is_ok_and(|a| a.count() > 0) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(kafka("no partitions assigned to this consumer"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Marks `delivery` handled; its offset is committed with the next auto-commit.
    pub fn done(&self, delivery: &Delivery) -> Result<()> {
        self.inner
            .store_offset(
                &self.bus.name(delivery.topic),
                delivery.partition,
                delivery.offset,
            )
            .map_err(kafka)
    }
}

/// Handles every record of `consumer`, forever. Records with the same topic
/// and key are handled one after another in topic order; others run
/// concurrently, up to [`IN_FLIGHT`]. `handle` returns the replies to one
/// record; they are enqueued before the next record with that key is
/// handled. Offsets are stored in topic order, each only once its record's
/// replies are delivered. A handler error (e.g. a database failure) retries
/// the same record.
pub async fn run<F, Fut>(bus: Bus, consumer: Arc<Consumer>, name: &str, handle: F)
where
    F: Fn(Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<Outgoing>>> + Send + 'static,
{
    let handle = Arc::new(handle);
    type Handled = JoinHandle<(Vec<Pending>, bool)>;
    let (acks, mut pending) = mpsc::unbounded_channel::<(Delivery, Handled)>();
    let acker = {
        let consumer = consumer.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            while let Some((delivery, handled)) = pending.recv().await {
                let (replies, mut delivered) = match handled.await {
                    Ok(handled) => handled,
                    Err(error) => {
                        eprintln!("{name}: handler for {} died: {error}", delivery.kind());
                        return;
                    }
                };
                for reply in replies {
                    if let Err(error) = reply.await {
                        eprintln!("{name}: reply to {} lost: {error}", delivery.kind());
                        delivered = false;
                    }
                }
                if delivered && let Err(error) = consumer.done(&delivery) {
                    eprintln!("{name}: {error}");
                }
            }
        })
    };
    let _acker = AbortOnDrop(acker);
    let slots = Arc::new(Semaphore::new(IN_FLIGHT));
    let mut previous: HashMap<(Topic, String), oneshot::Receiver<()>> = HashMap::new();
    loop {
        let delivery = match consumer.recv().await {
            Ok(delivery) => delivery,
            Err(error) => {
                eprintln!("{name}: consume failed: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        if previous.len() > IN_FLIGHT * 4 {
            previous.retain(|_, done| matches!(done.try_recv(), Err(TryRecvError::Empty)));
        }
        let (finished, done) = oneshot::channel();
        let before = previous.insert((delivery.topic, delivery.key.clone()), done);
        let (bus, handle, record) = (bus.clone(), handle.clone(), delivery.clone());
        let task_name = name.to_owned();
        let handled = tokio::spawn(async move {
            if let Some(before) = before {
                let _ = before.await;
            }
            let replies = loop {
                match handle(record.clone()).await {
                    Ok(replies) => break replies,
                    Err(error) => {
                        eprintln!(
                            "{task_name}: handling {} failed, retrying: {error}",
                            record.kind()
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            };
            let mut futures = Vec::with_capacity(replies.len());
            let mut enqueued = true;
            for reply in &replies {
                match bus.send(reply) {
                    Ok(future) => futures.push(future),
                    Err(error) => {
                        eprintln!("{task_name}: enqueue failed: {error}");
                        enqueued = false;
                    }
                }
            }
            let _ = finished.send(());
            drop(slot);
            (futures, enqueued)
        });
        if acks.send((delivery, handled)).is_err() {
            return;
        }
    }
}

/// Spawns `task`, which must poll `consumer`, and waits for the consumer to
/// join its group. A join that takes longer (e.g. a previous member has not
/// left yet) is logged, and the consumer keeps trying in the background.
pub async fn start(
    name: &str,
    consumer: &Consumer,
    task: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    let handle = tokio::spawn(task);
    if let Err(error) = consumer.joined(Duration::from_secs(30)).await {
        eprintln!("{name}: {error}; continuing");
    }
    handle
}

/// Aborts a background task when the owner goes away.
pub struct AbortOnDrop(pub JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
