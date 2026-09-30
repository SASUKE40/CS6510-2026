//! Generic pipes-and-filters runtime. Every filter owns its state and runs on its
//! own thread; filters share nothing and talk only through bounded channels.
use serde_json::Value;
use std::{
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TryRecvError, sync_channel},
    },
    thread::{self, JoinHandle},
};

// A full pipe blocks its producer, so a slow stage applies backpressure
// instead of growing memory without bound.
const PIPE_CAPACITY: usize = 16_384;
const MAX_BATCH: usize = 1_024;

pub(crate) type Snapshot = Arc<Value>;
pub(crate) type Reply = SyncSender<Snapshot>;

pub(crate) enum Message<T> {
    Data(T),
    /// Flows behind every earlier message; the sink answers it only after
    /// all of that earlier data has been published.
    Barrier(Reply),
    /// Drains the pipeline, then stops each filter in order.
    Stop,
}

pub(crate) type Inlet<T> = SyncSender<Message<T>>;
pub(crate) type Outlet<T> = Receiver<Message<T>>;

pub(crate) fn pipe<T>() -> (Inlet<T>, Outlet<T>) {
    sync_channel(PIPE_CAPACITY)
}

pub(crate) trait Filter: Send + 'static {
    type In: Send + 'static;
    type Out: Send + 'static;
    /// Receives every message that was already waiting, in arrival order.
    fn process(&mut self, batch: Vec<Self::In>) -> Vec<Self::Out>;
}

pub(crate) trait Sink: Send + 'static {
    type In: Send + 'static;
    fn consume(&mut self, batch: Vec<Self::In>);
    fn current(&self) -> Snapshot;
}

enum Control {
    Barrier(Reply),
    Stop,
    Closed,
}

fn next_batch<T>(input: &Outlet<T>) -> (Vec<T>, Option<Control>) {
    let mut batch = Vec::new();
    let mut next = input.recv().map_err(|_| TryRecvError::Disconnected);
    loop {
        match next {
            Ok(Message::Data(item)) => batch.push(item),
            Ok(Message::Barrier(reply)) => return (batch, Some(Control::Barrier(reply))),
            Ok(Message::Stop) => return (batch, Some(Control::Stop)),
            Err(TryRecvError::Disconnected) => return (batch, Some(Control::Closed)),
            Err(TryRecvError::Empty) => return (batch, None),
        }
        if batch.len() == MAX_BATCH {
            return (batch, None);
        }
        next = input.try_recv();
    }
}

pub(crate) fn spawn_filter<F: Filter>(
    name: &str,
    mut filter: F,
    input: Outlet<F::In>,
    output: Inlet<F::Out>,
) -> JoinHandle<()> {
    spawn(name, move || {
        loop {
            let (batch, control) = next_batch(&input);
            if !batch.is_empty() {
                for item in filter.process(batch) {
                    if output.send(Message::Data(item)).is_err() {
                        return;
                    }
                }
            }
            match control {
                None => {}
                Some(Control::Barrier(reply)) => {
                    if output.send(Message::Barrier(reply)).is_err() {
                        return;
                    }
                }
                Some(Control::Stop) => {
                    let _ = output.send(Message::Stop);
                    return;
                }
                Some(Control::Closed) => return,
            }
        }
    })
}

pub(crate) fn spawn_sink<S: Sink>(name: &str, mut sink: S, input: Outlet<S::In>) -> JoinHandle<()> {
    spawn(name, move || {
        loop {
            let (batch, control) = next_batch(&input);
            if !batch.is_empty() {
                sink.consume(batch);
            }
            match control {
                None => {}
                Some(Control::Barrier(reply)) => {
                    let _ = reply.send(sink.current());
                }
                Some(Control::Stop | Control::Closed) => return,
            }
        }
    })
}

fn spawn(name: &str, body: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("filter-{name}"))
        .spawn(body)
        .expect("failed to spawn a pipeline filter thread")
}
