// SPDX-License-Identifier: Apache-2.0

//! The outbound half of one `Extract` call: a queue bounded in bytes between
//! the parse and the response stream tonic sends from.
//!
//! # Why the bound is in bytes
//!
//! The parse runs on tokio's blocking pool (see [`crate::service`]) and its
//! content handlers put every event here as it happens. When the queue is
//! full a handler waits for room, which holds the parse still, which stops
//! the driver reading the upload: backpressure reaches the client without a
//! buffer growing anywhere behind it. The bound is in bytes rather than in
//! events because events differ by orders of magnitude. A matched `<br>` is a
//! few dozen bytes and a reassembled text node can be megabytes, so a count
//! of events bounds nothing.
//!
//! # A client that stops reading
//!
//! A bounded queue is only half of the answer, because a client that never
//! reads leaves the waiting handler waiting forever. That client exists: one
//! that uploads the whole document before reading anything wedges against
//! any bounded server once the output outgrows the queue and the transport's
//! windows, and neither side moves again. So a wait for room gives up once
//! the send timeout passes without the client taking a single event
//! ([`SendError::Stalled`]), and the driver then [aborts](Outbound::abort)
//! the call. The wait is idle, not total: every event the client takes starts
//! it over, so a slow reader is never mistaken for a stuck one.
//!
//! Progress is counted in events, but reading is done in bytes. An event is
//! taken the moment it is handed to the transport, and the transport takes
//! no more until the client has read most of what it holds, which for a text
//! node of megabytes can take longer than the send timeout without anything
//! being wrong. So the timeout starts only once the client could have read
//! each event handed over, on its own, at [`MIN_READ_RATE`]: a client reading
//! at least that fast is never cut off mid-event, and one that has stopped
//! reading is cut off at most the largest recent event's reading time later
//! than the timeout alone would cut it off. Events are not added up, so a
//! client that reads quickly and then stops has banked no more than that,
//! and the many small events a transport can hold are what the timeout
//! itself is for.
//!
//! An abort discards what is queued at once and leaves only the status. Left
//! in place, the backlog would live as long as the HTTP/2 stream, which a
//! client that never reads can keep open for as long as it likes, long after
//! the call's stream slot has gone to somebody else.
//!
//! # Never OK without a result
//!
//! The stream ends cleanly only after it has handed over a `finished` or
//! `error` event. If every producer goes away without queuing one, as a
//! driver that panicked would, the stream ends with `INTERNAL` rather than
//! with the OK status a client would read as an empty success.

use std::collections::VecDeque;
use std::mem::size_of;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use prost::Message;
use tokio::sync::Notify;
use tokio_stream::Stream;
use tonic::Status;

use crate::proto::v1 as pb;

/// One entry of the response stream.
type Item = Result<pb::ExtractResponse, Status>;

/// Queued bytes after which the events so far are released to the stream.
///
/// The stream takes only released events, in batches, never one at a time.
/// It runs on an async worker while the parse runs on a blocking thread, so
/// left to take whatever is there it chases the parse: every poll finds the
/// event or two queued since the last one, and each becomes a DATA frame of
/// its own. That costs a frame header and a task wake-up per matched element,
/// and h2 (0.4.18 onward) treats a pile of unread tiny frames as a flood and
/// closes the connection. Batches of about this much are roughly what tonic
/// gathers into one frame anyway, and the driver [flushes](Outbound::flush)
/// whenever it is about to wait for the client's next chunk, so nothing waits
/// on a batch that is not coming.
const RELEASE_BYTES: usize = 32 * 1024;

/// The slowest a client may read one event without being taken for one that
/// has stopped, in bytes per second.
///
/// The send timeout starts once the client could have read each event it has
/// been handed at this rate, so this bounds how slowly a large event may be
/// read, and with it how long a client that stops reading partway through one
/// keeps its stream past the timeout: a 1 MiB event adds 16 seconds, and a
/// 64 MiB text node, the largest the default memory limit lets through, 1024.
pub const MIN_READ_RATE: u64 = 64 * 1024;

/// Create a queue that holds at most `capacity` bytes of events, returning
/// its producer handle and the stream tonic sends from.
pub fn channel(capacity: usize) -> (Outbound, OutboundStream) {
    let capacity = capacity.max(1);
    let shared = Arc::new(Shared {
        capacity,
        release_bytes: RELEASE_BYTES.min(capacity),
        state: Mutex::new(State {
            queue: VecDeque::new(),
            queued: 0,
            released: 0,
            unreleased: 0,
            producers: 1,
            ended: false,
            delivered: false,
            closed: false,
            producer_waiting: false,
            driver_waiting: false,
            waker: None,
        }),
        batch: Mutex::new(Batch::default()),
        taken: AtomicU64::new(0),
        epoch: Instant::now(),
        read_by: AtomicU64::new(0),
        room: Condvar::new(),
        progress: Notify::new(),
    });
    (
        Outbound {
            shared: Arc::clone(&shared),
        },
        OutboundStream {
            shared,
            done: false,
        },
    )
}

/// Why an event could not be queued.
#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    /// The response stream is gone, or the call has already ended.
    Closed,
    /// The client took nothing for the whole send timeout, counted from when
    /// it could have read what it took at [`MIN_READ_RATE`].
    Stalled {
        /// Bytes waiting in the queue when the wait gave up.
        queued_bytes: usize,
    },
}

/// How a wait for the call's terminal item ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    /// The response stream has taken the terminal item.
    Delivered,
    /// The response stream is gone.
    Gone,
    /// The client took nothing for the whole send timeout, counted from when
    /// it could have read what it took at [`MIN_READ_RATE`].
    Stalled,
}

/// Producer handle: the driver holds one and every content handler a clone.
pub struct Outbound {
    shared: Arc<Shared>,
}

/// The response stream for tonic, draining the queue.
pub struct OutboundStream {
    shared: Arc<Shared>,
    /// Set once a terminal item has been handed over.
    done: bool,
}

struct Shared {
    /// Byte bound on the queue.
    capacity: usize,
    /// Unreleased bytes after which the queue is released to the stream.
    release_bytes: usize,
    /// The producers' side: the queue, and everything known about the call.
    state: Mutex<State>,
    /// The stream's side: the batch it is handing over.
    ///
    /// The stream moves a whole release out of `state` at once and hands it
    /// over from here, so the two sides meet on `state` once per batch rather
    /// than once per event. They run on different threads, and a lock both
    /// take for every event costs more in contention than the events cost to
    /// build. It lives here rather than in the stream so that an abort can
    /// free it. Wherever both locks are held, `batch` is taken first.
    batch: Mutex<Batch>,
    /// Events the stream has handed over. A waiter that sees this move knows
    /// the client is still reading.
    taken: AtomicU64,
    /// Where [`Shared::read_by`] counts from.
    epoch: Instant,
    /// When a client reading at [`MIN_READ_RATE`] will have read the events
    /// handed over so far, each on its own, in nanoseconds after `epoch`.
    /// Only the stream writes it, before `taken` moves.
    read_by: AtomicU64,
    /// Signalled when room is made and a producer is waiting.
    room: Condvar,
    /// Signalled when the terminal item is taken, or the stream goes away,
    /// and the driver is waiting.
    progress: Notify,
}

/// One event waiting to be handed over.
struct Queued {
    item: Item,
    /// Bytes charged against the queue's bound.
    charged: usize,
    /// Its encoded size, which is what the client has to read.
    size: u64,
}

impl Queued {
    /// An item that is not charged against the bound.
    fn free(item: Item) -> Self {
        Self {
            item,
            charged: 0,
            size: 0,
        }
    }
}

/// The events the stream took in one go and is handing over.
#[derive(Default)]
struct Batch {
    items: VecDeque<Queued>,
    /// Bytes charged to `items`. They stay counted in [`State::queued`] until
    /// the whole batch has been handed over.
    bytes: usize,
}

struct State {
    /// Events not yet taken.
    queue: VecDeque<Queued>,
    /// Bytes charged to everything in `queue` and in the stream's batch.
    queued: usize,
    /// How many events at the front of `queue` the stream may take.
    released: usize,
    /// Bytes queued since the last release.
    unreleased: usize,
    /// Live producer handles.
    producers: usize,
    /// A terminal item is queued, so nothing more is accepted.
    ended: bool,
    /// The stream has taken the terminal item.
    delivered: bool,
    /// The stream is gone, so nothing will be read again.
    closed: bool,
    /// A producer is blocked waiting for room.
    producer_waiting: bool,
    /// The driver is waiting for delivery.
    driver_waiting: bool,
    /// The stream's waker, while it waits for a release.
    waker: Option<Waker>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing panics while holding these locks, but a poisoned queue is
        // still a queue: carrying on beats turning one panic into two.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_batch(&self) -> MutexGuard<'_, Batch> {
        self.batch.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Events taken so far, and when a client reading at [`MIN_READ_RATE`]
    /// will have read them.
    fn progress(&self) -> (u64, Instant) {
        let taken = self.taken.load(Ordering::Acquire);
        let read_by = Duration::from_nanos(self.read_by.load(Ordering::Relaxed));
        (taken, self.epoch + read_by)
    }

    /// Account for an event of `size` encoded bytes handed over to the
    /// transport.
    fn hand_over(&self, size: u64) {
        let read = nanos(self.epoch.elapsed()).saturating_add(reading_nanos(size));
        // Later than an earlier, larger event the client may still be
        // reading, never earlier.
        self.read_by.fetch_max(read, Ordering::Relaxed);
        self.taken.fetch_add(1, Ordering::Release);
    }
}

impl State {
    /// Let the stream take everything queued so far, returning its waker
    /// for the caller to wake once the lock is released.
    fn release(&mut self) -> Option<Waker> {
        self.released = self.queue.len();
        self.unreleased = 0;
        if self.released == 0 {
            None
        } else {
            self.waker.take()
        }
    }
}

impl Outbound {
    /// Queue an event the driver produces itself: `started`, `finished`, or
    /// an in-band `error`. Never waits and is not charged against the bound,
    /// because there are at most two of these per call. Returns false if the
    /// event can no longer be delivered.
    pub fn push(&self, event: pb::extract_response::Event) -> bool {
        self.enqueue(Ok(pb::ExtractResponse { event: Some(event) }))
    }

    /// End the call with `status`, after whatever is already queued. Never
    /// waits.
    pub fn fail(&self, status: Status) {
        self.enqueue(Err(status));
    }

    /// Queue an event from a content handler, waiting for room.
    ///
    /// Blocks the calling thread, so it must only be called from the blocking
    /// pool, never from an async worker. An event larger than the whole queue
    /// is let through on its own once the queue is empty rather than refused.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] once nothing can be delivered any more, and
    /// [`SendError::Stalled`] once the client has taken nothing for
    /// `timeout`.
    pub fn send_blocking(
        &self,
        response: pb::ExtractResponse,
        timeout: Duration,
    ) -> Result<(), SendError> {
        let size = response.encoded_len();
        let cost = cost(&response, size).min(self.shared.capacity);
        let mut state = self.shared.lock();
        // Started lazily, so the common case of a queue with room never reads
        // the clock.
        let mut wait: Option<(u64, Instant)> = None;

        while state.queued + cost > self.shared.capacity {
            if state.closed || state.ended {
                return Err(SendError::Closed);
            }

            let now = Instant::now();
            let (taken, read_by) = self.shared.progress();
            let deadline = match wait {
                // The client took nothing since the last look.
                Some((seen, deadline)) if seen == taken => deadline,
                // It did, so it is reading, and the wait starts over once it
                // has had time to read what it took.
                _ => {
                    let deadline = now.max(read_by) + timeout;
                    wait = Some((taken, deadline));
                    deadline
                }
            };
            if now >= deadline {
                return Err(SendError::Stalled {
                    queued_bytes: state.queued,
                });
            }

            // Whatever is queued is all the stream will get until there is
            // room, so it is released now rather than at the next batch
            // boundary, or both sides would wait on each other.
            let waker = state.release();
            state.producer_waiting = true;
            if let Some(waker) = waker {
                drop(state);
                waker.wake();
                state = self.shared.lock();
                continue;
            }
            state = self
                .shared
                .room
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }

        if state.closed || state.ended {
            return Err(SendError::Closed);
        }
        state.producer_waiting = false;
        state.queue.push_back(Queued {
            item: Ok(response),
            charged: cost,
            size: size as u64,
        });
        state.queued += cost;
        state.unreleased += cost;
        let waker = if state.unreleased >= self.shared.release_bytes {
            state.release()
        } else {
            None
        };
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Release whatever is queued to the response stream, without waiting
    /// for a full batch.
    pub fn flush(&self) {
        let waker = self.shared.lock().release();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// End the call with `status` now, discarding everything still queued,
    /// including what the stream has taken but not yet handed over.
    ///
    /// A failure status that is already queued is kept rather than replaced,
    /// because it is the more specific of the two. Does nothing once the
    /// terminal item has been delivered.
    pub fn abort(&self, status: Status) {
        let mut batch = self.shared.lock_batch();
        let mut state = self.shared.lock();
        if state.closed || state.delivered {
            return;
        }
        // Nothing follows a terminal item, so a queued failure is the last
        // thing in the queue or, if the stream has taken it, in the batch.
        let kept = if matches!(state.queue.back(), Some(Queued { item: Err(_), .. })) {
            state.queue.pop_back()
        } else if matches!(batch.items.back(), Some(Queued { item: Err(_), .. })) {
            batch.items.pop_back()
        } else {
            None
        };
        let discarded = (
            std::mem::take(&mut state.queue),
            std::mem::take(&mut batch.items),
        );
        batch.bytes = 0;
        state.queued = 0;
        state
            .queue
            .push_back(kept.unwrap_or_else(|| Queued::free(Err(status))));
        state.ended = true;
        let waker = state.release();
        let producer_waiting = std::mem::take(&mut state.producer_waiting);
        drop(state);
        drop(batch);

        // Outside the locks: this can be megabytes of events.
        drop(discarded);
        if producer_waiting {
            self.shared.room.notify_all();
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Wait until the response stream has taken the call's terminal item.
    ///
    /// Gives up once the client has taken nothing for `timeout`; like the
    /// wait for room, every event taken starts the wait over, once the client
    /// has had time to read it.
    pub async fn delivered(&self, timeout: Duration) -> Delivery {
        let mut wait: Option<(u64, tokio::time::Instant)> = None;
        loop {
            // Registered before the state is read, so a notification sent in
            // between is not lost.
            let notified = self.shared.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let deadline = {
                let mut state = self.shared.lock();
                // Delivery first: a stream that took the result and then
                // ended, as every stream does, still delivered it.
                if state.delivered {
                    state.driver_waiting = false;
                    return Delivery::Delivered;
                }
                if state.closed {
                    state.driver_waiting = false;
                    return Delivery::Gone;
                }
                let now = tokio::time::Instant::now();
                let (taken, read_by) = self.shared.progress();
                let deadline = match wait {
                    Some((seen, deadline)) if seen == taken => {
                        if now >= deadline {
                            state.driver_waiting = false;
                            return Delivery::Stalled;
                        }
                        deadline
                    }
                    _ => {
                        let deadline = now.max(read_by.into()) + timeout;
                        wait = Some((taken, deadline));
                        deadline
                    }
                };
                state.driver_waiting = true;
                deadline
            };

            // Either way round the loop: a notification is news to look at,
            // and an expiry is checked against the progress made meanwhile.
            let _ = tokio::time::timeout_at(deadline, notified).await;
        }
    }

    /// Bytes currently queued, for logs.
    pub fn queued_bytes(&self) -> usize {
        self.shared.lock().queued
    }

    /// Queue an item without waiting or charging it, and release the queue.
    fn enqueue(&self, item: Item) -> bool {
        let mut state = self.shared.lock();
        if state.closed || state.ended {
            return false;
        }
        if is_terminal(&item) {
            state.ended = true;
        }
        state.queue.push_back(Queued::free(item));
        let waker = state.release();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }
}

impl Clone for Outbound {
    fn clone(&self) -> Self {
        self.shared.lock().producers += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for Outbound {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.producers -= 1;
        let waker = if state.producers == 0 {
            // Nothing more is coming, so everything queued is released, and
            // the stream is woken even with nothing queued: it has to find
            // out whether the call ended with a result.
            state.release();
            state.waker.take()
        } else {
            None
        };
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Stream for OutboundStream {
    type Item = Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }

        let mut batch = this.shared.lock_batch();
        if batch.items.is_empty() {
            // The batch is all handed over: give its bytes back and take the
            // next release in its place.
            let mut state = this.shared.lock();
            state.queued -= batch.bytes;
            let released = std::mem::take(&mut state.released);
            if released == state.queue.len() {
                std::mem::swap(&mut batch.items, &mut state.queue);
            } else {
                batch.items.extend(state.queue.drain(..released));
            }
            batch.bytes = batch.items.iter().map(|queued| queued.charged).sum();
            let producer_waiting = std::mem::take(&mut state.producer_waiting);

            if batch.items.is_empty() {
                let finished = state.producers == 0;
                if !finished {
                    match &mut state.waker {
                        Some(waker) if waker.will_wake(cx.waker()) => {}
                        slot => *slot = Some(cx.waker().clone()),
                    }
                }
                drop(state);
                drop(batch);
                if producer_waiting {
                    this.shared.room.notify_one();
                }
                if !finished {
                    return Poll::Pending;
                }
                // Every producer is gone and none of them left a result.
                // Ending here would be an OK status on a call that produced
                // nothing, which a client cannot tell from an empty document.
                this.done = true;
                return Poll::Ready(Some(Err(Status::internal(
                    "the server ended the call without a result",
                ))));
            }

            drop(state);
            if producer_waiting {
                this.shared.room.notify_one();
            }
        }

        let Some(Queued { item, size, .. }) = batch.items.pop_front() else {
            unreachable!("a batch that was just refilled is not empty");
        };
        drop(batch);
        this.shared.hand_over(size);

        if is_terminal(&item) {
            this.done = true;
            let mut state = this.shared.lock();
            state.delivered = true;
            let driver_waiting = state.driver_waiting;
            drop(state);
            if driver_waiting {
                this.shared.progress.notify_one();
            }
        }
        Poll::Ready(Some(item))
    }
}

impl Drop for OutboundStream {
    fn drop(&mut self) {
        let mut batch = self.shared.lock_batch();
        let mut state = self.shared.lock();
        state.closed = true;
        let discarded = (
            std::mem::take(&mut state.queue),
            std::mem::take(&mut batch.items),
        );
        batch.bytes = 0;
        state.queued = 0;
        state.released = 0;
        let producer_waiting = std::mem::take(&mut state.producer_waiting);
        let driver_waiting = state.driver_waiting;
        drop(state);
        drop(batch);

        drop(discarded);
        if producer_waiting {
            self.shared.room.notify_all();
        }
        if driver_waiting {
            self.shared.progress.notify_one();
        }
    }
}

/// Whether an item ends the response stream.
const fn is_terminal(item: &Item) -> bool {
    matches!(
        item,
        Err(_)
            | Ok(pb::ExtractResponse {
                event: Some(
                    pb::extract_response::Event::Finished(_)
                        | pb::extract_response::Event::Error(_)
                ),
            })
    )
}

/// How long a client reading at [`MIN_READ_RATE`] takes to read `bytes`, in
/// nanoseconds.
const fn reading_nanos(bytes: u64) -> u64 {
    bytes.saturating_mul(1_000_000_000) / MIN_READ_RATE
}

/// `duration` in nanoseconds, saturating.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What one queued event is charged against the queue's bound, given its
/// encoded size.
///
/// Its encoded size alone would undercount badly: a matched `<a>` is a few
/// dozen bytes on the wire and several hundred in memory once the message
/// struct and its attribute list are counted. So the in-memory size of the
/// message and of each attribute are added, which keeps a queue of tiny
/// events honest and makes no difference to a large text node, where the
/// text dominates.
fn cost(response: &pb::ExtractResponse, encoded_len: usize) -> usize {
    let attributes = match &response.event {
        Some(pb::extract_response::Event::Element(element)) => element.attributes.len(),
        _ => 0,
    };
    encoded_len + size_of::<pb::ExtractResponse>() + attributes * size_of::<pb::Attribute>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_stream::StreamExt;

    fn text(body: &str) -> pb::ExtractResponse {
        pb::ExtractResponse {
            event: Some(pb::extract_response::Event::Text(pb::TextNode {
                rule_id: "r".to_owned(),
                text: body.to_owned(),
                ..Default::default()
            })),
        }
    }

    /// What `event` is charged against the bound.
    fn charge(event: &pb::ExtractResponse) -> usize {
        cost(event, event.encoded_len())
    }

    fn finished() -> pb::extract_response::Event {
        pb::extract_response::Event::Finished(pb::ExtractFinished::default())
    }

    fn body(item: Option<Item>) -> String {
        match item {
            Some(Ok(pb::ExtractResponse {
                event: Some(pb::extract_response::Event::Text(node)),
            })) => node.text,
            other => panic!("expected a text event, got {other:?}"),
        }
    }

    /// Events come out in the order they went in, and the stream ends right
    /// after the terminal event rather than waiting for its producers.
    #[tokio::test]
    async fn events_come_out_in_order_and_the_stream_ends_after_the_result() {
        let (tx, mut rx) = channel(1 << 20);
        tx.send_blocking(text("one"), Duration::from_secs(1))
            .unwrap();
        tx.send_blocking(text("two"), Duration::from_secs(1))
            .unwrap();
        assert!(tx.push(finished()));

        assert_eq!(body(rx.next().await), "one");
        assert_eq!(body(rx.next().await), "two");
        assert!(matches!(
            rx.next().await,
            Some(Ok(pb::ExtractResponse {
                event: Some(pb::extract_response::Event::Finished(_))
            }))
        ));
        assert!(rx.next().await.is_none(), "nothing follows the result");
        assert!(
            !tx.push(finished()),
            "a second result is refused rather than queued"
        );
    }

    /// Handler events reach the stream in released batches, never one at a
    /// time as they are queued: a stream that could see every event the
    /// moment it lands would chase the parse and send a frame per event.
    #[tokio::test]
    async fn events_wait_for_a_release_rather_than_trickling_out() {
        let (tx, mut rx) = channel(1 << 20);
        tx.send_blocking(text("held"), Duration::from_secs(1))
            .unwrap();

        let early = tokio::time::timeout(Duration::from_millis(50), rx.next()).await;
        assert!(early.is_err(), "an unreleased event must not come out yet");

        tx.flush();
        assert_eq!(body(rx.next().await), "held");

        // A full batch releases itself without a flush.
        let filler = "z".repeat(RELEASE_BYTES);
        tx.send_blocking(text(&filler), Duration::from_secs(1))
            .unwrap();
        assert_eq!(body(rx.next().await), filler);
    }

    /// A full queue holds the producer until the stream takes something.
    #[tokio::test]
    async fn a_full_queue_holds_the_producer_until_the_stream_takes_an_event() {
        let event = text("x");
        let (tx, mut rx) = channel(charge(&event));
        tx.send_blocking(event.clone(), Duration::from_secs(5))
            .unwrap();

        let producer = std::thread::spawn(move || {
            let started = Instant::now();
            tx.send_blocking(event, Duration::from_secs(5)).unwrap();
            started.elapsed()
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(body(rx.next().await), "x");
        // Only reachable once the second send found room.
        assert_eq!(body(rx.next().await), "x");
        let waited = tokio::task::spawn_blocking(move || producer.join().unwrap())
            .await
            .unwrap();
        assert!(
            waited >= Duration::from_millis(80),
            "the second event should have waited for room, waited {waited:?}"
        );
    }

    /// An abort frees what the stream has taken but not handed over as well
    /// as what is still queued, so nothing is left for a client that may
    /// never read again.
    #[tokio::test]
    async fn an_abort_frees_a_batch_the_stream_has_already_taken() {
        let (tx, mut rx) = channel(1 << 20);
        for _ in 0..10 {
            tx.send_blocking(text("taken"), Duration::from_secs(1))
                .unwrap();
        }
        tx.flush();
        assert_eq!(body(rx.next().await), "taken");
        assert!(tx.queued_bytes() > 0, "the rest of the batch is still held");

        tx.abort(Status::resource_exhausted("not reading"));
        assert_eq!(tx.queued_bytes(), 0);
        let status = rx.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    /// Nobody reading means the wait gives up, and says how much was waiting.
    #[test]
    fn a_producer_facing_a_stream_nobody_reads_gives_up_after_the_timeout() {
        let event = text("x");
        let (tx, _rx) = channel(charge(&event));
        tx.send_blocking(event.clone(), Duration::from_secs(5))
            .unwrap();

        let started = Instant::now();
        let err = tx
            .send_blocking(event.clone(), Duration::from_millis(100))
            .unwrap_err();
        assert_eq!(
            err,
            SendError::Stalled {
                queued_bytes: charge(&event)
            }
        );
        assert!(started.elapsed() >= Duration::from_millis(100));
    }

    /// The wait allows for the last event taken to be read at the slowest
    /// rate a client is held to, so a large event the client is still reading
    /// is not mistaken for a client that has stopped.
    #[tokio::test]
    async fn a_large_event_in_flight_extends_the_wait_by_its_reading_time() {
        let large = text(&"l".repeat(16 * 1024));
        let reading = Duration::from_nanos(reading_nanos(large.encoded_len() as u64));
        assert!(reading >= Duration::from_millis(250));

        let (tx, mut rx) = channel(charge(&large));
        tx.send_blocking(large, Duration::from_secs(5)).unwrap();
        tx.flush();
        assert_eq!(body(rx.next().await).len(), 16 * 1024);

        // The stream has handed the large event over and takes nothing more,
        // as it would while the client reads it.
        let small = text("s");
        let (sent, waited) = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let sent = tx.send_blocking(small, Duration::from_millis(50));
            (sent, started.elapsed())
        })
        .await
        .unwrap();
        assert!(matches!(sent, Err(SendError::Stalled { .. })));
        assert!(waited >= reading, "gave up after {waited:?}");

        let (tx, mut rx) = channel(1 << 20);
        tx.send_blocking(text(&"l".repeat(16 * 1024)), Duration::from_secs(5))
            .unwrap();
        assert!(tx.push(finished()));
        assert_eq!(body(rx.next().await).len(), 16 * 1024);
        let started = std::time::Instant::now();
        assert_eq!(
            tx.delivered(Duration::from_millis(50)).await,
            Delivery::Stalled
        );
        assert!(
            started.elapsed() >= reading,
            "gave up after {:?}",
            started.elapsed()
        );
    }

    /// A client that goes away releases a waiting producer at once rather
    /// than after the timeout.
    #[tokio::test]
    async fn dropping_the_stream_releases_a_waiting_producer() {
        let event = text("x");
        let (tx, rx) = channel(charge(&event));
        tx.send_blocking(event.clone(), Duration::from_secs(5))
            .unwrap();

        let producer = std::thread::spawn(move || {
            let started = Instant::now();
            let result = tx.send_blocking(event, Duration::from_secs(30));
            (result, started.elapsed())
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(rx);

        let (result, waited) = tokio::task::spawn_blocking(move || producer.join().unwrap())
            .await
            .unwrap();
        assert_eq!(result, Err(SendError::Closed));
        assert!(waited < Duration::from_secs(10), "waited {waited:?}");
    }

    /// An abort throws the backlog away and the status is the next thing out.
    #[tokio::test]
    async fn an_abort_discards_the_backlog_and_ends_with_its_status() {
        let (tx, mut rx) = channel(1 << 20);
        for _ in 0..100 {
            tx.send_blocking(text("queued"), Duration::from_secs(1))
                .unwrap();
        }
        assert!(tx.queued_bytes() > 0);

        tx.abort(Status::resource_exhausted("not reading"));
        assert_eq!(tx.queued_bytes(), 0, "the backlog is freed immediately");

        let status = rx.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(rx.next().await.is_none());
    }

    /// A failure that is already queued survives a later abort, because it is
    /// the more specific explanation.
    #[tokio::test]
    async fn an_abort_keeps_a_failure_that_was_already_queued() {
        let (tx, mut rx) = channel(1 << 20);
        tx.send_blocking(text("queued"), Duration::from_secs(1))
            .unwrap();
        tx.fail(Status::deadline_exceeded("idle"));
        tx.abort(Status::resource_exhausted("not reading"));

        let status = rx.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    /// One event bigger than the whole queue still goes out, alone.
    #[tokio::test]
    async fn an_event_larger_than_the_queue_goes_through_on_its_own() {
        let (tx, mut rx) = channel(64);
        let big = "y".repeat(4096);
        tx.send_blocking(text(&big), Duration::from_secs(1))
            .unwrap();
        tx.flush();
        assert_eq!(body(rx.next().await), big);
    }

    /// Producers that all leave without a result end the stream with an
    /// error, never with the OK status of an empty success.
    #[tokio::test]
    async fn losing_every_producer_without_a_result_is_an_error() {
        let (tx, mut rx) = channel(1 << 20);
        let handler = tx.clone();
        handler
            .send_blocking(text("partial"), Duration::from_secs(1))
            .unwrap();
        drop(handler);
        drop(tx);

        assert_eq!(body(rx.next().await), "partial");
        let status = rx.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(rx.next().await.is_none());
    }

    /// Delivery is reported once the stream takes the result, and a stream
    /// nobody reads is given up on after the timeout.
    #[tokio::test]
    async fn delivery_waits_for_the_result_and_gives_up_without_progress() {
        let (tx, mut rx) = channel(1 << 20);
        tx.send_blocking(text("one"), Duration::from_secs(1))
            .unwrap();
        assert!(tx.push(finished()));

        let started = std::time::Instant::now();
        assert_eq!(
            tx.delivered(Duration::from_millis(100)).await,
            Delivery::Stalled
        );
        assert!(started.elapsed() >= Duration::from_millis(100));

        let reader = tokio::spawn(async move { while rx.next().await.is_some() {} });
        assert_eq!(
            tx.delivered(Duration::from_secs(5)).await,
            Delivery::Delivered
        );
        reader.await.unwrap();
    }

    /// A stream that goes away while the driver waits ends the wait.
    #[tokio::test]
    async fn delivery_ends_when_the_stream_goes_away() {
        let (tx, rx) = channel(1 << 20);
        assert!(tx.push(finished()));
        let dropper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(rx);
        });
        assert_eq!(tx.delivered(Duration::from_secs(30)).await, Delivery::Gone);
        dropper.await.unwrap();
    }

    /// Small events are charged for what they hold in memory, not only for
    /// their few encoded bytes.
    #[test]
    fn a_tiny_event_is_charged_more_than_its_encoded_size() {
        let element = pb::ExtractResponse {
            event: Some(pb::extract_response::Event::Element(pb::ElementMatched {
                rule_id: "a".to_owned(),
                tag_name: "a".to_owned(),
                attributes: vec![pb::Attribute {
                    name: "x".to_owned(),
                    value: "1".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        };
        assert!(charge(&element) > 4 * element.encoded_len());
    }
}
