// SPDX-License-Identifier: Apache-2.0

//! The `Extract` driver: lol-html's synchronous handlers bridged onto a tonic
//! response stream without losing backpressure.
//!
//! # Why this is not a one-liner
//!
//! lol-html's content handlers are plain synchronous `FnMut` closures. A tonic
//! response stream is fed asynchronously. You cannot await inside a closure,
//! and blocking inside one on an async worker stalls that worker, or on a
//! single-threaded runtime deadlocks it outright. The obvious escape, an
//! unbounded channel from the handlers, throws away backpressure, which is
//! the one property this service advertises.
//!
//! So the parse never runs on an async worker. Every `write()` and the final
//! `end()` run on tokio's blocking pool, and the handlers put events straight
//! onto the call's [`outbound`] queue, which is bounded in bytes. A handler
//! that finds it full waits, on its blocking thread, until the response
//! stream takes something. That holds the parse still, which stops the
//! driver reading the next inbound chunk, so a slow client slows the parser
//! rather than growing a queue behind it, whatever the chunk size and however
//! many rules match.
//!
//! The blocking pool is also what makes a large chunk harmless to everyone
//! else: it is one long parse on a thread nobody is waiting for, rather than
//! seconds in which an async worker cannot answer health checks, send
//! keepalives or move other streams.
//!
//! Three limits keep one caller from pinning the process: an open stream that
//! stops sending frames is ended with `DEADLINE_EXCEEDED` after the idle
//! timeout; one whose client stops reading responses is ended with
//! `RESOURCE_EXHAUSTED` after the send timeout; and the number of live
//! streams is bounded by a semaphore, past which calls fail fast with
//! `RESOURCE_EXHAUSTED`. Every finished stream logs its bytes, matches and
//! duration on a per-stream `extract` span.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use lol_html::errors::RewritingError;
use lol_html::html_content::{Comment, Doctype, EndTag, TextChunk};
use lol_html::send::{DocumentContentHandlers, ElementContentHandlers, HtmlRewriter, Settings};
use tokio::sync::Semaphore;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::errors::HandlerStop;
use crate::outbound::{self, Delivery, Outbound, OutboundStream, SendError};
use crate::proto::v1 as pb;
use crate::rules::{CompiledOptions, CompiledRule};
use crate::{convert, errors, rules};

/// Largest single inbound chunk accepted, in bytes.
///
/// Not a document size limit: a document is any number of chunks. This bounds
/// how much one message carries: how long one uninterruptible
/// `rewriter.write()` runs, and how large a buffer each in-flight call holds
/// while it does.
///
/// Generous on purpose, so a caller who wants to hand over a whole document
/// in one message can. Nothing is gained by it: the benchmark shows upload
/// throughput plateaus around a 256 KiB chunk, and 16 KiB against 1 MiB is the
/// difference between roughly 85 and 96 MiB/s. Past that a larger chunk only
/// buys a longer stretch in which one request occupies a blocking thread and
/// a bigger transient buffer per in-flight call, so prefer 256 KiB to 1 MiB in
/// a client unless there is a reason not to.
///
/// [`LolHtmlGrpc::into_service`] derives tonic's decoding limit from this, so
/// the two cannot drift. They did once: this was 8 MiB while tonic's default
/// stayed at 4 MiB, which made every chunk between the two fail with an opaque
/// `OutOfRange` from the transport instead of the `INVALID_ARGUMENT` below
/// that says what to do about it.
pub const DEFAULT_MAX_CHUNK_BYTES: usize = 100 * 1024 * 1024;

/// Default for how long an open `Extract` stream may go without an inbound
/// frame, in milliseconds.
///
/// A client that sends its options and then stalls would otherwise pin a
/// server task forever; this is the bound on that. Sixty seconds is generous
/// for a protocol whose only reason to pause is a slow upstream of its own.
pub const DEFAULT_IDLE_TIMEOUT_MS: usize = 60_000;

/// Default for how long an open `Extract` stream may go without its client
/// taking a single response while events wait, in milliseconds.
///
/// The counterpart of the idle timeout, for the other direction, and idle
/// rather than total in the same way: every response the client takes starts
/// it over, so a slow reader never reaches it and only one that has stopped
/// reading does. It starts over only once the client has had time to read
/// what it took at [`outbound::MIN_READ_RATE`], so a response of megabytes
/// that takes longer than this to read is not mistaken for a client that has
/// stopped. In practice that is a client that uploads the whole document
/// before reading anything. Once the output outgrows the outbound buffer and
/// the transport's windows, such a client and any bounded server wait on each
/// other forever; this turns the wait into `RESOURCE_EXHAUSTED` and gives the
/// stream slot back.
pub const DEFAULT_SEND_TIMEOUT_MS: usize = 60_000;

/// Default byte budget for one stream's events waiting to be sent.
///
/// A client that reads while it uploads never comes near it. It is what an
/// upload-then-read client gets before backpressure, and then the send
/// timeout, applies: 8 MiB holds the whole output of most pages, and bounds
/// what the default 64 streams can hold at 512 MiB.
pub const DEFAULT_OUTBOUND_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// Default ceiling on the memory limit a call may ask for, in bytes.
///
/// A call names its own limit in `MemoryLimits.max_bytes`, and without a
/// ceiling it could name `u64::MAX` and switch the parser's only memory guard
/// off. The default equals the per-call default, so callers can lower their
/// limit but not raise it unless the operator raises this.
pub const DEFAULT_MEMORY_CEILING_BYTES: usize = rules::DEFAULT_MAX_MEMORY_BYTES as usize;

/// Default cap on simultaneously open `Extract` streams.
///
/// Each stream is a parser instance with its own buffers, so an unbounded
/// count is an unbounded memory commitment. Calls past the cap fail fast with
/// `RESOURCE_EXHAUSTED` rather than queueing, because a parser that starts
/// late is worse than a client that retries. This is per-process and
/// orthogonal to tonic's per-connection `max_concurrent_streams`.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 64;

/// The `lolhtml.v1.LolHtmlService` implementation.
pub struct LolHtmlGrpc {
    max_chunk_bytes: usize,
    idle_timeout: Duration,
    send_timeout: Duration,
    outbound_buffer_bytes: usize,
    memory_ceiling: usize,
    stream_permits: Arc<Semaphore>,
}

impl Default for LolHtmlGrpc {
    fn default() -> Self {
        Self::new()
    }
}

impl LolHtmlGrpc {
    /// Create a service with default limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_chunk_bytes: DEFAULT_MAX_CHUNK_BYTES,
            idle_timeout: Duration::from_millis(DEFAULT_IDLE_TIMEOUT_MS as u64),
            send_timeout: Duration::from_millis(DEFAULT_SEND_TIMEOUT_MS as u64),
            outbound_buffer_bytes: DEFAULT_OUTBOUND_BUFFER_BYTES,
            memory_ceiling: DEFAULT_MEMORY_CEILING_BYTES,
            stream_permits: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_STREAMS)),
        }
    }

    /// Override the largest accepted inbound chunk.
    #[must_use]
    pub fn with_max_chunk_bytes(mut self, bytes: usize) -> Self {
        self.max_chunk_bytes = bytes;
        self
    }

    /// Override how long an `Extract` stream may idle before the server
    /// ends it with `DEADLINE_EXCEEDED`.
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Override how long an `Extract` stream may go without its client taking
    /// a response before the server ends it with `RESOURCE_EXHAUSTED`.
    #[must_use]
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Override the byte budget for one stream's events waiting to be sent.
    #[must_use]
    pub fn with_outbound_buffer_bytes(mut self, bytes: usize) -> Self {
        self.outbound_buffer_bytes = bytes;
        self
    }

    /// Override the ceiling on the memory limit a call may ask for.
    #[must_use]
    pub fn with_max_memory_bytes(mut self, bytes: usize) -> Self {
        self.memory_ceiling = bytes;
        self
    }

    /// Override the cap on simultaneously open `Extract` streams.
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max: usize) -> Self {
        self.stream_permits = Arc::new(Semaphore::new(max));
        self
    }

    /// Wrap this service in its generated tonic server.
    ///
    /// tonic's decoding limit is set to twice the chunk cap, deliberately
    /// above it rather than equal to it. The two limits mean different things:
    /// the cap is advice, refused with an `INVALID_ARGUMENT` that names the
    /// number and says to split the document, while tonic's is a hard backstop
    /// against a hostile length prefix. Setting them equal would make the
    /// backstop fire first for every ordinary overshoot, and the caller would
    /// get `OutOfRange` and a sentence about decoded message lengths instead
    /// of the one telling them what to do.
    ///
    /// zstd is enabled in both directions. Responses compress well — match
    /// events are repetitive small messages — but a response is only ever
    /// compressed for a client that advertised the encoding, so nothing
    /// changes for one that did not ask.
    #[must_use]
    pub fn into_service(self) -> pb::lol_html_service_server::LolHtmlServiceServer<Self> {
        let backstop = self.max_chunk_bytes.saturating_mul(2);
        pb::lol_html_service_server::LolHtmlServiceServer::new(self)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(backstop)
    }
}

#[tonic::async_trait]
impl pb::lol_html_service_server::LolHtmlService for LolHtmlGrpc {
    type ExtractStream = OutboundStream;

    async fn extract(
        &self,
        request: Request<Streaming<pb::ExtractRequest>>,
    ) -> Result<Response<Self::ExtractStream>, Status> {
        // Held until the stream's driver task ends, so the count of open
        // streams is the count of live parsers. Fail fast rather than queue:
        // a caller past the cap needs to know now, not after its upload.
        let permit = Arc::clone(&self.stream_permits)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(
                    "the server is at its concurrent stream limit; retry shortly",
                )
            })?;

        let mut inbound = request.into_inner();

        // The options frame must arrive before anything is parsed, so every
        // way the request can be rejected is resolved here, as a status on
        // the call. Once the stream opens, only a parse failure can end it
        // badly, and that arrives in-band.
        let options = match inbound.message().await? {
            Some(pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Options(options)),
            }) => options,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "the first frame must carry `options`",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "the request stream closed before sending `options`",
                ));
            }
        };

        let compiled = rules::compile(&options, self.memory_ceiling)?;
        let (outbound, stream) = outbound::channel(self.outbound_buffer_bytes);
        let limits = Limits {
            max_chunk_bytes: self.max_chunk_bytes,
            idle_timeout: self.idle_timeout,
            send_timeout: self.send_timeout,
        };

        let span = tracing::info_span!("extract", rules = compiled.rules.len());
        tokio::spawn(
            async move {
                let ending = drive(&mut inbound, compiled, &outbound, limits).await;

                // Queued events are memory the stream slot accounts for, so
                // the slot is held until the client has taken them, or has
                // taken nothing for the send timeout and they are thrown
                // away. Released any earlier, a client that never reads
                // could leave a full queue behind for every slot it cycles
                // through.
                if matches!(ending, Ending::Complete | Ending::Early)
                    && outbound.delivered(limits.send_timeout).await == Delivery::Stalled
                {
                    tracing::warn!(
                        queued_bytes = outbound.queued_bytes(),
                        send_timeout_ms = limits.send_timeout.as_millis() as u64,
                        "client stopped reading; discarding its unread events"
                    );
                    outbound.abort(not_reading(limits.send_timeout));
                }
                drop(permit);

                // A client that reads only once its upload is finished cannot
                // see how the call ended until it has finished uploading, and
                // the server no longer reading its upload is what stops it.
                // So the rest is read and dropped, holding no slot and no
                // parser, until the status has gone out, the upload ends or
                // goes idle, or the client takes nothing for the send timeout.
                if matches!(ending, Ending::Early | Ending::NotReading) {
                    discard_upload(&mut inbound, &outbound, limits).await;
                }
            }
            .instrument(span),
        );

        Ok(Response::new(stream))
    }

    async fn validate_selectors(
        &self,
        request: Request<pb::ValidateSelectorsRequest>,
    ) -> Result<Response<pb::ValidateSelectorsResponse>, Status> {
        let rules = request.into_inner().rules;
        // The same shape limits as Extract, so a set this call passes is
        // never refused there for its size.
        rules::check_rule_set(&rules)?;
        let diagnostics = rules::diagnose(&rules);
        Ok(Response::new(pb::ValidateSelectorsResponse { diagnostics }))
    }

    async fn get_service_info(
        &self,
        _request: Request<pb::GetServiceInfoRequest>,
    ) -> Result<Response<pb::GetServiceInfoResponse>, Status> {
        // The UI block is a property of the build, hardcoded to match the
        // frontend this repo ships, so the shared demo shell can mount it
        // without any configuration.
        Ok(Response::new(pb::GetServiceInfoResponse {
            name: "grpc-lol-html".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            ui: Some(pb::UiInfo {
                title: "LOL HTML".to_owned(),
                path: "/ui/lol-html".to_owned(),
                description: "Streams CSS-selector matches out of HTML via lol-html".to_owned(),
            }),
        }))
    }
}

/// The per-call limits the driver enforces.
#[derive(Clone, Copy)]
struct Limits {
    max_chunk_bytes: usize,
    idle_timeout: Duration,
    send_timeout: Duration,
}

/// How a call's parse ended, which decides what is left to do for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// A terminal item is queued and the upload was read to its end.
    Complete,
    /// A terminal item is queued, but the client may still be uploading.
    Early,
    /// The client stopped reading: its unread events were discarded and a
    /// status queued in their place.
    NotReading,
    /// The response stream is gone; nothing more can be delivered.
    Gone,
}

/// The status a call ends with when its client stops reading.
fn not_reading(send_timeout: Duration) -> Status {
    Status::resource_exhausted(format!(
        "the client took no responses for {} ms while events were waiting, so the stream \
         was ended; read the response stream while uploading, not after",
        send_timeout.as_millis()
    ))
}

/// Run one document through the rewriter, forwarding events as they occur.
///
/// Returns once a terminal item is queued or the client is gone. What
/// happens after that, waiting for the client to take its result and
/// draining an upload that is still arriving, is the caller's.
async fn drive(
    inbound: &mut Streaming<pb::ExtractRequest>,
    compiled: CompiledOptions,
    outbound: &Outbound,
    limits: Limits,
) -> Ending {
    let started_at = Instant::now();
    let counters: Vec<Arc<AtomicU64>> = compiled
        .rules
        .iter()
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let encoding = compiled.encoding_name().to_owned();
    let sink = Sink {
        outbound: outbound.clone(),
        send_timeout: limits.send_timeout,
        text: Arc::new(TextBudget {
            held: AtomicUsize::new(0),
            limit: compiled.max_memory_bytes,
        }),
    };
    let settings = build_settings(&compiled, &sink, &counters);
    let mut rewriter = HtmlRewriter::new(settings, |_: &[u8]| {});

    // The sink is cloned into every handler; this original would otherwise
    // count as a producer for no reason.
    drop(sink);

    let started = pb::extract_response::Event::Started(pb::ExtractStarted {
        encoding,
        rule_count: u32::try_from(compiled.rules.len()).unwrap_or(u32::MAX),
    });
    if !outbound.push(started) {
        return Ending::Gone;
    }

    let mut bytes_parsed = 0u64;

    loop {
        // An open stream whose client has gone quiet still holds a task and a
        // parser, so silence is bounded. The timeout is idle, not total: it
        // resets with every frame, and a document of any length that keeps
        // sending never trips it.
        let frame = match next_frame(inbound, outbound, limits.idle_timeout).await {
            Ok(Ok(Some(request))) => request.frame,
            Ok(Ok(None)) => break,
            // The client's own stream failed. There is no useful in-band
            // event for that; propagate the status and stop.
            Ok(Err(status)) => {
                outbound.fail(status);
                return Ending::Early;
            }
            // Same shape as the arm above, for the same reason: the error
            // taxonomy in the contract mirrors lol-html's, and an idle client
            // is not a parse failure, so this ends the call with a status
            // rather than an in-band event.
            Err(_elapsed) => {
                tracing::warn!(
                    idle_timeout_ms = limits.idle_timeout.as_millis() as u64,
                    "client went idle; ending the stream"
                );
                outbound.fail(Status::deadline_exceeded(format!(
                    "no frame received within {} ms; the stream has been closed",
                    limits.idle_timeout.as_millis()
                )));
                return Ending::Early;
            }
        };

        let chunk = match frame {
            Some(pb::extract_request::Frame::Chunk(chunk)) => chunk,
            Some(pb::extract_request::Frame::Options(_)) => {
                outbound.fail(Status::invalid_argument(
                    "`options` may only be sent once, as the first frame",
                ));
                return Ending::Early;
            }
            // An empty frame carries nothing and means nothing; skip it
            // rather than treat it as end of document.
            None => continue,
        };

        if chunk.len() > limits.max_chunk_bytes {
            outbound.fail(Status::invalid_argument(format!(
                "chunk of {} bytes exceeds the {} byte limit; \
                 split the document into more, smaller chunks",
                chunk.len(),
                limits.max_chunk_bytes,
            )));
            return Ending::Early;
        }

        // CPU-bound, and blocking whenever the client reads slower than the
        // parse produces: the handlers wait in here for room in the outbound
        // queue. Neither belongs on an async worker.
        let size = chunk.len() as u64;
        let (returned, written) = match tokio::task::spawn_blocking(move || {
            let written = rewriter.write(&chunk);
            (rewriter, written)
        })
        .await
        {
            Ok(parsed) => parsed,
            Err(err) => {
                parse_task_failed(outbound, &err);
                return Ending::Early;
            }
        };
        rewriter = returned;

        if let Err(err) = written {
            if let Some(ending) = stopped_by_client(&err, outbound, limits, bytes_parsed) {
                return ending;
            }
            // Events produced before the failure are still valid and were
            // paid for, so they stay queued ahead of the error. The failed
            // chunk itself is not counted: it was not parsed.
            finish_with_error(
                outbound,
                &err,
                &compiled,
                bytes_parsed,
                &counters,
                &started_at,
            );
            return Ending::Early;
        }

        bytes_parsed += size;
    }

    // Whatever `end()` queues goes out with the terminal item, which always
    // wakes the response stream, so there is nothing to flush here.
    let ended = match tokio::task::spawn_blocking(move || rewriter.end()).await {
        Ok(ended) => ended,
        Err(err) => {
            parse_task_failed(outbound, &err);
            return Ending::Complete;
        }
    };

    if let Err(err) = ended {
        if let Some(ending) = stopped_by_client(&err, outbound, limits, bytes_parsed) {
            return ending;
        }
        finish_with_error(
            outbound,
            &err,
            &compiled,
            bytes_parsed,
            &counters,
            &started_at,
        );
        return Ending::Complete;
    }

    let matches = total_matches(&counters);
    let finished = pb::extract_response::Event::Finished(pb::ExtractFinished {
        bytes_parsed,
        matches_by_rule: tally(&compiled, &counters),
        bailed_out: false,
        bail_out_reason: String::new(),
    });
    outbound.push(finished);
    tracing::info!(
        bytes_parsed,
        matches,
        bailed_out = false,
        duration_ms = started_at.elapsed().as_millis() as u64,
        "stream finished"
    );
    Ending::Complete
}

/// The next frame of the upload, first releasing queued events to the client
/// if that frame is not already here.
///
/// Events are released when the driver is about to wait, not after every
/// chunk. Released per chunk, a client that uploads small chunks faster than
/// they are parsed gets one tiny DATA frame each, and h2 (0.4.18 onward)
/// treats a pile of unread tiny frames as a flood and closes the connection.
/// Waiting is the moment nothing more is coming soon, so this costs no
/// latency: a match still reaches the client before the rest of the upload.
async fn next_frame(
    inbound: &mut Streaming<pb::ExtractRequest>,
    outbound: &Outbound,
    idle_timeout: Duration,
) -> Result<Result<Option<pb::ExtractRequest>, Status>, tokio::time::error::Elapsed> {
    let mut next = std::pin::pin!(inbound.message());
    if let Poll::Ready(frame) = std::future::poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx))).await
    {
        return Ok(frame);
    }
    outbound.flush();
    tokio::time::timeout(idle_timeout, next).await
}

/// The ending for a parse one of the server's own handlers stopped because
/// of the client rather than the document, or `None` when the failure is the
/// document's and belongs in-band.
fn stopped_by_client(
    err: &RewritingError,
    outbound: &Outbound,
    limits: Limits,
    bytes_parsed: u64,
) -> Option<Ending> {
    match errors::handler_stop(err)? {
        HandlerStop::ClientGone => Some(Ending::Gone),
        HandlerStop::NotReading { queued_bytes } => {
            tracing::warn!(
                queued_bytes,
                bytes_parsed,
                send_timeout_ms = limits.send_timeout.as_millis() as u64,
                "client stopped reading; ending the stream"
            );
            // Discarded rather than left for a client that may never read:
            // the queue is the memory the stream slot, about to be released,
            // was accounting for.
            outbound.abort(not_reading(limits.send_timeout));
            Some(Ending::NotReading)
        }
        HandlerStop::TextOverLimit { .. } => None,
    }
}

/// End a call whose parse task died, which only a bug can cause: a panic in
/// a debug build, since release builds abort on panic.
fn parse_task_failed(outbound: &Outbound, err: &tokio::task::JoinError) {
    tracing::error!(error = %err, "the parse task failed");
    outbound.fail(Status::internal("the parse failed inside the server"));
}

/// Read and drop the rest of an upload the server has stopped parsing, so a
/// client that reads only after uploading gets to its read and sees how the
/// call ended.
///
/// Stops as soon as the terminal item has gone out or the client is gone,
/// when the upload ends, fails or goes idle, or once the client has taken
/// nothing for the send timeout, which bounds how long a client that keeps
/// uploading can keep this going.
async fn discard_upload(
    inbound: &mut Streaming<pb::ExtractRequest>,
    outbound: &Outbound,
    limits: Limits,
) {
    let delivered = outbound.delivered(limits.send_timeout);
    tokio::pin!(delivered);
    loop {
        tokio::select! {
            _ = &mut delivered => return,
            frame = tokio::time::timeout(limits.idle_timeout, inbound.message()) => {
                if !matches!(frame, Ok(Ok(Some(_)))) {
                    return;
                }
            }
        }
    }
}

/// Close a run that ended in a parse failure.
///
/// A memory-limit failure becomes a truncated-but-successful run when the
/// caller asked for that; everything else is terminal. An ambiguity bail-out
/// is never graceful, because continuing past it is exactly what strict mode
/// exists to refuse.
fn finish_with_error(
    outbound: &Outbound,
    err: &RewritingError,
    compiled: &CompiledOptions,
    bytes_parsed: u64,
    counters: &[Arc<AtomicU64>],
    started_at: &Instant,
) {
    let event = if compiled.graceful_bail_out && errors::is_memory_limit(err) {
        pb::extract_response::Event::Finished(pb::ExtractFinished {
            bytes_parsed,
            matches_by_rule: tally(compiled, counters),
            bailed_out: true,
            bail_out_reason: err.to_string(),
        })
    } else {
        pb::extract_response::Event::Error(errors::stream_error(err))
    };
    let bailed_out = matches!(&event, pb::extract_response::Event::Finished(_));
    outbound.push(event);
    tracing::info!(
        bytes_parsed,
        matches = total_matches(counters),
        bailed_out,
        error = %err,
        duration_ms = started_at.elapsed().as_millis() as u64,
        "stream ended on a parse failure"
    );
}

/// Sum every rule's matches into one number, for the log line.
fn total_matches(counters: &[Arc<AtomicU64>]) -> u64 {
    counters.iter().map(|c| c.load(Ordering::Relaxed)).sum()
}

/// Sum per-rule match counts, keyed by rule id.
///
/// Rule ids need not be unique, so counts are summed rather than overwritten,
/// and every rule appears even at zero so a caller can tell "matched nothing"
/// from "no such rule".
fn tally(compiled: &CompiledOptions, counters: &[Arc<AtomicU64>]) -> HashMap<String, u64> {
    let mut totals = HashMap::with_capacity(compiled.rules.len());
    for (rule, counter) in compiled.rules.iter().zip(counters) {
        *totals.entry(rule.id.clone()).or_insert(0) += counter.load(Ordering::Relaxed);
    }
    totals
}

/// Where the handlers put events: the call's outbound queue, waiting up to
/// the send timeout for room, plus the count of text held for reassembly.
#[derive(Clone)]
struct Sink {
    outbound: Outbound,
    send_timeout: Duration,
    text: Arc<TextBudget>,
}

impl Sink {
    /// Queue one event, or stop the parse if it can no longer be delivered.
    ///
    /// Runs on the blocking pool, inside `write()`: waiting here for room is
    /// what holds the parse still for a slow client.
    fn emit(&self, event: pb::extract_response::Event) -> lol_html::HandlerResult {
        let response = pb::ExtractResponse { event: Some(event) };
        self.outbound
            .send_blocking(response, self.send_timeout)
            .map_err(|err| {
                match err {
                    SendError::Closed => HandlerStop::ClientGone,
                    SendError::Stalled { queued_bytes } => HandlerStop::NotReading { queued_bytes },
                }
                .into()
            })
    }
}

/// Text held for reassembly across one call's handlers, counted against the
/// call's memory limit.
///
/// lol-html's own limit cannot see this text. lol-html streams text out in
/// pieces precisely so that it never holds a whole node; the server holds it
/// instead, and without a count of its own one long text node, or one copy
/// per text rule, would grow without bound.
struct TextBudget {
    held: AtomicUsize,
    limit: usize,
}

impl TextBudget {
    /// Count `bytes` more text as held, reporting whether that stays within
    /// the limit. Over it the parse ends, so the count is not rolled back.
    fn hold(&self, bytes: usize) -> bool {
        self.held.fetch_add(bytes, Ordering::Relaxed) + bytes <= self.limit
    }

    /// Stop counting `bytes` of text that has left the handler.
    fn release(&self, bytes: usize) {
        self.held.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// Assemble the rewriter settings from a validated request.
fn build_settings(
    compiled: &CompiledOptions,
    sink: &Sink,
    counters: &[Arc<AtomicU64>],
) -> Settings<'static, 'static> {
    let mut settings = Settings::new_send()
        .with_encoding(compiled.encoding)
        .with_memory_settings(compiled.memory_settings())
        .with_strict(compiled.strict)
        .with_enable_esi_tags(compiled.enable_esi_tags)
        .with_adjust_charset_on_meta_tag(compiled.adjust_charset_on_meta_tag);

    for (rule, counter) in compiled.rules.iter().zip(counters) {
        settings = settings.append_element_content_handler((
            std::borrow::Cow::Owned(rule.selector.clone()),
            element_handlers(rule, sink, Arc::clone(counter), compiled),
        ));
    }

    if let Some(scope) = &compiled.document {
        settings =
            settings.append_document_content_handler(document_handlers(scope, sink, compiled));
    }

    settings
}

/// Build the per-selector handlers for one rule.
fn element_handlers(
    rule: &CompiledRule,
    sink: &Sink,
    counter: Arc<AtomicU64>,
    compiled: &CompiledOptions,
) -> ElementContentHandlers<'static> {
    let mut handlers = ElementContentHandlers::default();

    {
        let (id, sink) = (rule.id.clone(), sink.clone());
        let (want_tag, want_attrs, want_spans, want_end) =
            (rule.tag_name, rule.attributes, rule.spans, rule.end_tag);

        handlers = handlers.element(move |el: &mut lol_html::send::Element<'_, '_>| {
            counter.fetch_add(1, Ordering::Relaxed);

            let event = pb::ElementMatched {
                rule_id: id.clone(),
                tag_name: if want_tag {
                    el.tag_name()
                } else {
                    String::new()
                },
                tag_name_raw: if want_tag {
                    el.tag_name_preserve_case()
                } else {
                    String::new()
                },
                namespace: convert::namespace(el.namespace_uri()) as i32,
                self_closing: el.is_self_closing(),
                can_have_content: el.can_have_content(),
                attributes: if want_attrs {
                    el.attributes()
                        .iter()
                        .map(|attr| convert::attribute(attr, want_spans))
                        .collect()
                } else {
                    Vec::new()
                },
                span: want_spans.then(|| convert::source_span(&el.source_location())),
            };
            sink.emit(pb::extract_response::Event::Element(event))?;

            if want_end {
                let (id, sink) = (id.clone(), sink.clone());
                // Fires only if the element actually closes. A void element,
                // a self-closing tag, and an element the document never
                // closes all produce nothing here, which is why the contract
                // says this is not a dependable "element finished" signal.
                let registered = el.on_end_tag(Box::new(move |end: &mut EndTag<'_>| {
                    sink.emit(pb::extract_response::Event::EndTag(pb::EndTagFound {
                        rule_id: id,
                        name: end.name(),
                        name_raw: end.name_preserve_case(),
                        span: want_spans.then(|| convert::source_span(&end.source_location())),
                    }))
                }));
                // `on_end_tag` refuses on an element that cannot have one.
                // That is information, not a failure: there is simply no end
                // tag to report.
                let _ = registered;
            }

            Ok(())
        });
    }

    if rule.text {
        handlers = handlers.text(text_handler(
            rule.id.clone(),
            sink.clone(),
            rule.spans,
            compiled,
        ));
    }

    if rule.comments {
        let (id, sink, want_spans) = (rule.id.clone(), sink.clone(), rule.spans);
        handlers = handlers.comments(move |comment: &mut Comment<'_>| {
            sink.emit(pb::extract_response::Event::Comment(pb::CommentFound {
                rule_id: id.clone(),
                text: comment.text(),
                span: want_spans.then(|| convert::source_span(&comment.source_location())),
            }))
        });
    }

    handlers
}

/// Build the document-scope handlers.
fn document_handlers(
    scope: &rules::DocumentScope,
    sink: &Sink,
    compiled: &CompiledOptions,
) -> DocumentContentHandlers<'static> {
    let mut handlers = DocumentContentHandlers::default();

    if scope.doctype {
        let (id, sink, want_spans) = (scope.id.clone(), sink.clone(), scope.spans);
        handlers = handlers.doctype(move |doctype: &mut Doctype<'_>| {
            sink.emit(pb::extract_response::Event::Doctype(pb::DoctypeFound {
                rule_id: id.clone(),
                name: doctype.name(),
                public_id: doctype.public_id(),
                system_id: doctype.system_id(),
                span: want_spans.then(|| convert::source_span(&doctype.source_location())),
            }))
        });
    }

    if scope.comments {
        let (id, sink, want_spans) = (scope.id.clone(), sink.clone(), scope.spans);
        handlers = handlers.comments(move |comment: &mut Comment<'_>| {
            sink.emit(pb::extract_response::Event::Comment(pb::CommentFound {
                rule_id: id.clone(),
                text: comment.text(),
                span: want_spans.then(|| convert::source_span(&comment.source_location())),
            }))
        });
    }

    if scope.text {
        handlers = handlers.text(text_handler(
            scope.id.clone(),
            sink.clone(),
            scope.spans,
            compiled,
        ));
    }

    handlers
}

/// Build a text handler that reassembles text nodes, or forwards raw
/// fragments when the caller asked for those.
///
/// lol-html splits text at buffer boundaries, so one text node arrives as any
/// number of `TextChunk`s with `last_in_text_node()` marking the end. Handing
/// those to a caller unreassembled is the single most reliable way to make a
/// client look correct in testing and cut words in half in production, which
/// is why reassembly is the default and the raw view is opt-in.
///
/// The text held while a node is reassembled counts against the call's
/// memory limit, summed across rules. A node that outgrows it ends the run
/// the way any other memory-limit overrun does, rather than being split into
/// pieces a caller relying on whole nodes would not expect.
fn text_handler(
    rule_id: String,
    sink: Sink,
    want_spans: bool,
    compiled: &CompiledOptions,
) -> impl FnMut(&mut TextChunk<'_>) -> lol_html::HandlerResult + Send + 'static {
    let raw = compiled.raw_text_chunks;
    let allowed = compiled.text_types.clone();

    let mut buffer = String::new();
    let mut span: Option<(u64, u64)> = None;

    move |chunk: &mut TextChunk<'_>| {
        let text_type = convert::text_type(chunk.text_type());
        let last = chunk.last_in_text_node();

        if !allowed.contains(&text_type) {
            // Reset even for text we are dropping: a filtered node must not
            // bleed into whatever is accumulated next.
            if last {
                sink.text.release(buffer.len());
                buffer.clear();
                span = None;
            }
            return Ok(());
        }

        if raw {
            // Every fragment as lol-html produced it, including the empty
            // terminator, so `last_in_node` always reaches the client and
            // concatenating a node's fragments reproduces the reassembled
            // text exactly.
            return sink.emit(pb::extract_response::Event::Text(pb::TextNode {
                rule_id: rule_id.clone(),
                text: chunk.as_str().to_owned(),
                text_type: text_type as i32,
                span: want_spans.then(|| convert::source_span(&chunk.source_location())),
                last_in_node: last,
            }));
        }

        if want_spans {
            let bytes = convert::source_span(&chunk.source_location());
            span = Some(match span {
                Some((start, _)) => (start, bytes.end),
                None => (bytes.start, bytes.end),
            });
        }

        let piece = chunk.as_str();
        if !piece.is_empty() {
            // Counted before it is copied, so the copy that would cross the
            // limit is never made.
            if !sink.text.hold(piece.len()) {
                return Err(HandlerStop::TextOverLimit {
                    limit_bytes: sink.text.limit,
                }
                .into());
            }
            buffer.push_str(piece);
        }

        if last {
            sink.text.release(buffer.len());
            // A text node with no text is not an event, and lol-html emits an
            // empty terminating chunk for every node, so this is the common
            // case rather than an edge one.
            if !buffer.is_empty() {
                // Taken rather than copied or cleared, so a long node leaves
                // no allocation of its size behind in this handler.
                let written = std::mem::take(&mut buffer);
                // lol-html hands text back exactly as written, so the entity
                // decode the proto promises for DATA and RCDATA happens
                // here, after reassembly — an entity split across fragments
                // is whole again by this point. Raw mode never reaches this
                // branch: fragments go out verbatim, as documented.
                let text = if chunk.text_type().allows_html_entities() && written.contains('&') {
                    htmlize::unescape(written.as_str()).into_owned()
                } else {
                    written
                };
                sink.emit(pb::extract_response::Event::Text(pb::TextNode {
                    rule_id: rule_id.clone(),
                    text,
                    text_type: text_type as i32,
                    span: span.map(|(start, end)| pb::SourceSpan { start, end }),
                    last_in_node: true,
                }))?;
            }
            span = None;
        }

        Ok(())
    }
}
