// SPDX-License-Identifier: Apache-2.0

//! The `Extract` driver: lol-html's synchronous handlers bridged onto a tonic
//! response stream without losing backpressure.
//!
//! # Why this is not a one-liner
//!
//! lol-html's content handlers are plain synchronous `FnMut` closures. A tonic
//! response stream is fed by `tx.send(event).await`. You cannot await inside a
//! closure, and `Sender::blocking_send` inside the async runtime deadlocks the
//! worker it is running on. The obvious escape, an unbounded channel from the
//! handlers, throws away backpressure, which is the one property this service
//! advertises.
//!
//! So the handlers push into an unbounded [`tokio::sync::mpsc`], whose `send`
//! is a plain synchronous call that never blocks, and [`drain`] empties that
//! queue after every `rewriter.write()`, awaiting each forward onto the
//! bounded outbound channel. Unbounded is safe precisely because the queue is
//! drained every chunk, so it never holds more than one chunk's worth of
//! events; the bounded outbound channel is where backpressure actually lives.
//! Awaiting the drain is what stops the driver reading the next inbound
//! chunk, so a slow client slows the parser rather than growing a queue
//! behind it.
//!
//! The queue is tokio's rather than [`std::sync::mpsc`] for a narrow reason:
//! `std`'s `Receiver` is `Send` but not `Sync`, so holding one across an
//! `.await` makes the whole future non-`Send` and `tokio::spawn` rejects it.
//!
//! `write()` is CPU-bound but runs one bounded chunk at a time, so it runs
//! inline on the async task rather than on the blocking pool. The cap on
//! inbound chunk size is what makes that safe: one oversized chunk would be
//! one long uninterruptible parse.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lol_html::errors::RewritingError;
use lol_html::html_content::{Comment, Doctype, EndTag, TextChunk};
use lol_html::send::{DocumentContentHandlers, ElementContentHandlers, HtmlRewriter, Settings};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::v1 as pb;
use crate::rules::{CompiledOptions, CompiledRule};
use crate::{convert, errors, rules};

/// Events buffered on the outbound channel before the driver has to wait.
const OUTBOUND_BUFFER: usize = 256;

/// Largest single inbound chunk accepted, in bytes.
///
/// Not a document size limit: a document is any number of chunks. This bounds
/// how long one uninterruptible `rewriter.write()` can run on an async worker.
const DEFAULT_MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// Queue the handlers push into, drained after every chunk.
type EventSink = mpsc::UnboundedSender<pb::extract_response::Event>;

/// The `lolhtml.v1.LolHtmlService` implementation.
pub struct LolHtmlGrpc {
    max_chunk_bytes: usize,
}

impl Default for LolHtmlGrpc {
    fn default() -> Self {
        Self::new()
    }
}

impl LolHtmlGrpc {
    /// Create a service with default limits.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_chunk_bytes: DEFAULT_MAX_CHUNK_BYTES,
        }
    }

    /// Override the largest accepted inbound chunk.
    #[must_use]
    pub const fn with_max_chunk_bytes(mut self, bytes: usize) -> Self {
        self.max_chunk_bytes = bytes;
        self
    }

    /// Wrap this service in its generated tonic server.
    #[must_use]
    pub fn into_service(self) -> pb::lol_html_service_server::LolHtmlServiceServer<Self> {
        pb::lol_html_service_server::LolHtmlServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::lol_html_service_server::LolHtmlService for LolHtmlGrpc {
    type ExtractStream = ReceiverStream<Result<pb::ExtractResponse, Status>>;

    async fn extract(
        &self,
        request: Request<Streaming<pb::ExtractRequest>>,
    ) -> Result<Response<Self::ExtractStream>, Status> {
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

        let compiled = rules::compile(&options)?;
        let (tx, rx) = mpsc::channel(OUTBOUND_BUFFER);
        let max_chunk_bytes = self.max_chunk_bytes;

        tokio::spawn(async move { drive(inbound, compiled, tx, max_chunk_bytes).await });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn validate_selectors(
        &self,
        request: Request<pb::ValidateSelectorsRequest>,
    ) -> Result<Response<pb::ValidateSelectorsResponse>, Status> {
        let diagnostics = rules::diagnose(&request.into_inner().rules);
        Ok(Response::new(pb::ValidateSelectorsResponse { diagnostics }))
    }
}

/// Run one document through the rewriter, forwarding events as they occur.
async fn drive(
    mut inbound: Streaming<pb::ExtractRequest>,
    compiled: CompiledOptions,
    tx: mpsc::Sender<Result<pb::ExtractResponse, Status>>,
    max_chunk_bytes: usize,
) {
    let (sink, mut events) = mpsc::unbounded_channel();
    let counters: Vec<Arc<AtomicU64>> = compiled
        .rules
        .iter()
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let encoding = compiled.encoding_name().to_owned();
    let settings = build_settings(&compiled, &sink, &counters);
    let mut rewriter = HtmlRewriter::new(settings, |_: &[u8]| {});

    // The sink is cloned into every handler; this original would otherwise
    // keep the channel open for no reason.
    drop(sink);

    let started = pb::extract_response::Event::Started(pb::ExtractStarted {
        encoding,
        rule_count: u32::try_from(compiled.rules.len()).unwrap_or(u32::MAX),
    });
    if !send(&tx, started).await {
        return;
    }

    let mut bytes_parsed = 0u64;

    loop {
        let frame = match inbound.message().await {
            Ok(Some(request)) => request.frame,
            Ok(None) => break,
            // The client's own stream failed. There is no useful in-band
            // event for that; propagate the status and stop.
            Err(status) => {
                let _ = tx.send(Err(status)).await;
                return;
            }
        };

        let chunk = match frame {
            Some(pb::extract_request::Frame::Chunk(chunk)) => chunk,
            Some(pb::extract_request::Frame::Options(_)) => {
                let _ = tx
                    .send(Err(Status::invalid_argument(
                        "`options` may only be sent once, as the first frame",
                    )))
                    .await;
                return;
            }
            // An empty frame carries nothing and means nothing; skip it
            // rather than treat it as end of document.
            None => continue,
        };

        if chunk.len() > max_chunk_bytes {
            let _ = tx
                .send(Err(Status::invalid_argument(format!(
                    "chunk of {} bytes exceeds the {max_chunk_bytes} byte limit; \
                     split the document into more, smaller chunks",
                    chunk.len(),
                ))))
                .await;
            return;
        }

        bytes_parsed += chunk.len() as u64;

        if let Err(err) = rewriter.write(&chunk) {
            // Events produced before the failure are still valid and were
            // paid for, so they go out ahead of the error.
            drain(&mut events, &tx).await;
            finish_with_error(&tx, &err, &compiled, bytes_parsed, &counters).await;
            return;
        }

        if !drain(&mut events, &tx).await {
            return;
        }
    }

    if let Err(err) = rewriter.end() {
        drain(&mut events, &tx).await;
        finish_with_error(&tx, &err, &compiled, bytes_parsed, &counters).await;
        return;
    }

    if !drain(&mut events, &tx).await {
        return;
    }

    let finished = pb::extract_response::Event::Finished(pb::ExtractFinished {
        bytes_parsed,
        matches_by_rule: tally(&compiled, &counters),
        bailed_out: false,
        bail_out_reason: String::new(),
    });
    send(&tx, finished).await;
}

/// Close a run that ended in a parse failure.
///
/// A memory-limit failure becomes a truncated-but-successful run when the
/// caller asked for that; everything else is terminal. An ambiguity bail-out
/// is never graceful, because continuing past it is exactly what strict mode
/// exists to refuse.
async fn finish_with_error(
    tx: &mpsc::Sender<Result<pb::ExtractResponse, Status>>,
    err: &RewritingError,
    compiled: &CompiledOptions,
    bytes_parsed: u64,
    counters: &[Arc<AtomicU64>],
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
    send(tx, event).await;
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

/// Forward one event, reporting whether the client is still there.
async fn send(
    tx: &mpsc::Sender<Result<pb::ExtractResponse, Status>>,
    event: pb::extract_response::Event,
) -> bool {
    tx.send(Ok(pb::ExtractResponse { event: Some(event) }))
        .await
        .is_ok()
}

/// Empty the handler queue onto the response stream.
///
/// This is where backpressure lives: each send awaits, so a client that reads
/// slowly stops the driver here, and the driver therefore stops reading
/// inbound chunks. Returns false once the client has gone away.
async fn drain(
    events: &mut mpsc::UnboundedReceiver<pb::extract_response::Event>,
    tx: &mpsc::Sender<Result<pb::ExtractResponse, Status>>,
) -> bool {
    // `try_recv` returns Disconnected once the rewriter, and with it every
    // handler, has been dropped. Either error means nothing more is coming.
    while let Ok(event) = events.try_recv() {
        if !send(tx, event).await {
            return false;
        }
    }
    true
}

/// Assemble the rewriter settings from a validated request.
fn build_settings(
    compiled: &CompiledOptions,
    sink: &EventSink,
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
    sink: &EventSink,
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
            let _ = sink.send(pb::extract_response::Event::Element(event));

            if want_end {
                let (id, sink) = (id.clone(), sink.clone());
                // Fires only if the element actually closes. A void element,
                // a self-closing tag, and an element the document never
                // closes all produce nothing here, which is why the contract
                // says this is not a dependable "element finished" signal.
                let registered = el.on_end_tag(Box::new(move |end: &mut EndTag<'_>| {
                    let _ = sink.send(pb::extract_response::Event::EndTag(pb::EndTagFound {
                        rule_id: id,
                        name: end.name(),
                        name_raw: end.name_preserve_case(),
                        span: want_spans.then(|| convert::source_span(&end.source_location())),
                    }));
                    Ok(())
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
            let _ = sink.send(pb::extract_response::Event::Comment(pb::CommentFound {
                rule_id: id.clone(),
                text: comment.text(),
                span: want_spans.then(|| convert::source_span(&comment.source_location())),
            }));
            Ok(())
        });
    }

    handlers
}

/// Build the document-scope handlers.
fn document_handlers(
    scope: &rules::DocumentScope,
    sink: &EventSink,
    compiled: &CompiledOptions,
) -> DocumentContentHandlers<'static> {
    let mut handlers = DocumentContentHandlers::default();

    if scope.doctype {
        let (id, sink, want_spans) = (scope.id.clone(), sink.clone(), scope.spans);
        handlers = handlers.doctype(move |doctype: &mut Doctype<'_>| {
            let _ = sink.send(pb::extract_response::Event::Doctype(pb::DoctypeFound {
                rule_id: id.clone(),
                name: doctype.name(),
                public_id: doctype.public_id(),
                system_id: doctype.system_id(),
                span: want_spans.then(|| convert::source_span(&doctype.source_location())),
            }));
            Ok(())
        });
    }

    if scope.comments {
        let (id, sink, want_spans) = (scope.id.clone(), sink.clone(), scope.spans);
        handlers = handlers.comments(move |comment: &mut Comment<'_>| {
            let _ = sink.send(pb::extract_response::Event::Comment(pb::CommentFound {
                rule_id: id.clone(),
                text: comment.text(),
                span: want_spans.then(|| convert::source_span(&comment.source_location())),
            }));
            Ok(())
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
fn text_handler(
    rule_id: String,
    sink: EventSink,
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
            let _ = sink.send(pb::extract_response::Event::Text(pb::TextNode {
                rule_id: rule_id.clone(),
                text: chunk.as_str().to_owned(),
                text_type: text_type as i32,
                span: want_spans.then(|| convert::source_span(&chunk.source_location())),
                last_in_node: last,
            }));
            return Ok(());
        }

        if want_spans {
            let bytes = convert::source_span(&chunk.source_location());
            span = Some(match span {
                Some((start, _)) => (start, bytes.end),
                None => (bytes.start, bytes.end),
            });
        }
        buffer.push_str(chunk.as_str());

        if last {
            // A text node with no text is not an event, and lol-html emits an
            // empty terminating chunk for every node, so this is the common
            // case rather than an edge one.
            if !buffer.is_empty() {
                let _ = sink.send(pb::extract_response::Event::Text(pb::TextNode {
                    rule_id: rule_id.clone(),
                    text: std::mem::take(&mut buffer),
                    text_type: text_type as i32,
                    span: span.map(|(start, end)| pb::SourceSpan { start, end }),
                    last_in_node: true,
                }));
            }
            buffer.clear();
            span = None;
        }

        Ok(())
    }
}
