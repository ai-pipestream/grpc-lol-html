// SPDX-License-Identifier: Apache-2.0

//! The claim this service is built on, measured rather than asserted.
//!
//! "Server memory does not grow with document size" is the reason there is no
//! handle store, no `document_id` and no `CloseDocument` in the contract.
//! Every other test here checks what the server *says*; these check what it
//! *costs*, by watching the real server binary's peak resident set while the
//! document it is fed grows by two orders of magnitude, and while one text
//! node grows to the size of the whole document.
//!
//! It runs the shipped binary as a child process rather than a server inside
//! the test, because a test-hosted server shares an address space with the
//! client, the fixtures and the harness, and the number would mean nothing.
//!
//! Linux only: peak RSS comes from `/proc/<pid>/status`. Elsewhere the test
//! compiles to nothing, and CI is Linux.

#![cfg(target_os = "linux")]

use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};

use grpc_lol_html::proto::v1 as pb;
use grpc_lol_html::proto::v1::lol_html_service_client::LolHtmlServiceClient;

/// Upload chunk size, matching the benchmark's default.
const CHUNK: usize = 256 * 1024;

/// The cold run, reported but not asserted on.
///
/// A process that has just started has not yet touched its HTTP/2 windows, its
/// allocator arenas or its per-connection buffers, so the step up from it
/// measures starting up rather than the thing being claimed.
const COLD_MIB: usize = 1;

/// The warm-up document, repeated until the server's resident set stops
/// moving, and the size the large document is compared against.
///
/// mimalloc keeps a heap per thread, and the parse moves between the async
/// workers and the blocking pool from one chunk to the next, so resident memory
/// keeps climbing until every thread that will carry the work has held its
/// share of it. On a 4-core machine that took anywhere from 16 MiB to 128 MiB
/// of document, and it scaled with the worker count (about 30, 42 and 52 MiB
/// at one, two and four workers) rather than with the document.
const WARM_UP_MIB: usize = 64;

/// How many warm-up documents run before anything is measured.
///
/// Settling is not asserted on: what the warm-ups leave behind is the
/// reference, and the comparisons below are made against it.
const WARM_UPS: usize = 3;

/// The async worker count this test runs the server with.
///
/// Settled memory grows with the worker count, so the default of one per core
/// would make the reference depend on the machine. Four is what the CI runners
/// have.
const WORKERS: &str = "4";

/// The outbound buffer and HTTP/2 window this test runs the server with.
///
/// Both are bounded by configuration rather than by the document, and both
/// fill only while the client falls behind, which a test client sharing one
/// thread with its own uploader does now and then. Small ones keep how full
/// they happen to run out of the reading.
const BUFFER_BYTES: u64 = 1024 * 1024;

/// The document compared against the warm-up sized ones, four times their size.
const LARGE_MIB: usize = 256;

/// How many warm-up sized documents the large one is compared with: the same
/// number of bytes in documents a quarter of the size.
const SMALL_DOCUMENTS: usize = LARGE_MIB / WARM_UP_MIB;

/// How many times the comparison runs, interleaved.
///
/// The resident set sits several MiB higher while the machine is busy, because
/// a client that is starved of CPU falls behind and the server's bounded
/// buffers run fuller. Run back to back, small then large, a burst of load
/// from anything else on the machine lands on one side and reads as growth.
/// Interleaved (half the small documents, the large one, the other half, and
/// again), load that comes and goes over seconds falls on both sides alike.
const ROUNDS: usize = 2;

/// How much the median may climb from the first round to the last.
///
/// This check is not about document size but about state held between calls,
/// which the interleaving above hides from the size comparison. Round to round
/// the median wandered between -10 and +7 MiB in 41 runs of a server that
/// holds nothing, so this sits clear of that. A server keeping 1/16 of every
/// chunk climbed about 40 MiB and failed every time; one keeping 1/64 climbed
/// 5 to 12 MiB and is below what this can tell from noise.
const ALLOWED_CLIMB_BYTES: u64 = 16 * 1024 * 1024;

/// The tests in this file each start a server and stream hundreds of MiB
/// through it, and run side by side they load the machine for each other.
/// Holding this for the whole of a test keeps each one's numbers its own.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How often the server's resident set is sampled while a document streams.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(2);

/// How much higher the median resident set may sit while the large document
/// streams than while the same bytes stream as warm-up sized documents.
///
/// The comparison is between medians of the resident set sampled throughout,
/// not between high-water marks. A high-water mark is the single largest moment
/// of a run. In the image build on 2026-10-20 one such moment, about 10 MiB
/// above a server that was otherwise flat, failed the high-water versions of
/// this test in three of the four publish legs that ran them, under conditions
/// that 220 repeated runs on GitHub runners and on a workstation, the same
/// multi-arch build among them, never reproduced. A median
/// does not move for a moment. A server that retained the document grows
/// through the whole large run: with a copy of every chunk held for the call,
/// the large side's median sat 147 to 159 MiB up. This sits far below that and
/// above the -7 to +4 MiB the comparison moved by in 71 runs of a server that
/// holds nothing.
const ALLOWED_GROWTH_BYTES: u64 = 8 * 1024 * 1024;

/// How much peak RSS a 128 MiB text node may add over a 64 MiB warm-up.
///
/// Looser than [`ALLOWED_GROWTH_BYTES`] because this test strings together
/// three runs that behave differently, raw fragments and an in-band failure,
/// and the peak wanders by several MiB between identical runs of a process
/// with a thread pool's worth of allocator heaps. It still sits an order of
/// magnitude below what holding the node would cost: all 128 MiB of it, and
/// a decoded copy besides.
const ALLOWED_TEXT_NODE_GROWTH_BYTES: u64 = 16 * 1024 * 1024;

/// A parser cap far below the document size, so the run also shows that
/// streaming a 64 MiB page does not accumulate 64 MiB of parser state.
const MAX_PARSER_BYTES: u64 = 1024 * 1024;

/// Kills the server on the way out, including when an assertion panics.
struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Current resident set size of a process, in bytes.
fn rss(pid: u32) -> u64 {
    proc_status_kb(pid, "VmRSS:") * 1024
}

/// Peak resident set size of a process, in bytes.
fn peak_rss(pid: u32) -> u64 {
    proc_status_kb(pid, "VmHWM:") * 1024
}

/// One kB-valued field of `/proc/<pid>/status`.
fn proc_status_kb(pid: u32, field: &str) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("read /proc status");
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|kb| kb.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{field} in /proc status"))
}

/// Start the shipped binary on a free port, with `envs` added to its
/// environment, and connect to it.
async fn start_server_process(
    envs: &[(&str, &str)],
) -> (ServerProcess, LolHtmlServiceClient<Channel>) {
    // Take a port from the kernel, then hand the number to the child. There is
    // a window between the bind and the child's own bind, which is why the
    // connect below retries rather than assuming.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");

    let child = Command::new(env!("CARGO_BIN_EXE_grpc-lol-html"))
        .env("GRPC_LOL_HTML_ADDR", &addr)
        // mimalloc holds freed spans for a purge delay before returning them,
        // which is allocator caching proportional to throughput, not document
        // retention — and exactly the noise this test exists to exclude.
        // Zeroing the delay makes the shipped binary behave for the
        // measurement like the reading the claim needs.
        .env("MIMALLOC_PURGE_DELAY", "0")
        .envs(envs.iter().copied())
        .spawn()
        .expect("spawn the server binary");
    let server = ServerProcess(child);

    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    for attempt in 0..100 {
        if let Ok(channel) = endpoint.connect().await {
            return (server, LolHtmlServiceClient::new(channel));
        }
        tokio::time::sleep(Duration::from_millis(50 * (attempt / 10 + 1))).await;
    }
    panic!("the server binary never accepted a connection on {addr}");
}

/// One repeating ~1 KiB block, so a document of any size is the same block
/// repeated and bytes are the only variable between runs.
fn block() -> Vec<u8> {
    let mut out = String::with_capacity(1100);
    out.push_str("<section class=\"card\"><h2>Heading</h2><div class=\"row\">");
    for n in 0..6 {
        out.push_str(&format!(
            "<a href=\"/item/{n}\" rel=\"noopener\" title=\"Item {n}\">Item {n}</a>"
        ));
    }
    out.push_str("<p class=\"body\">");
    while out.len() < 1000 {
        out.push_str("streaming parser throughput selector boundary ");
    }
    out.push_str("</p></div></section>\n");
    out.into_bytes()
}

/// Stream `mib` mebibytes of generated HTML through `Extract` and return how
/// many elements matched.
///
/// The document is generated a chunk at a time and fed through a bounded
/// channel, so the *client* never holds it either. A test that built a 64 MiB
/// `Vec` first would still prove the point about the server, but it would also
/// be measuring a harness that does exactly what the server is being praised
/// for not doing.
async fn stream_document(client: &LolHtmlServiceClient<Channel>, mib: usize) -> u64 {
    let mut client = client.clone();
    let (tx, rx) = mpsc::channel(4);

    let options = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "links".to_owned(),
            selector: "a[href]".to_owned(),
            captures: vec![pb::Capture::TagName as i32, pb::Capture::Attributes as i32],
        }],
        limits: Some(pb::MemoryLimits {
            max_bytes: MAX_PARSER_BYTES,
            ..Default::default()
        }),
        ..Default::default()
    };

    tokio::spawn(async move {
        let sent = tx
            .send(pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Options(options)),
            })
            .await;
        if sent.is_err() {
            return;
        }

        let block = block();
        let target = mib * 1024 * 1024;
        let mut chunk = Vec::with_capacity(CHUNK + block.len());
        let mut written = 0;
        while written < target {
            while chunk.len() < CHUNK {
                chunk.extend_from_slice(&block);
            }
            written += chunk.len();
            let next = Vec::with_capacity(CHUNK + block.len());
            let frame = pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Chunk(std::mem::replace(
                    &mut chunk, next,
                ))),
            };
            if tx.send(frame).await.is_err() {
                return;
            }
        }
    });

    let mut stream = client
        .extract(ReceiverStream::new(rx))
        .await
        .expect("extract")
        .into_inner();

    let mut matches = 0;
    while let Some(response) = stream.message().await.expect("stream") {
        match response.event {
            Some(pb::extract_response::Event::Element(_)) => matches += 1,
            Some(pb::extract_response::Event::Error(err)) => {
                panic!("the server reported an error: {err:?}")
            }
            _ => {}
        }
    }
    matches
}

/// Stream a document that is a single `<p>` holding `mib` mebibytes of text,
/// with a rule capturing that text, and return how many text bytes came back
/// and the event that ended the run.
///
/// Generated and fed a chunk at a time, like [`stream_document`], so the
/// client never holds the node either.
async fn stream_text_node(
    client: &LolHtmlServiceClient<Channel>,
    mib: usize,
    raw_text_chunks: bool,
) -> (usize, Option<pb::extract_response::Event>) {
    let mut client = client.clone();
    let (tx, rx) = mpsc::channel(4);

    let options = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "p".to_owned(),
            selector: "p".to_owned(),
            captures: vec![pb::Capture::Text as i32],
        }],
        raw_text_chunks,
        limits: Some(pb::MemoryLimits {
            max_bytes: MAX_PARSER_BYTES,
            ..Default::default()
        }),
        ..Default::default()
    };

    tokio::spawn(async move {
        let frame = |bytes: Vec<u8>| pb::ExtractRequest {
            frame: Some(pb::extract_request::Frame::Chunk(bytes)),
        };
        let opened = tx
            .send(pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Options(options)),
            })
            .await;
        if opened.is_err() || tx.send(frame(b"<p>".to_vec())).await.is_err() {
            return;
        }
        let text: Vec<u8> = b"streaming text with no markup in it at all "
            .iter()
            .copied()
            .cycle()
            .take(CHUNK)
            .collect();
        for _ in 0..(mib * 1024 * 1024) / CHUNK {
            // A run that ended early stops reading, and the send fails.
            if tx.send(frame(text.clone())).await.is_err() {
                return;
            }
        }
        let _ = tx.send(frame(b"</p>".to_vec())).await;
    });

    let mut stream = client
        .extract(ReceiverStream::new(rx))
        .await
        .expect("extract")
        .into_inner();

    let mut text_bytes = 0;
    let mut last = None;
    while let Some(response) = stream.message().await.expect("stream") {
        if let Some(pb::extract_response::Event::Text(node)) = &response.event {
            text_bytes += node.text.len();
        }
        last = response.event;
    }
    (text_bytes, last)
}

/// One text node as large as the document must not cost its size either.
///
/// lol-html streams text out in pieces and never holds a whole node, so the
/// server's reassembly is the one place a node is held, and the call's memory
/// limit caps it. Fetched as raw fragments, which are never held, a 128 MiB
/// node streams through without moving the peak a 64 MiB one set.
/// Reassembled, the same node outgrows the 1 MiB limit and the run ends
/// in-band as soon as it does, rather than holding 128 MiB and then a decoded
/// copy of it.
#[tokio::test]
async fn one_enormous_text_node_does_not_grow_the_server() {
    let _alone = ONE_AT_A_TIME.lock().await;
    // Each raw fragment is a whole upload chunk of text, so the default 8 MiB
    // outbound buffer holds only a few dozen and how full it happens to run
    // swamps the measurement. A 1 MiB buffer is full from the first chunks,
    // which leaves the node itself as the only thing that could move the peak.
    let (server, client) =
        start_server_process(&[("GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES", "1048576")]).await;
    let pid = server.0.id();

    // As in the test above, the first run is warm-up, and is compared
    // against rather than asserted on.
    let (text, end) = stream_text_node(&client, 64, true).await;
    assert_eq!(text, 64 * 1024 * 1024, "every raw fragment should arrive");
    assert!(matches!(
        end,
        Some(pb::extract_response::Event::Finished(_))
    ));
    let warm = peak_rss(pid);

    let (text, end) = stream_text_node(&client, 128, true).await;
    assert_eq!(text, 128 * 1024 * 1024, "every raw fragment should arrive");
    assert!(matches!(
        end,
        Some(pb::extract_response::Event::Finished(_))
    ));
    let raw = peak_rss(pid);

    let (text, end) = stream_text_node(&client, 128, false).await;
    assert_eq!(text, 0, "no part of the node is passed off as all of it");
    match end {
        Some(pb::extract_response::Event::Error(err)) => assert_eq!(
            err.code,
            pb::ParseErrorCode::MemoryLimitExceeded as i32,
            "{}",
            err.message
        ),
        other => panic!("expected an in-band memory-limit error, got {other:?}"),
    }
    let reassembled = peak_rss(pid);

    let report = format!(
        "warm {} MiB; 128 MiB raw -> {} MiB; 128 MiB reassembled -> {} MiB peak RSS",
        warm / 1024 / 1024,
        raw / 1024 / 1024,
        reassembled / 1024 / 1024
    );
    println!("  {report}");
    let growth = reassembled.saturating_sub(warm);
    assert!(
        growth < ALLOWED_TEXT_NODE_GROWTH_BYTES,
        "peak RSS grew by {} MiB over a single 128 MiB text node: {report}",
        growth / 1024 / 1024,
    );
}

/// Stream a `mib` MiB document while sampling the server's resident set, note
/// the run in `runs` and return the samples.
async fn measure(
    client: &LolHtmlServiceClient<Channel>,
    pid: u32,
    mib: usize,
    runs: &mut Vec<String>,
) -> Vec<u64> {
    let streaming = Arc::new(AtomicBool::new(true));
    let sampler = {
        let streaming = Arc::clone(&streaming);
        std::thread::spawn(move || {
            let mut samples = Vec::new();
            while streaming.load(Ordering::Relaxed) {
                samples.push(rss(pid));
                std::thread::sleep(SAMPLE_INTERVAL);
            }
            samples
        })
    };
    let matches = stream_document(client, mib).await;
    streaming.store(false, Ordering::Relaxed);
    let samples = sampler.join().expect("the sampler thread");
    assert!(matches > 0, "{mib} MiB produced no matches");
    // Where in the run the largest sample fell, so a burst in the report can
    // be placed against the upload rather than guessed at.
    let (at, max) = samples
        .iter()
        .copied()
        .enumerate()
        .max_by_key(|&(_, rss)| rss)
        .unwrap_or_default();
    runs.push(format!(
        "{mib} MiB -> median {} MiB, max {} MiB at {}% RSS ({matches} matches)",
        median(&samples) / 1024 / 1024,
        max / 1024 / 1024,
        at * 100 / samples.len().max(1),
    ));
    samples
}

/// The median of some samples.
fn median(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted
        .get(sorted.len() / 2)
        .copied()
        .expect("at least one sample")
}

/// Peak server RSS must not track document size.
///
/// Successively larger documents go through one process. If anything held a
/// document, or accumulated per-element state, the peak would climb by roughly
/// the difference between them. It does not, and that is the whole argument for
/// a streaming contract with no handles in it.
#[tokio::test]
async fn peak_server_memory_does_not_track_document_size() {
    let _alone = ONE_AT_A_TIME.lock().await;
    let buffer = BUFFER_BYTES.to_string();
    let (server, client) = start_server_process(&[
        ("GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES", &buffer),
        ("GRPC_LOL_HTML_WINDOW_BYTES", &buffer),
        ("GRPC_LOL_HTML_WORKERS", WORKERS),
    ])
    .await;
    let pid = server.0.id();

    let idle = rss(pid);
    let mut runs = Vec::new();

    measure(&client, pid, COLD_MIB, &mut runs).await;
    for _ in 0..WARM_UPS {
        measure(&client, pid, WARM_UP_MIB, &mut runs).await;
    }

    runs.push("measuring".to_owned());
    let mut small = Vec::new();
    let mut large = Vec::new();
    let mut rounds = Vec::new();
    for _ in 0..ROUNDS {
        let mut round = Vec::new();
        for _ in 0..SMALL_DOCUMENTS / 2 {
            round.extend(measure(&client, pid, WARM_UP_MIB, &mut runs).await);
        }
        let big = measure(&client, pid, LARGE_MIB, &mut runs).await;
        for _ in 0..SMALL_DOCUMENTS / 2 {
            round.extend(measure(&client, pid, WARM_UP_MIB, &mut runs).await);
        }
        small.extend_from_slice(&round);
        large.extend_from_slice(&big);
        round.extend(big);
        rounds.push(median(&round));
    }
    let (small, large) = (median(&small), median(&large));

    let report = format!(
        "idle {} MiB; {}; median {} MiB over {} x {WARM_UP_MIB} MiB, \
         {} MiB over {ROUNDS} x {LARGE_MIB} MiB; by round {:?} MiB",
        idle / 1024 / 1024,
        runs.join("; "),
        small / 1024 / 1024,
        ROUNDS * SMALL_DOCUMENTS,
        large / 1024 / 1024,
        rounds
            .iter()
            .map(|rss| rss / 1024 / 1024)
            .collect::<Vec<_>>(),
    );
    println!("  {report}");
    let growth = large.saturating_sub(small);
    assert!(
        growth < ALLOWED_GROWTH_BYTES,
        "median RSS was {} MiB higher while {LARGE_MIB} MiB documents streamed than \
         while the same bytes streamed as {WARM_UP_MIB} MiB ones, which is what retaining \
         the document would look like: {report}",
        growth / 1024 / 1024,
    );

    // The interleaving puts small and large documents through each round
    // alike, so something held from one call to the next would raise both
    // sides together and pass the comparison above. It shows instead as each
    // round sitting higher than the one before.
    let climb = rounds.last().unwrap().saturating_sub(rounds[0]);
    assert!(
        climb < ALLOWED_CLIMB_BYTES,
        "median RSS climbed {} MiB from the first round of identical documents to the \
         last, which is what holding something between calls would look like: {report}",
        climb / 1024 / 1024,
    );
}
