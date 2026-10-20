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

/// The warm-up document, repeated until the peak stops moving.
///
/// `VmHWM` is a high-water mark and never falls, so the peak a warm process
/// settles at is the reference everything after it is measured against.
/// Warming up takes longer than one small document. mimalloc keeps a heap per
/// thread, and the parse moves between the async workers and the blocking pool
/// from one chunk to the next, so the peak keeps climbing until every thread
/// that will carry the work has held its share of it. On a 4-core machine that
/// took anywhere from 16 MiB to 128 MiB of document, the settled peak scaled
/// with the worker count (about 30, 42 and 52 MiB at one, two and four
/// workers) rather than with the document, and none of it moved again through
/// 512 MiB. Comparing 64 MiB against 16 MiB, as this test once did, compared
/// a settled peak with one still warming up and failed one run in ten or
/// twenty, on any toolchain from 1.97 to 1.99, on differences of up to 17 MiB
/// that were none of the document's doing.
const WARM_UP_MIB: usize = 64;

/// A warm-up run that raises the peak by less than this is a quiet one.
const SETTLED_BYTES: u64 = 1024 * 1024;

/// How many quiet warm-up runs in a row count as settled.
///
/// One is not enough: in forty runs, every one where the large document still
/// added 3 to 6 MiB had stopped after a single quiet warm-up.
const SETTLED_RUNS: usize = 2;

/// How many warm-up runs the peak gets to settle in.
///
/// Repeating one document cannot grow a server that holds nothing between
/// calls, so a peak that is still climbing after this many identical runs is
/// accumulating something across them, and that fails the test on its own.
const MAX_WARM_UPS: usize = 10;

/// The async worker count this test runs the server with.
///
/// The settled peak grows with the worker count, and so does the time it takes
/// to settle, so the default of one per core would make the reference depend on
/// the machine. Four is what the CI runners have and what the numbers above
/// were measured with.
const WORKERS: &str = "4";

/// The outbound buffer and HTTP/2 windows this test runs the server with.
///
/// Both are bounded by configuration rather than by the document, and both
/// fill only while the client falls behind, which a test client sharing one
/// thread with its own uploader does now and then. At the defaults, 8 MiB of
/// outbound buffer and 4 MiB windows, a rare run where they fill together
/// lifts the peak by 8 to 10 MiB at whatever point it happens, settled or
/// not, so the bound itself would be measured rather than the document. At
/// 1 MiB each, with the settling above, the most the large document added in
/// seventy runs was 6 MiB and none failed, which leaves the allowance below to
/// mean what it says.
const BUFFER_BYTES: &str = "1048576";

/// The document measured against the settled peak, four times the warm-up.
const LARGE_MIB: usize = 256;

/// How much peak RSS the large document may add over the settled peak.
///
/// It has to separate "flat" from "proportional". A server that retained the
/// document would need the full 192 MiB difference between the warm-up and the
/// large document, so this sits far below that and far above the megabyte or
/// two that allocator behaviour moves between runs.
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

/// Peak resident set size of a process, in bytes.
fn peak_rss(pid: u32) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("read /proc status");
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|kb| kb.parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .expect("VmHWM in /proc status")
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

/// Stream a `mib` MiB document, note the peak it leaves in `runs` and return
/// it.
async fn measure(
    client: &LolHtmlServiceClient<Channel>,
    pid: u32,
    mib: usize,
    runs: &mut Vec<String>,
) -> u64 {
    let matches = stream_document(client, mib).await;
    assert!(matches > 0, "{mib} MiB produced no matches");
    let peak = peak_rss(pid);
    runs.push(format!(
        "{mib} MiB -> {} MiB peak RSS ({matches} matches)",
        peak / 1024 / 1024
    ));
    peak
}

/// Peak server RSS must not track document size.
///
/// Successively larger documents go through one process. If anything held a
/// document, or accumulated per-element state, the peak would climb by roughly
/// the difference between them. It does not, and that is the whole argument for
/// a streaming contract with no handles in it.
#[tokio::test]
async fn peak_server_memory_does_not_track_document_size() {
    let (server, client) = start_server_process(&[
        ("GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES", BUFFER_BYTES),
        ("GRPC_LOL_HTML_WINDOW_BYTES", BUFFER_BYTES),
        ("GRPC_LOL_HTML_WORKERS", WORKERS),
    ])
    .await;
    let pid = server.0.id();

    let idle = peak_rss(pid);
    let mut runs = Vec::new();

    let mut settled = measure(&client, pid, COLD_MIB, &mut runs).await;
    let mut warm_ups = 0;
    let mut quiet = 0;
    loop {
        let peak = measure(&client, pid, WARM_UP_MIB, &mut runs).await;
        warm_ups += 1;
        quiet = if peak - settled < SETTLED_BYTES {
            quiet + 1
        } else {
            0
        };
        settled = peak;
        if quiet == SETTLED_RUNS {
            break;
        }
        assert!(
            warm_ups < MAX_WARM_UPS,
            "peak RSS had not settled after {MAX_WARM_UPS} identical {WARM_UP_MIB} MiB \
             documents, which is what holding something between calls would look like: \
             idle {} MiB; {}",
            idle / 1024 / 1024,
            runs.join("; "),
        );
    }
    let large = measure(&client, pid, LARGE_MIB, &mut runs).await;

    let report = format!("idle {} MiB; {}", idle / 1024 / 1024, runs.join("; "));
    println!("  {report}");
    let growth = large.saturating_sub(settled);
    assert!(
        growth < ALLOWED_GROWTH_BYTES,
        "peak RSS grew by {} MiB while the document grew by {} MiB, which is what \
         retaining the document would look like: {report}",
        growth / 1024 / 1024,
        LARGE_MIB - WARM_UP_MIB,
    );
}
