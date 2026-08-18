// SPDX-License-Identifier: Apache-2.0

//! The claim this service is built on, measured rather than asserted.
//!
//! "Server memory stays flat regardless of document size" is the reason there
//! is no handle store, no `document_id` and no `CloseDocument` in the contract.
//! Every other test here checks what the server *says*; this one checks what it
//! *costs*, by watching the real server binary's peak resident set while the
//! document it is fed grows by two orders of magnitude.
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

/// Document sizes to run, smallest first.
///
/// `VmHWM` is a high-water mark and never falls, so sizes ascend and any growth
/// is attributable to the run that caused it.
///
/// The first entry is a cold run and is reported but not asserted on. A process
/// that has just started has not yet touched its HTTP/2 windows, its allocator
/// arenas or its per-connection buffers, so the step up from it measures
/// starting up rather than the thing being claimed. Measured across 1, 16, 64,
/// 128 and 256 MiB the curve is 13, 22, 24, 26, 27 MiB: nearly all of the
/// movement is that first step, and the assertion below deliberately excludes
/// it so that it is testing memory against document size and nothing else.
const SIZES_MIB: [usize; 3] = [1, 16, 64];

/// How much peak RSS the larger of the two warm documents may add.
///
/// It has to separate "flat" from "proportional". A server that retained the
/// document would need the full 48 MiB difference between the two warm sizes,
/// so this sits far below that and far above the megabyte or two that allocator
/// behaviour moves between runs.
const ALLOWED_GROWTH_BYTES: u64 = 8 * 1024 * 1024;

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

/// Start the shipped binary on a free port and connect to it.
async fn start_server_process() -> (ServerProcess, LolHtmlServiceClient<Channel>) {
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

/// Peak server RSS must not track document size.
///
/// Successively larger documents go through one process. If anything held a
/// document, or accumulated per-element state, the peak would climb by roughly
/// the difference between them. It does not, and that is the whole argument for
/// a streaming contract with no handles in it.
#[tokio::test]
async fn peak_server_memory_does_not_track_document_size() {
    let (server, client) = start_server_process().await;
    let pid = server.0.id();

    let baseline = peak_rss(pid);
    let mut peaks = Vec::new();

    for mib in SIZES_MIB {
        let matches = stream_document(&client, mib).await;
        assert!(matches > 0, "{mib} MiB produced no matches");
        peaks.push((mib, matches, peak_rss(pid)));
    }

    let report: Vec<String> = peaks
        .iter()
        .map(|(mib, matches, rss)| {
            format!(
                "{mib} MiB -> {} MiB peak RSS ({matches} matches)",
                rss / 1024 / 1024
            )
        })
        .collect();
    println!(
        "  idle {} MiB; {}",
        baseline / 1024 / 1024,
        report.join("; ")
    );

    // The two largest, so the comparison is between two warm runs.
    let (small_mib, _, small_peak) = peaks[peaks.len() - 2];
    let (large_mib, _, large_peak) = *peaks.last().unwrap();
    let growth = large_peak.saturating_sub(small_peak);
    let document_growth = ((large_mib - small_mib) * 1024 * 1024) as u64;

    assert!(
        growth < ALLOWED_GROWTH_BYTES,
        "peak RSS grew by {} MiB while the document grew by {} MiB, which is what \
         retaining the document would look like: {}",
        growth / 1024 / 1024,
        document_growth / 1024 / 1024,
        report.join("; "),
    );
}
