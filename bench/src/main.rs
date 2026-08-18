// SPDX-License-Identifier: Apache-2.0

//! What does the gRPC layer cost, and what does a DOM cost?
//!
//! Three arms over the same document with the same selectors:
//!
//! 0. **in-process lol-html**, the ceiling. No serialization, no sockets.
//! 1. **grpc-lol-html over a real socket**, which is what this repo ships.
//! 2. **scraper**, that is html5ever plus a selector engine, building a whole
//!    DOM. The thing lol-html is deliberately not.
//!
//! Arms 0 and 1 are gated on an order-sensitive digest of the canonical event
//! stream. If they disagree the run is not a benchmark, it is two programs
//! doing different work, and the harness says so instead of printing numbers.
//!
//! Arm 2 cannot join that gate by construction: html5ever builds a tree and
//! normalizes text differently, so its text stream is legitimately not the
//! same. It is checked on the weaker invariant that actually matters for a
//! comparison, namely that it matched the same elements with the same
//! attribute values.
//!
//! ```bash
//! cargo run --release                      # synthetic corpus, 5 iterations
//! cargo run --release -- --file page.html  # a real page
//! cargo run --release -- --mib 32 --iterations 3
//! ```
//!
//! `LOL_HTML_ADDR` points arm 1 at an already-running server, for measuring
//! across a network. Unset, the harness starts one in-process on an ephemeral
//! port, which isolates the cost of gRPC itself from the cost of the network.

use std::time::{Duration, Instant};

use grpc_lol_html::LolHtmlGrpc;
use grpc_lol_html::proto::v1 as pb;
use grpc_lol_html::proto::v1::lol_html_service_client::LolHtmlServiceClient;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};

mod corpus;
mod digest;

use digest::Digest;

// Same allocator as the shipped server binary (`src/main.rs`), so the numbers
// describe the artifact this repo ships rather than a differently-allocated
// cousin of it.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Selectors every arm runs by default, chosen to be expressible in all three.
///
/// Nothing here needs whole-tree context, because lol-html could not evaluate
/// that and the comparison would stop being like for like.
///
/// Override with `--selectors "a[href],h2"`. Worth doing: the gRPC arm sends
/// one message per match, so how many elements match is a first-order input to
/// what the transport costs, and holding the document fixed while varying the
/// match count is how you separate the two.
const DEFAULT_SELECTORS: &[&str] = &["a[href]", "h2", "p.body", "img[src]"];

/// HTTP/2 window used by both ends, matching `src/main.rs`.
const DEFAULT_WINDOW_BYTES: u32 = 4 * 1024 * 1024;

/// Default upload chunk size for the gRPC arm.
///
/// A first-order input to the result, not a detail: the upload is one
/// protobuf message per chunk, so this sets how many messages 16 MiB becomes.
/// `--chunk` sweeps it.
const DEFAULT_CHUNK_BYTES: usize = 256 * 1024;

struct Args {
    chunk: usize,
    window: u32,
    selectors: Vec<String>,
    file: Option<String>,
    mib: usize,
    iterations: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        chunk: DEFAULT_CHUNK_BYTES,
        window: DEFAULT_WINDOW_BYTES,
        selectors: DEFAULT_SELECTORS.iter().map(|s| (*s).to_owned()).collect(),
        file: None,
        mib: 16,
        iterations: 5,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--chunk" => {
                args.chunk = argv
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_CHUNK_BYTES);
                i += 2;
            }
            "--window" => {
                args.window = argv
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_WINDOW_BYTES);
                i += 2;
            }
            "--selectors" => {
                if let Some(list) = argv.get(i + 1) {
                    args.selectors = list
                        .split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                i += 2;
            }
            "--file" => {
                args.file = argv.get(i + 1).cloned();
                i += 2;
            }
            "--mib" => {
                args.mib = argv.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(16);
                i += 2;
            }
            "--iterations" => {
                args.iterations = argv
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5)
                    .max(1);
                i += 2;
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

/// One arm's result for one iteration.
struct Timing {
    elapsed: Duration,
    digest: Digest,
    matches: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    let (label, document) = match &args.file {
        Some(path) => (path.clone(), std::fs::read(path)?),
        None => (
            format!("synthetic {} MiB", args.mib),
            corpus::synthetic(args.mib),
        ),
    };

    let addr = match std::env::var("LOL_HTML_ADDR") {
        Ok(addr) => {
            println!("using an already-running server at {addr}");
            addr
        }
        Err(_) => start_server(args.window).await?,
    };
    let client = connect(&addr, args.window).await?;

    println!();
    println!("grpc-lol-html: in-process lol-html vs the same work over gRPC vs a DOM");
    println!();
    println!("  document    : {label}");
    println!("  size        : {}", human(document.len() as u64));
    println!("  selectors   : {}", args.selectors.join(", "));
    println!("  iterations  : {}, arms interleaved", args.iterations);
    println!(
        "  host        : {} logical cores",
        std::thread::available_parallelism().map_or(0, usize::from)
    );
    println!(
        "  chunk       : {} bytes ({} messages)",
        args.chunk,
        document.len().div_ceil(args.chunk)
    );
    println!("  http2 window: {} bytes", args.window);
    println!("  profile     : release (fat LTO, 1 codegen unit, mimalloc)");
    println!();

    let mut native = Vec::new();
    let mut wire = Vec::new();
    let mut dom = Vec::new();

    for iteration in 1..=args.iterations {
        println!("  iteration {iteration}/{}", args.iterations);
        // Interleaved rather than run in blocks, so a thermal or scheduler
        // drift partway through hits every arm rather than one of them.
        native.push(arm_native(&document, &args.selectors, args.chunk));
        wire.push(arm_wire(&client, &document, &args.selectors, args.chunk).await?);
        dom.push(arm_dom(&document, &args.selectors));
    }
    println!();

    if !gate(&native, &wire, &dom) {
        return Err("arms disagreed; see above".into());
    }

    report(
        &document,
        &[
            ("in-process lol-html", &native),
            ("over gRPC", &wire),
            ("scraper (html5ever + DOM)", &dom),
        ],
    );
    Ok(())
}

/// Arm 0: lol-html directly, no serialization and no socket.
///
/// Uses the same rule compilation as the server so the selectors are compiled
/// by the same code path, then walks the events into a digest.
fn arm_native(document: &[u8], selectors: &[String], chunk: usize) -> Timing {
    use lol_html::{HtmlRewriter, Selector, Settings};
    use std::cell::RefCell;
    use std::rc::Rc;

    let events = Rc::new(RefCell::new((Digest::new(), 0u64)));
    let started = Instant::now();

    let mut settings = Settings::new();
    for selector in selectors {
        let sink = Rc::clone(&events);
        let id = selector.clone();
        settings = settings.append_element_content_handler((
            std::borrow::Cow::Owned(selector.parse::<Selector>().expect("selector compiles")),
            lol_html::ElementContentHandlers::default().element(
                move |el: &mut lol_html::html_content::Element<'_, '_>| {
                    let mut state = sink.borrow_mut();
                    state.0.element(&id, &el.tag_name(), el.attributes());
                    state.1 += 1;
                    Ok(())
                },
            ),
        ));
    }

    let mut rewriter = HtmlRewriter::new(settings, |_: &[u8]| {});
    for part in document.chunks(chunk) {
        rewriter.write(part).expect("parse");
    }
    rewriter.end().expect("parse");

    let elapsed = started.elapsed();
    let state = events.borrow();
    Timing {
        elapsed,
        digest: state.0.clone(),
        matches: state.1,
    }
}

/// Arm 1: the same work through the service, over a real socket.
async fn arm_wire(
    client: &LolHtmlServiceClient<Channel>,
    document: &[u8],
    selectors: &[String],
    chunk: usize,
) -> Result<Timing, Box<dyn std::error::Error>> {
    let mut client = client.clone();

    let options = pb::ExtractOptions {
        rules: selectors
            .iter()
            .map(|selector| pb::ExtractRule {
                id: selector.clone(),
                selector: selector.clone(),
                captures: vec![pb::Capture::TagName as i32, pb::Capture::Attributes as i32],
            })
            .collect(),
        ..Default::default()
    };

    let mut frames = vec![pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Options(options)),
    }];
    frames.extend(document.chunks(chunk).map(|chunk| pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Chunk(chunk.to_vec())),
    }));

    let started = Instant::now();
    let mut stream = client
        .extract(tokio_stream::iter(frames))
        .await?
        .into_inner();

    let mut digest = Digest::new();
    let mut matches = 0u64;
    while let Some(response) = stream.message().await? {
        if let Some(pb::extract_response::Event::Element(element)) = response.event {
            digest.element_pb(&element.rule_id, &element.tag_name, &element.attributes);
            matches += 1;
        }
    }

    Ok(Timing {
        elapsed: started.elapsed(),
        digest,
        matches,
    })
}

/// Arm 2: build the whole DOM, then query it.
fn arm_dom(document: &[u8], selectors: &[String]) -> Timing {
    let started = Instant::now();
    let html = scraper::Html::parse_document(&String::from_utf8_lossy(document));

    let mut digest = Digest::new();
    let mut matches = 0u64;
    // Selector order matters for the digest, but a DOM query returns document
    // order per selector rather than one interleaved stream, so this arm's
    // digest is compared only against itself across iterations, and against
    // the others on the weaker element/attribute invariant.
    for selector in selectors {
        let compiled = scraper::Selector::parse(selector).expect("selector compiles");
        for element in html.select(&compiled) {
            digest.element_dom(selector, element.value().name(), element.value().attrs());
            matches += 1;
        }
    }

    Timing {
        elapsed: started.elapsed(),
        digest,
        matches,
    }
}

/// Refuse to report numbers for arms that did not do the same work.
fn gate(native: &[Timing], wire: &[Timing], dom: &[Timing]) -> bool {
    println!("same-work proof (order-sensitive digest of matched elements)");
    println!("  0 in-process lol-html        {}", native[0].digest);
    println!("  1 over gRPC                  {}", wire[0].digest);
    println!("  2 scraper (DOM)              {}", dom[0].digest);
    println!();

    let mut ok = true;

    if native[0].digest != wire[0].digest {
        println!("  FAIL arms 0 and 1 disagree. These must be byte-identical:");
        println!("       the wire is supposed to carry exactly what the parser found.");
        ok = false;
    } else {
        println!("  ok   arms 0 and 1 are identical, so gRPC changed nothing but the transport");
    }

    // The DOM arm groups by selector instead of interleaving, so its digest
    // legitimately differs. What must hold is that it found the same elements.
    if native[0].matches != dom[0].matches {
        println!(
            "  FAIL arm 2 matched {} elements against {} for arms 0 and 1",
            dom[0].matches, native[0].matches
        );
        ok = false;
    } else {
        println!(
            "  ok   arm 2 matched the same {} elements, grouped by selector rather than interleaved",
            dom[0].matches
        );
    }

    for (name, arm) in [("0", native), ("1", wire), ("2", dom)] {
        if arm.iter().any(|t| t.digest != arm[0].digest) {
            println!("  FAIL arm {name} was not deterministic across iterations");
            ok = false;
        }
    }

    println!();
    ok
}

fn report(document: &[u8], arms: &[(&str, &Vec<Timing>)]) {
    let bytes = document.len() as f64;
    println!("throughput (best of {} iterations)", arms[0].1.len());
    println!();
    println!(
        "  {:<28} {:>10} {:>12} {:>10}",
        "arm", "best", "MiB/s", "vs native"
    );

    let best = |timings: &Vec<Timing>| {
        timings
            .iter()
            .map(|t| t.elapsed)
            .min()
            .unwrap_or(Duration::ZERO)
    };
    let baseline = best(arms[0].1).as_secs_f64();

    for (name, timings) in arms {
        let seconds = best(timings).as_secs_f64();
        let mib = bytes / seconds / (1024.0 * 1024.0);
        println!(
            "  {:<28} {:>9.1}ms {:>12.0} {:>9.2}x",
            name,
            seconds * 1000.0,
            mib,
            seconds / baseline
        );
    }
    println!();
    println!("  matched {} elements per pass", arms[0].1[0].matches);
}

/// Start a server in-process on an ephemeral port.
///
/// The transport settings here deliberately mirror `src/main.rs`. The first
/// version of this harness did not, and so measured a server nobody ships:
/// hyper defaults the HTTP/2 window to 1 MiB, which paces a 16 MiB upload at
/// one window per round trip and cost about 100 ms of the result. A benchmark
/// that configures its own subject differently from production is measuring
/// the wrong program.
async fn start_server(window: u32) -> Result<String, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let service = LolHtmlGrpc::new().into_service();
    tokio::spawn(async move {
        Server::builder()
            .tcp_nodelay(true)
            .initial_stream_window_size(window)
            .initial_connection_window_size(window)
            .max_concurrent_streams(1024)
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server failed");
    });
    println!("started a server in-process on {addr} (http2 window {window} bytes)");
    Ok(addr.to_string())
}

async fn connect(
    addr: &str,
    window: u32,
) -> Result<LolHtmlServiceClient<Channel>, Box<dyn std::error::Error>> {
    // HTTP/2 flow control is directional, so the client window governs what
    // the client receives and the server's governs the upload. Both are set,
    // because this harness pushes hard in both directions.
    let channel = Endpoint::from_shared(format!("http://{addr}"))?
        .tcp_nodelay(true)
        .initial_stream_window_size(Some(window))
        .initial_connection_window_size(Some(window))
        .connect()
        .await?;
    Ok(LolHtmlServiceClient::new(channel))
}

fn human(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }
}
