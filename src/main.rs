// SPDX-License-Identifier: Apache-2.0

//! Binary entry point for the lol-html gRPC server.
//!
//! Runtime sizing (all optional environment overrides):
//! - `GRPC_LOL_HTML_ADDR` — listen address (default `0.0.0.0:50057`).
//! - `GRPC_LOL_HTML_WORKERS` — tokio worker threads (default: CPU count).
//! - `GRPC_LOL_HTML_MAX_CHUNK_BYTES` — largest inbound chunk accepted
//!   (default: 100 MiB). Not a document size limit: a document is any number
//!   of chunks and has no ceiling. This only bounds how much one message may
//!   carry, and with it how long a single uninterruptible parse can occupy an
//!   async worker. Lower it on a host serving many concurrent callers, since
//!   an in-flight chunk is buffered per call.
//! - `GRPC_LOL_HTML_WINDOW_BYTES` — HTTP/2 initial stream and connection
//!   window (default: 4 MiB).
//! - `GRPC_LOL_HTML_IDLE_TIMEOUT_MS` — how long an open `Extract` stream may
//!   go without an inbound frame before the server ends it
//!   (default: 60000).
//! - `GRPC_LOL_HTML_MAX_MEMORY_BYTES` — ceiling on the memory limit a call
//!   may ask for in `MemoryLimits.max_bytes`; larger requests get the
//!   ceiling (default: 64 MiB).
//! - `GRPC_LOL_HTML_MAX_CONCURRENT_STREAMS` — cap on simultaneously open
//!   `Extract` streams; calls past the cap fail fast with
//!   `RESOURCE_EXHAUSTED` (default: 64).
//!
//! Logging goes through `tracing`: `RUST_LOG` selects the filter
//! (default `info`).
//!
//! There is no blocking-pool setting, unlike the sibling grpc-calamine
//! server: parsing happens one bounded chunk at a time on the async task
//! itself, so there is no pool to size.

use std::time::Duration;

use tonic::transport::Server;

use grpc_lol_html::LolHtmlGrpc;
use grpc_lol_html::proto::v1 as pb;

/// The per-event allocation rate here is high — several heap objects per
/// matched element, none of them retained — so the global allocator is on the
/// hot path. mimalloc's thread-local free lists serve that shape of load
/// better than glibc malloc's arena locking.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Default listen address when `GRPC_LOL_HTML_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50057";

/// Serialized `FileDescriptorSet` for `proto/lolhtml/v1`, backing gRPC server
/// reflection.
///
/// Codegen here runs through `buf generate`, not a build.rs, so there is no
/// build-time descriptor set to reuse; this is the same `buf build` output
/// checked in next to the generated Rust. Regenerate after any proto change:
///
/// ```sh
/// buf build -o src/gen/file_descriptor_set.binpb
/// ```
const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("gen/file_descriptor_set.binpb");

/// Default HTTP/2 initial window, for both the stream and the connection.
///
/// hyper defaults to 1 MiB. Documents here are pages rather than the hundreds
/// of megabytes a workbook can reach, so this is sized to keep an upload off
/// the one-window-per-round-trip floor rather than to absorb a bulk transfer.
const DEFAULT_WINDOW_BYTES: u32 = 4 * 1024 * 1024;

/// Read a `usize` environment variable, falling back to `default`.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `RUST_LOG` drives the filter when set; the default is `info`, which is
    // one line per finished stream plus lifecycle events.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let workers = env_usize(
        "GRPC_LOL_HTML_WORKERS",
        std::thread::available_parallelism().map_or(4, usize::from),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(workers)
        .build()?;

    runtime.block_on(serve())
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("GRPC_LOL_HTML_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;

    let grpc = LolHtmlGrpc::new()
        .with_max_chunk_bytes(env_usize(
            "GRPC_LOL_HTML_MAX_CHUNK_BYTES",
            grpc_lol_html::service::DEFAULT_MAX_CHUNK_BYTES,
        ))
        .with_idle_timeout(Duration::from_millis(env_usize(
            "GRPC_LOL_HTML_IDLE_TIMEOUT_MS",
            grpc_lol_html::service::DEFAULT_IDLE_TIMEOUT_MS,
        ) as u64))
        .with_max_memory_bytes(env_usize(
            "GRPC_LOL_HTML_MAX_MEMORY_BYTES",
            grpc_lol_html::service::DEFAULT_MEMORY_CEILING_BYTES,
        ))
        .with_max_concurrent_streams(env_usize(
            "GRPC_LOL_HTML_MAX_CONCURRENT_STREAMS",
            grpc_lol_html::service::DEFAULT_MAX_CONCURRENT_STREAMS,
        ));
    let service = grpc.into_service();

    let window = u32::try_from(env_usize(
        "GRPC_LOL_HTML_WINDOW_BYTES",
        DEFAULT_WINDOW_BYTES as usize,
    ))
    .unwrap_or(DEFAULT_WINDOW_BYTES);

    // Reflection (v1) so clients like grpcurl can discover the contract from a
    // live server instead of shipping the .proto files around.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    // The standard health service, so an orchestrator can probe the process
    // over gRPC instead of guessing at the port.
    let (health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<pb::lol_html_service_server::LolHtmlServiceServer<LolHtmlGrpc>>()
        .await;

    tracing::info!(%addr, window, "grpc-lol-html listening");
    Server::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .max_concurrent_streams(1024)
        .add_service(service)
        .add_service(reflection)
        .add_service(health)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    tracing::info!("grpc-lol-html shut down");
    Ok(())
}

/// Resolve when the process receives SIGINT (Ctrl-C) or SIGTERM, so open
/// streams can drain instead of being cut mid-document.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm.recv() => {}
    }
}
