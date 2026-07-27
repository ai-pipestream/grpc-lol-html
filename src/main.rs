// SPDX-License-Identifier: Apache-2.0

//! Binary entry point for the lol-html gRPC server.
//!
//! Runtime sizing (all optional environment overrides):
//! - `GRPC_LOL_HTML_ADDR` — listen address (default `0.0.0.0:50051`).
//! - `GRPC_LOL_HTML_WORKERS` — tokio worker threads (default: CPU count).
//! - `GRPC_LOL_HTML_MAX_CHUNK_BYTES` — largest inbound chunk accepted
//!   (default: 100 MiB). Not a document size limit: a document is any number
//!   of chunks and has no ceiling. This only bounds how much one message may
//!   carry, and with it how long a single uninterruptible parse can occupy an
//!   async worker. Lower it on a host serving many concurrent callers, since
//!   an in-flight chunk is buffered per call.
//! - `GRPC_LOL_HTML_WINDOW_BYTES` — HTTP/2 initial stream and connection
//!   window (default: 4 MiB).
//!
//! There is no blocking-pool setting, unlike the sibling grpc-calamine
//! server: parsing happens one bounded chunk at a time on the async task
//! itself, so there is no pool to size.

use std::time::Duration;

use tonic::transport::Server;

use grpc_lol_html::LolHtmlGrpc;

/// Default listen address when `GRPC_LOL_HTML_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50051";

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

    let mut grpc = LolHtmlGrpc::new();
    if let Some(max) = std::env::var("GRPC_LOL_HTML_MAX_CHUNK_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        grpc = grpc.with_max_chunk_bytes(max);
    }
    let service = grpc.into_service();

    let window = u32::try_from(env_usize(
        "GRPC_LOL_HTML_WINDOW_BYTES",
        DEFAULT_WINDOW_BYTES as usize,
    ))
    .unwrap_or(DEFAULT_WINDOW_BYTES);

    eprintln!("grpc-lol-html listening on {addr} (http2 window {window} bytes)");
    Server::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .max_concurrent_streams(1024)
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    eprintln!("grpc-lol-html shut down");
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
