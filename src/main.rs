// SPDX-License-Identifier: Apache-2.0

//! Binary entry point for the lol-html gRPC server.
//!
//! Runtime sizing (all optional environment overrides; every one but the
//! address is a positive whole number, and anything else stops the server at
//! startup rather than falling back to the default):
//! - `GRPC_LOL_HTML_ADDR` — listen address (default `0.0.0.0:50057`).
//! - `GRPC_LOL_HTML_WORKERS` — tokio worker threads (default: CPU count).
//! - `GRPC_LOL_HTML_MAX_CHUNK_BYTES` — largest inbound chunk accepted
//!   (default: 100 MiB). Not a document size limit: a document is any number
//!   of chunks and has no ceiling. This only bounds how much one message may
//!   carry, and with it how long a single uninterruptible parse runs. Lower
//!   it on a host serving many concurrent callers, since an in-flight chunk
//!   is buffered per call.
//! - `GRPC_LOL_HTML_WINDOW_BYTES` — HTTP/2 initial stream and connection
//!   window (default: 4 MiB).
//! - `GRPC_LOL_HTML_IDLE_TIMEOUT_MS` — how long an open `Extract` stream may
//!   go without an inbound chunk carrying document bytes before the server
//!   ends it (default: 60000).
//! - `GRPC_LOL_HTML_UPLOAD_TIMEOUT_MS` — how long an `Extract` stream's whole
//!   upload may take before the server ends it (default: 600000).
//! - `GRPC_LOL_HTML_SEND_TIMEOUT_MS` — how long an open `Extract` stream may
//!   go without its client taking a response while events wait, before the
//!   server ends it with `RESOURCE_EXHAUSTED` (default: 60000).
//! - `GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES` — byte budget for one stream's
//!   events waiting to be sent (default: 8 MiB).
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
//! Parsing runs on tokio's blocking pool, one chunk per task, and a stream
//! holds at most one blocking thread at a time. The pool's default ceiling
//! of 512 threads sits well above the default stream cap, so it is left
//! alone; raise it only alongside a stream cap in the hundreds.

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

/// The largest window HTTP/2 allows.
const MAX_WINDOW_BYTES: u32 = (1 << 31) - 1;

/// Read a positive `usize` environment variable, falling back to `default`
/// when it is not set.
///
/// Every setting read this way is a size, a count or a timeout, and zero is
/// a mistake for each of them: a server that takes no streams or times out
/// at once. A typo quietly replaced by the default is worse than a server
/// that refuses to start and says why, so both are errors.
fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => positive(name, &value),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(value)) => {
            Err(format!("{name}={value:?} is not a positive whole number"))
        }
    }
}

/// Parse the value of setting `name` as a positive whole number.
fn positive(name: &str, value: &str) -> Result<usize, String> {
    match value.parse() {
        Ok(0) | Err(_) => Err(format!("{name}={value:?} is not a positive whole number")),
        Ok(parsed) => Ok(parsed),
    }
}

/// The HTTP/2 window size from setting `name`, which may be no larger than
/// the 2^31 - 1 bytes HTTP/2 allows.
fn window_size(name: &str, bytes: usize) -> Result<u32, String> {
    u32::try_from(bytes)
        .ok()
        .filter(|&window| window <= MAX_WINDOW_BYTES)
        .ok_or_else(|| format!("{name}={bytes} is larger than HTTP/2 allows ({MAX_WINDOW_BYTES})"))
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
    )?;

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
        )?)
        .with_idle_timeout(Duration::from_millis(env_usize(
            "GRPC_LOL_HTML_IDLE_TIMEOUT_MS",
            grpc_lol_html::service::DEFAULT_IDLE_TIMEOUT_MS,
        )? as u64))
        .with_upload_timeout(Duration::from_millis(env_usize(
            "GRPC_LOL_HTML_UPLOAD_TIMEOUT_MS",
            grpc_lol_html::service::DEFAULT_UPLOAD_TIMEOUT_MS,
        )? as u64))
        .with_send_timeout(Duration::from_millis(env_usize(
            "GRPC_LOL_HTML_SEND_TIMEOUT_MS",
            grpc_lol_html::service::DEFAULT_SEND_TIMEOUT_MS,
        )? as u64))
        .with_outbound_buffer_bytes(env_usize(
            "GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES",
            grpc_lol_html::service::DEFAULT_OUTBOUND_BUFFER_BYTES,
        )?)
        .with_max_memory_bytes(env_usize(
            "GRPC_LOL_HTML_MAX_MEMORY_BYTES",
            grpc_lol_html::service::DEFAULT_MEMORY_CEILING_BYTES,
        )?)
        .with_max_concurrent_streams(env_usize(
            "GRPC_LOL_HTML_MAX_CONCURRENT_STREAMS",
            grpc_lol_html::service::DEFAULT_MAX_CONCURRENT_STREAMS,
        )?);
    let service = grpc.into_service();

    let window = window_size(
        "GRPC_LOL_HTML_WINDOW_BYTES",
        env_usize("GRPC_LOL_HTML_WINDOW_BYTES", DEFAULT_WINDOW_BYTES as usize)?,
    )?;

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

#[cfg(test)]
mod tests {
    use super::*;

    /// A setting is a positive whole number, and nothing else is quietly
    /// read as the default.
    #[test]
    fn a_setting_must_be_a_positive_whole_number() {
        assert_eq!(positive("N", "1"), Ok(1));
        assert_eq!(positive("N", "60000"), Ok(60_000));
        for bad in ["0", "", "-1", "1.5", " 5", "5ms", "sixty"] {
            let err = positive("N", bad).expect_err(bad);
            assert!(err.starts_with("N="), "the error names the setting: {err}");
        }
    }

    /// A window must fit HTTP/2's 31 bits rather than wrap or be replaced.
    #[test]
    fn a_window_larger_than_http2_allows_is_refused() {
        assert_eq!(window_size("W", 1 << 20), Ok(1 << 20));
        assert_eq!(window_size("W", (1 << 31) - 1), Ok((1 << 31) - 1));
        assert!(window_size("W", 1 << 31).is_err());
        assert!(window_size("W", usize::MAX).is_err());
    }
}
