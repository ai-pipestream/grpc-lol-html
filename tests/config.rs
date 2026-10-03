// SPDX-License-Identifier: Apache-2.0

//! Startup configuration, checked against the shipped binary: a setting the
//! server cannot use stops it before it listens, rather than being replaced
//! by the default without a word.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Run the binary with `name` set to `value`, returning its stderr once it
/// exits, or `None` if it is still running after a few seconds.
fn startup_error(name: &str, value: &str) -> Option<(bool, String)> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_grpc-lol-html"))
        .env("GRPC_LOL_HTML_ADDR", "127.0.0.1:0")
        .env(name, value)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the server binary");

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if child.try_wait().expect("poll the server").is_some() {
            let output = child.wait_with_output().expect("collect the output");
            return Some((
                output.status.success(),
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

/// Zero, a typo or a window HTTP/2 cannot carry stops the server at startup,
/// naming the setting, instead of serving with the default.
#[test]
fn an_unusable_setting_stops_the_server_at_startup() {
    for (name, value) in [
        ("GRPC_LOL_HTML_SEND_TIMEOUT_MS", "0"),
        ("GRPC_LOL_HTML_IDLE_TIMEOUT_MS", "60s"),
        ("GRPC_LOL_HTML_MAX_MEMORY_BYTES", "0"),
        ("GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES", "-1"),
        ("GRPC_LOL_HTML_MAX_CONCURRENT_STREAMS", "0"),
        ("GRPC_LOL_HTML_WORKERS", "0"),
        ("GRPC_LOL_HTML_WINDOW_BYTES", "4294967296"),
    ] {
        let (succeeded, stderr) = startup_error(name, value)
            .unwrap_or_else(|| panic!("{name}={value} should stop the server, and it is serving"));
        assert!(!succeeded, "{name}={value} should exit with a failure");
        assert!(
            stderr.contains(name),
            "{name}={value}: the error should name the setting: {stderr}"
        );
    }
}
