#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Start/stop wrapper for the lol-html gRPC server.
#
#   service.sh start     build (release, --locked) and run in the background
#   service.sh stop      graceful SIGTERM, SIGKILL after 10s
#   service.sh restart   stop then start
#   service.sh status    pid + listen check
#
# Runtime env overrides (GRPC_LOL_HTML_ADDR, _WORKERS, _MAX_CHUNK_BYTES,
# _WINDOW_BYTES — see src/main.rs) are passed through to the server.
# Logs land in logs/grpc-lol-html.log; pid in run/grpc-lol-html.pid.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN="$SCRIPT_DIR/target/release/grpc-lol-html"
PID_FILE="$SCRIPT_DIR/run/grpc-lol-html.pid"
LOG_FILE="$SCRIPT_DIR/logs/grpc-lol-html.log"
ADDR="${GRPC_LOL_HTML_ADDR:-0.0.0.0:50051}"

running_pid() {
  # Prints the live pid, or nothing. A stale pid file is removed.
  [ -f "$PID_FILE" ] || return 1
  local pid
  pid="$(cat "$PID_FILE")"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "$pid"
  else
    rm -f "$PID_FILE"
    return 1
  fi
}

start() {
  if pid="$(running_pid)"; then
    echo "grpc-lol-html already running (pid $pid)"
    return 0
  fi
  command -v cargo >/dev/null 2>&1 || {
    echo "error: cargo is required to build grpc-lol-html" >&2
    exit 1
  }
  mkdir -p "$SCRIPT_DIR/run" "$SCRIPT_DIR/logs"
  echo "Building grpc-lol-html in release mode..."
  (cd "$SCRIPT_DIR" && cargo build --release --locked)
  echo "Starting grpc-lol-html on $ADDR ..."
  nohup "$BIN" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  sleep 1
  if pid="$(running_pid)"; then
    echo "grpc-lol-html started (pid $pid, log $LOG_FILE)"
  else
    echo "error: grpc-lol-html exited on startup; last log lines:" >&2
    tail -5 "$LOG_FILE" >&2
    exit 1
  fi
}

stop() {
  if ! pid="$(running_pid)"; then
    echo "grpc-lol-html is not running"
    return 0
  fi
  echo "Stopping grpc-lol-html (pid $pid) ..."
  kill "$pid"
  for _ in $(seq 1 10); do
    kill -0 "$pid" 2>/dev/null || { rm -f "$PID_FILE"; echo "stopped"; return 0; }
    sleep 1
  done
  echo "still up after 10s, sending SIGKILL"
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
}

status() {
  if pid="$(running_pid)"; then
    echo "grpc-lol-html running (pid $pid, addr $ADDR)"
  else
    echo "grpc-lol-html not running"
    return 1
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  *) echo "usage: $0 {start|stop|restart|status}" >&2; exit 2 ;;
esac
