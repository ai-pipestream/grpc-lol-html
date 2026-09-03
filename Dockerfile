# syntax=docker/dockerfile:1
# SPDX-License-Identifier: Apache-2.0

# ---------------------------------------------------------------------------
# Build stage.
#
# The tests run here, against the release profile the image ships, before the
# binary is built, so a red suite fails the image rather than shipping. That
# is the whole reason this is a multi-stage build and not a `COPY` of
# something built on a laptop: the artifact and the evidence for it come out
# of the same command.
#
# No protoc and no buf. Code generation happens at development time (see
# buf.gen.yaml) and the generated Rust is checked in, so the image build needs
# a Rust toolchain and nothing else.
# ---------------------------------------------------------------------------
FROM dhi.io/rust:1 AS builder

# The hardened toolchain image runs as a nonroot user; the build needs to
# write only under /src and the cargo home, so give it a writable workspace.
USER root
WORKDIR /src
COPY . .

# `--locked` makes the build reproducible and fails loudly if Cargo.lock is out
# of date, rather than quietly resolving to something never tested.
RUN cargo test --release --locked
RUN cargo build --release --locked

# ---------------------------------------------------------------------------
# Runtime stage.
#
# Docker Hardened Images debian-base: glibc and libgcc, no package manager,
# pulls from the docker.io ecosystem (dhi.io) with signed provenance, and
# runs as uid 65532 out of the box. There is no hot path that needs a shell,
# and this service never writes to disk, so the container can and should run
# with `--read-only`.
#
#   docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges \
#     -p 50057:50057 grpc-lol-html
#
# Health checking is the orchestrator's job over
# gRPC (`grpc.health.v1.Health/Check`, which this server registers) rather than
# a Dockerfile HEALTHCHECK, because there is no shell here to run one with.
# ---------------------------------------------------------------------------
FROM dhi.io/debian-base:trixie-debian13

COPY --from=builder /src/target/release/grpc-lol-html /usr/local/bin/grpc-lol-html

ENV GRPC_LOL_HTML_ADDR=0.0.0.0:50057
EXPOSE 50057
USER nonroot
ENTRYPOINT ["/usr/local/bin/grpc-lol-html"]
