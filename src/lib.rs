// SPDX-License-Identifier: Apache-2.0

//! A gRPC server that runs Cloudflare's `lol_html` streaming rewriter over
//! HTML the caller uploads, and reports CSS-selector matches as they happen.
//!
//! Design rules:
//! - **Nothing is retained.** `lol_html` is a forward-only transducer: bytes
//!   in, events out, no document to go back to. There is no handle store and
//!   no server-side copy of the input, so memory stays flat whatever the
//!   document size. This is the opposite of the sibling `grpc-calamine`
//!   service, and it is the library's nature rather than a simplification.
//! - **Backpressure survives the sync/async boundary.** `lol_html` handlers
//!   are synchronous closures that cannot await, so they queue into an
//!   unbounded [`tokio::sync::mpsc`] whose `send` never blocks, and
//!   [`service`] drains that queue after every chunk, awaiting each forward
//!   onto the bounded outbound channel where backpressure actually lives. A
//!   slow client therefore slows the parser instead of growing a buffer. See
//!   the [`service`] module documentation.
//! - **One-to-one contract.** The protobuf model in `proto/lolhtml/v1`
//!   mirrors lol-html's public types, including the parts that are easy to
//!   overlook: every `TextType`, every `Namespace`, per-attribute source
//!   spans, and both error taxonomies.

pub mod convert;
pub mod errors;
pub mod proto;
pub mod rules;
pub mod service;

pub use service::LolHtmlGrpc;
