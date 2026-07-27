//! Generated protobuf code for the `lolhtml.v1` package.
//!
//! The files under `src/gen` are produced by `buf generate` (see
//! `buf.gen.yaml`; never edit them by hand). They are re-generated from
//! `proto/lolhtml/v1/*.proto`.

/// Messages, enums, client, and server for the `lolhtml.v1` protobuf package.
///
/// Wire-level documentation lives in the `.proto` files (buf enforces
/// comments on every item there); the generated Rust carries it over where
/// prost supports it.
#[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
pub mod v1 {
    // The prost output already ends with `include!("lolhtml.v1.tonic.rs")`,
    // pulling in the client and server modules.
    include!("gen/lolhtml/v1/lolhtml.v1.rs");
}
