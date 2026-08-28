//! popfusion — SQL over remote Parquet, peer-to-peer.
//!
//! Library surface exists so the e2e integration tests can drive the same
//! code paths as the binary. See README.md for the full spec.

#![deny(unsafe_code)]

pub mod client;
pub mod proto;
pub mod query;
pub mod server;
