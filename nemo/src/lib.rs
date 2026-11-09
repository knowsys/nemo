//! The logical layer of the Nemo rule engine.
//!
//! This crate implements the Nemo rule language (its parser, the program model,
//! and the program transformations), reasoning (normalization, planning and semi-naive
//! evaluation), and tracing.
//! It builds on the [`nemo_physical`] database layer and the [`nemo_io`] format adapters,
//! and it is the crate to depend on to use Nemo as a library.
//!
//! The engine sends its requests for remote data through a [client](engine::remote::Client).
//! A new engine uses the HTTP client of the `http` feature, if the crate is built with it,
//! and [set_client](engine::Engine::set_client) replaces the client.
//! The `start_*` methods of the [Engine](engine::Engine) send nothing.
//! They return the pending requests, and [resume](engine::Engine::resume) accepts the responses
//! (see [engine::remote]).

pub mod codes;

pub mod diagnostics;
pub mod engine;
pub mod model;
pub mod parser;
pub mod pipeline;
pub mod telemetry;
pub mod trace;
pub mod values;

#[cfg(test)]
mod testing;
