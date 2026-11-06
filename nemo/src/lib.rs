//! The logical layer of the Nemo rule engine.
//!
//! This crate implements the Nemo language (parser, rule model, program transformations),
//! reasoning (normalization, planning, semi-naive execution), and tracing,
//! on top of the [`nemo_physical`] database layer and the [`nemo_io`] adapters.
//! It is the crate to depend on to use Nemo as a library.
//!
//! The engine sends the requests of the remote data a program needs through a
//! [client](engine::remote::Client): the HTTP client of the `http` feature by default,
//! or one set with [set_client](engine::Engine::set_client).
//! The `start_*` methods of the [Engine](engine::Engine) send nothing:
//! they return the requests, and [resume](engine::Engine::resume) takes the responses
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
