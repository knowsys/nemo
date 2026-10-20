//! The logical layer of the Nemo rule engine.
//!
//! This crate implements the Nemo language (parser, rule model, program transformations),
//! reasoning (normalization, planning, semi-naive execution), and tracing,
//! on top of the [`nemo_physical`] database layer and the [`nemo_io`] adapters.
//! It is the crate to depend on to use Nemo as a library.
//!
//! The engine does no network IO itself.
//! A method that needs data from the network suspends and lists the requests it needs
//! (see [pending](engine::Engine::pending)).
//! With the `http` feature, the `driver` module performs these requests over HTTP.

pub mod codes;

pub mod diagnostics;
#[cfg(feature = "http")]
pub mod driver;
pub mod engine;
pub mod model;
pub mod parser;
pub mod pipeline;
pub mod telemetry;
pub mod trace;
pub mod values;

#[cfg(test)]
mod testing;
