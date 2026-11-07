//! Resources beyond the program's text: remote imports over HTTP and SPARQL, a load that
//! is suspended for its remote data, RDF and Parquet serializations, and the options and
//! directories that resolve an import's resource.

mod exports;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "parquet")]
mod parquet;
mod resources;
mod suspended;

use nemo::engine::Engine;

use crate::common::parse_into;

/// Loads a program from source into a fresh engine, reasoning included.
fn run(source: &str) -> Engine {
    let mut engine = Engine::new();
    load_into(&mut engine, source);
    engine
}

/// Loads a program from source into the given engine and reasons,
/// remote imports fetched through the engine's client if the build has one.
fn load_into(engine: &mut Engine, source: &str) {
    let program = parse_into(engine, source);
    engine
        .load(program)
        .unwrap_or_else(|report| panic!("{report:?}"));
    engine
        .reason()
        .unwrap_or_else(|report| panic!("{report:?}"));
}
