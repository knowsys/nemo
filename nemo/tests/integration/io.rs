//! Resources beyond the program's text: remote imports over HTTP and SPARQL, the suspension
//! of a load for its remote data, RDF and Parquet serializations, and the options and
//! directories that resolve an import's resource.

mod exports;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "parquet")]
mod parquet;
mod resources;
mod suspension;

use nemo::engine::Engine;

use crate::common::parse_into;

/// Loads a program from source into a fresh engine, reasoning included.
fn run(source: &str) -> Engine {
    let mut engine = Engine::new();
    load_into(&mut engine, source);
    engine
}

/// Loads a program from source into the given engine and reasons,
/// through the driver if the build has it, so that remote imports fetch.
fn load_into(engine: &mut Engine, source: &str) {
    let program = parse_into(engine, source);
    #[cfg(feature = "http")]
    {
        let driver = nemo::driver::HttpDriver::new();
        driver
            .drive(engine, |engine| engine.load(program))
            .unwrap_or_else(|report| panic!("{report:?}"));
        driver
            .drive(engine, Engine::reason)
            .unwrap_or_else(|report| panic!("{report:?}"));
    }
    #[cfg(not(feature = "http"))]
    {
        engine
            .load(program)
            .unwrap_or_else(|report| panic!("{report:?}"));
        engine
            .reason()
            .unwrap_or_else(|report| panic!("{report:?}"));
    }
}
