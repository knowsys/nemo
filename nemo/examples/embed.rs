//! Embedding the engine in an application:
//! data in from the host, results out in bounded memory, cancellation, and in-memory exports.
//!
//! `api` tours construction, and this is the *data-flow* side,
//! the shape a server, notebook kernel, or pipeline embeds.
//! Usage: `cargo run -p nemo --example embed`

use std::time::Duration;

use nemo::{
    diagnostics::render::{Renderer, SourceTexts},
    engine::Engine,
    model::{FileId, Program},
    values::{DataValue, TupleValue},
};

fn main() {
    // ── Host data in ─────────────────────────────────────────────────
    //
    // Rows the application computed enter through `import_rows`
    // (typed values, structured ones included), and serialized data it
    // holds (a download, an upload, an embedded asset) through
    // `add_resource`, which serves `@import` in place of the
    // filesystem. Both precede `load`: the rows pass the same
    // declaration checks and seed the same type analysis as facts the
    // program states itself.
    let mut engine = Engine::new();
    engine
        .import_rows(
            "reading",
            (0..50_000).map(|index| {
                vec![
                    DataValue::iri(format!("sensor{}", index % 100)),
                    DataValue::integer(index),
                    DataValue::tuple(TupleValue::labeled(
                        "point",
                        vec![
                            DataValue::integer(index % 360),
                            DataValue::integer(index % 90),
                        ],
                    )),
                ]
            }),
        )
        .expect("rows import before the load");
    engine.add_resource(
        "thresholds.csv",
        "sensor7,40000\nsensor23,10000\n".as_bytes().to_vec(),
    );

    let source = "
        @declare reading(sensor: iri, value: int, at: point(int, int)) .
        @type point(int, int) .

        @import threshold from csv { resource = \"thresholds.csv\" } .

        alert(?sensor, ?value) :- reading(?sensor, ?value, ?at),
                                  threshold(?sensor, ?limit), ?value >= ?limit .
        @export alert to csv .
    ";
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    engine.load(program).expect("the program loads");

    // ── Reasoning, cancellable from another thread ───────────────────
    //
    // The token is cloneable and `Send`, and `reason` checks it between
    // rule applications. A cancelled run stops at that boundary with
    // code N0207, tables consistent and progress kept. Calling
    // `reason()` again continues toward the fixpoint, which is exactly
    // what this loop does. (A watchdog this tight may or may not land
    // mid-run, and both outcomes are shown.)
    let token = engine.cancel_token();
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1));
        token.cancel();
    });
    let mut resumed = 0;
    while let Err(report) = engine.reason() {
        assert_eq!(report.code().to_string(), "N0207", "{report}");
        resumed += 1;
    }
    watchdog.join().expect("the watchdog finishes");
    println!("fixpoint reached, resumed after cancellation {resumed} times");

    // ── Results out, memory bounded ──────────────────────────────────
    //
    // A cursor holds no borrow of the engine, and each `read` returns one
    // owned, column-major chunk. Column access is the natural fit for
    // anything columnar downstream (Arrow, dataframes); `RowChunk::rows`
    // serves the row-minded.
    let mut cursor = engine.cursor("alert").expect("the predicate exists");
    println!("alerts pending: {}", cursor.remaining());
    let mut largest = None;
    while !cursor.is_done() {
        let chunk = engine
            .read_chunk(&mut cursor, 4096)
            .expect("the read succeeds");
        largest = largest
            .into_iter()
            .chain(chunk.column(1).iter().cloned())
            .max();
    }
    // Values render in canonical term syntax.
    println!(
        "largest alerted value: {}",
        largest.map_or_else(|| String::from("none"), |value| value.to_string())
    );

    // ── Exports without a filesystem ─────────────────────────────────
    //
    // `export(dir)` writes the program's `@export` directives to disk,
    // and `export_bytes` serializes one specification into memory
    // instead, for embedders that stream results onward or have no
    // filesystem.
    let specs = engine.exports().to_vec();
    for spec in specs {
        let bytes = engine.export_bytes(&spec).expect("the export serializes");
        println!(
            "{} holds {} bytes; first line: {:?}",
            spec.file_name(),
            bytes.len(),
            String::from_utf8_lossy(&bytes).lines().next().unwrap_or("")
        );
    }

    // ── What the run wants to tell you ───────────────────────────────
    //
    // Warnings accumulate on the engine (dropped rows, unused inputs);
    // the shared renderer quotes each against the sources of this run.
    let mut sources = SourceTexts::new();
    sources.absorb(engine.sources());
    for report in engine.reports().to_vec() {
        print!("{}", Renderer::plain(&sources).show(&report));
    }
    println!(
        "loaded {} facts, derived {}, in {} rule applications",
        engine.loaded_facts(),
        engine.derived_facts(),
        engine.applications()
    );
}
