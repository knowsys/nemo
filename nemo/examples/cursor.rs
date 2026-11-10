//! Reading a large predicate in bounded memory, and exporting into memory.
//!
//! Usage: `cargo run -p nemo --example cursor`

use nemo::{
    engine::{Engine, ExportSpec},
    values::{DataValue, NumberRef, ValueRef},
};

/// The readings of sensors, and an alert for each reading above the threshold of its sensor.
const PROGRAM: &str = r#"
    @import threshold from csv { resource = "thresholds.csv" } .
    alert(?sensor, ?value) :- reading(?sensor, ?value), threshold(?sensor, ?most), ?value > ?most .
    @export alert to csv .
"#;

fn main() {
    let mut engine = Engine::new();

    // Rows computed in memory are given before the load, which checks them as it checks facts.
    engine
        .import_rows(
            "reading",
            (0..50_000).map(|index| {
                vec![
                    DataValue::iri(format!("sensor{}", index % 100)),
                    DataValue::integer(index),
                ]
            }),
        )
        .expect("the rows are kept for the load");
    // An `@import` of a resource with a registered name reads the registered bytes
    // in place of a file.
    engine.add_resource(
        "thresholds.csv",
        "sensor7,40000\nsensor23,10000\n".as_bytes().to_vec(),
    );
    engine
        .load_source("alerts.rls", PROGRAM)
        .expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    // A cursor is plain data, which borrows nothing from the engine.
    // Each read returns the next rows as one owned chunk of at most the given size,
    // so the memory in use is bounded by the chunk, however large the predicate is.
    let mut cursor = engine.cursor("alert").expect("the program derives `alert`");
    println!("{} rows to read", cursor.remaining());
    let (mut chunks, mut largest) = (0, 0);
    while !cursor.is_done() {
        let chunk = engine
            .read_chunk(&mut cursor, 128)
            .expect("the table is as it was when the cursor was opened");
        chunks += 1;
        // A chunk stores its values by column, which suits a sum, a maximum or a dataframe.
        for value in chunk.column(1) {
            if let ValueRef::Number(NumberRef::Decimal(number)) = value.as_ref() {
                largest = largest.max(number.to_i64().expect("a reading fits in 64 bits"));
            }
        }
    }
    println!("read in {chunks} chunks, and the largest value is {largest}");

    // A chunk also returns its rows one by one.
    let mut cursor = engine.cursor("alert").expect("the program derives `alert`");
    let first = engine
        .read_chunk(&mut cursor, 2)
        .expect("the table is as it was when the cursor was opened");
    for row in first.rows() {
        println!("alert({}, {}) .", row[0], row[1]);
    }

    // `export` writes the program's exports into a directory,
    // and `export_bytes` serializes one export into memory.
    for spec in engine.exports().to_vec() {
        let bytes = engine.export_bytes(&spec).expect("the export serializes");
        println!("{} holds {} bytes", spec.file_name(), bytes.len());
    }

    // An export specification can also be built, for a predicate that the program does not
    // export or for another serialization.
    let mut readings = ExportSpec::csv("reading");
    readings.limit = Some(2);
    let bytes = engine
        .export_bytes(&readings)
        .expect("the export serializes");
    print!("{}", String::from_utf8_lossy(&bytes));
}
