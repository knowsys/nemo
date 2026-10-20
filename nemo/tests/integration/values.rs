//! The canonical value rendering re-parses to the value it rendered.
//!
//! `Display` on [DataValue] writes Nemo term syntax; putting that text
//! into a fact and running it through parsing, translation, storage, and
//! decoding must yield the identical value. Nulls are the documented
//! exception (`_:id` is data syntax, not program syntax).

use nemo::{
    engine::Engine,
    model::{FileId, Program},
};
use nemo_physical::value::{DataValue, MapValue, TupleValue};

/// Loads a fact holding the value's rendered form and reads it back.
fn round_trips(value: DataValue) {
    let source = format!("p({value}) .");
    let (program, diagnostics) = Program::parse(&source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
    let mut engine = Engine::new();
    engine
        .load(program)
        .unwrap_or_else(|report| panic!("{source}: {report}"));
    engine
        .reason()
        .unwrap_or_else(|report| panic!("{source}: {report}"));
    let rows = engine.rows("p").expect("the predicate is registered");
    assert_eq!(rows, vec![vec![value]], "{source}");
}

#[test]
fn scalar_values_round_trip() {
    round_trips(DataValue::iri("alice"));
    round_trips(DataValue::iri("http://example.org/entry?a=1&b=2"));
    round_trips(DataValue::string("plain text"));
    round_trips(DataValue::string("with \"quotes\" and \\ escapes"));
    // The parser reads a line break in a literal normalized, so they are written escaped.
    round_trips(DataValue::string("lines\r\nand\rbreaks\n"));
    round_trips(DataValue::language_string("a\r\nb", "en").expect("valid tag"));
    round_trips(DataValue::language_string("hallo", "de").expect("valid tag"));
    round_trips(DataValue::boolean(true));
    round_trips(DataValue::boolean(false));
    round_trips(DataValue::integer(-42));
    round_trips(DataValue::decimal(15, -1).expect("representable"));
    round_trips(DataValue::double(2.5));
    round_trips(DataValue::float(2.5));
    round_trips(DataValue::other("http://example.org/unit", "5cm").expect("valid literal"));
}

#[test]
fn nonfinite_floats_round_trip_as_typed_literals() {
    round_trips(DataValue::double(f64::INFINITY));
    round_trips(DataValue::double(f64::NEG_INFINITY));
    round_trips(DataValue::double(f64::NAN));
    round_trips(DataValue::float(f32::NAN));
}

#[test]
fn datetimes_round_trip() {
    for lexical in [
        "2024-03-01T12:30:00Z",
        "2024-03-01T12:30:00.250+05:30",
        "2024-03-01T12:30:00",
        "-0044-03-15T09:00:00Z",
        "2024-03-01T12:30:00.123456Z",
        "-13798000000-01-01T00:00:00Z",
    ] {
        round_trips(DataValue::datetime_from_lexical(lexical).expect("valid lexical"));
    }
}

#[test]
fn wide_numbers_round_trip() {
    round_trips(DataValue::decimal_from_lexical("123456789012345678901").expect("canonical"));
    round_trips(DataValue::decimal_from_lexical("-1234567890.12345678901").expect("canonical"));
}

#[test]
fn imported_rows_read_back_identically() {
    // The host→value direction: rows fed in through the API come back
    // from the derived predicate exactly, rule application in between.
    let values = vec![
        DataValue::iri("alice"),
        DataValue::string("plain"),
        DataValue::language_string("hallo", "de").expect("valid tag"),
        DataValue::boolean(true),
        DataValue::integer(-42),
        DataValue::decimal(15, -1).expect("representable"),
        DataValue::double(2.5),
        DataValue::float(2.5),
        DataValue::datetime_from_lexical("2024-03-01T12:30:00Z").expect("valid"),
        DataValue::decimal_from_lexical("123456789012345678901").expect("canonical"),
        DataValue::tuple(TupleValue::labeled(
            "point",
            vec![DataValue::integer(1), DataValue::integer(2)],
        )),
        DataValue::map(
            MapValue::new([(DataValue::iri("port"), DataValue::integer(8080))])
                .expect("the keys differ"),
        ),
    ];

    let mut engine = Engine::new();
    let rows: Vec<Vec<DataValue>> = values.iter().cloned().map(|value| vec![value]).collect();
    engine.import_rows("p", rows.clone()).expect("rows import");
    let (program, diagnostics) = Program::parse("q(?x) :- p(?x) .", FileId::new(0));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    let mut derived = engine.rows("q").expect("the predicate is registered");
    let mut expected = rows;
    derived.sort();
    expected.sort();
    assert_eq!(derived, expected);
}

#[test]
fn nulls_do_not_import() {
    let mut engine = Engine::new();
    let refusal = engine
        .import_rows("p", vec![vec![DataValue::null(3)]])
        .expect_err("a null has no term");
    assert_eq!(refusal.code(), nemo::codes::NULL_IN_ROW, "{refusal:?}");
    assert_eq!(refusal.field("predicate"), Some("p"));
}

#[test]
fn rows_import_into_a_loaded_engine() {
    // Post-fixpoint rows are deltas: the next reason() carries them to
    // the new fixpoint. A predicate the program never uses stays
    // refused, since extend() introduces new ones.
    let (program, _) = Program::parse("p(1) . q(?x) :- p(?x) .", FileId::new(0));
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
        .import_rows("p", vec![vec![DataValue::integer(2)]])
        .expect("rows join a loaded engine");
    engine.reason().expect("reasoning continues");
    assert_eq!(engine.rows("q").expect("q exists").len(), 2);

    let refusal = engine
        .import_rows("r", vec![vec![DataValue::integer(1)]])
        .expect_err("an unknown predicate is refused");
    assert_eq!(
        refusal.code(),
        nemo::codes::UNKNOWN_PREDICATE,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("predicate"), Some("r"));
}

#[test]
fn structured_values_round_trip() {
    let tuple = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));
    round_trips(tuple(vec![DataValue::integer(1), DataValue::integer(2)]));
    // The one-element tuple keeps its comma.
    round_trips(tuple(vec![DataValue::string("solo")]));
    round_trips(DataValue::tuple(TupleValue::labeled(
        "point",
        vec![DataValue::integer(1), DataValue::integer(2)],
    )));
    round_trips(DataValue::map(
        MapValue::new([
            (DataValue::iri("port"), DataValue::integer(8080)),
            (DataValue::iri("host"), DataValue::string("localhost")),
        ])
        .expect("the keys differ"),
    ));
    round_trips(DataValue::map(
        MapValue::labeled("config", [(DataValue::iri("port"), DataValue::integer(80))])
            .expect("the keys differ"),
    ));
    // Nesting recurses.
    round_trips(tuple(vec![
        DataValue::integer(1),
        tuple(vec![DataValue::integer(2), DataValue::integer(3)]),
    ]));
    // A label that is no bare name keeps its angle brackets.
    for label in ["http://example.org/point", "", "_", "true", "1abc"] {
        round_trips(DataValue::tuple(TupleValue::labeled(
            label,
            vec![DataValue::integer(1)],
        )));
        round_trips(DataValue::map(
            MapValue::labeled(label, [(DataValue::iri("a"), DataValue::integer(1))])
                .expect("the keys differ"),
        ));
    }
}
