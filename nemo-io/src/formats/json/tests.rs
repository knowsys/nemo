//! Tests of reading and writing JSON records.

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    value::{DataValue, MapValue, TupleValue, ValueRef},
};

use crate::{
    load::{Loaded, Reader, RowCounts},
    value_format::ColumnFormat,
    write::Writer,
};

use super::{JsonReader, JsonReaderOptions, JsonWriter, JsonWriterOptions};

/// Returns the formats with the given names.
fn formats(names: &[&str]) -> Vec<ColumnFormat> {
    names
        .iter()
        .map(|name| ColumnFormat::from_name(name).expect("the format exists"))
        .collect()
}

/// Reads a text with the given options, and returns the rows and the counts.
fn read(
    options: JsonReaderOptions,
    text: &str,
) -> Result<(Vec<Vec<DataValue>>, RowCounts), Diagnostic> {
    let mut dictionary = Dictionary::new();
    let loaded: Loaded = JsonReader::new(options).read(
        text.as_bytes(),
        &mut dictionary,
        EvaluationContext::now(),
    )?;

    Ok((loaded.decoded(&dictionary), loaded.counts))
}

/// Writes rows with the given options, and returns the text and the number of dropped rows.
fn write(options: JsonWriterOptions, rows: &[Vec<DataValue>]) -> (String, u64) {
    let mut target = Vec::new();
    let written = JsonWriter::new(options)
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");

    (
        String::from_utf8(target).expect("the output is text"),
        written.dropped,
    )
}

/// One value of every kind, of which each row has three.
fn every_kind() -> Vec<Vec<DataValue>> {
    let typed = |datatype: &str, lexical: &str| {
        let datatype = format!("http://www.w3.org/2001/XMLSchema#{datatype}");
        DataValue::typed_literal(&datatype, lexical).expect("the literal is valid")
    };
    let tuple = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));
    let map = |pairs: Vec<(DataValue, DataValue)>| {
        DataValue::map(MapValue::new(pairs).expect("the keys differ"))
    };
    let values = vec![
        DataValue::string("Alice"),
        DataValue::string("say \"hi\"\\\n\t\u{1}é"),
        DataValue::boolean(true),
        DataValue::integer(34),
        DataValue::decimal_from_lexical("-123456789012345678901234567890").expect("valid"),
        DataValue::double(2.0),
        DataValue::double(-1e300),
        DataValue::double(f64::NEG_INFINITY),
        DataValue::float(2.5),
        DataValue::decimal_from_lexical("1.5").expect("valid"),
        DataValue::iri("http://example.org/bob"),
        DataValue::iri("alice"),
        DataValue::language_string("hallo", "de").expect("the tag is valid"),
        typed("dateTime", "2026-10-18T12:00:00Z"),
        typed("date", "2026-10-18"),
        typed("time", "12:00:00"),
        typed("duration", "P1Y2M3DT4H"),
        typed("dayTimeDuration", "PT1H"),
        DataValue::other("http://example.org/length", "5cm").expect("not native"),
        tuple(vec![]),
        tuple(vec![
            DataValue::integer(1),
            tuple(vec![DataValue::iri("x")]),
        ]),
        DataValue::tuple(TupleValue::labeled("point", vec![DataValue::integer(1)])),
        map(vec![
            (DataValue::string("host"), DataValue::string("example.org")),
            (DataValue::string("port"), DataValue::integer(80)),
        ]),
        map(vec![(DataValue::integer(1), DataValue::string("one"))]),
        map(vec![(
            DataValue::string("@iri"),
            DataValue::string("no tag"),
        )]),
        DataValue::map(MapValue::labeled("box", []).expect("the map is empty")),
        map(vec![]),
    ];

    values.chunks(3).map(<[DataValue]>::to_vec).collect()
}

/// Rows of every kind of value are read back as they were written,
/// as arrays and as objects, in a document and with one record per line.
#[test]
fn written_records_are_read_back() {
    let rows = every_kind();
    for (lines, keys) in [(false, false), (false, true), (true, false), (true, true)] {
        let columns =
            keys.then(|| vec![String::from("a"), String::from("b c"), String::from("@d")]);
        let mut writing = JsonWriterOptions::new(formats(&["term", "term", "term"]));
        writing.lines = lines;
        writing.columns = columns.clone();
        let (text, dropped) = write(writing, &rows);
        assert_eq!(dropped, 0);

        let mut reading = JsonReaderOptions::new(formats(&["term", "term", "term"]));
        reading.lines = lines;
        reading.columns = columns;
        let (read, counts) = read(reading, &text).expect("the text reads");
        assert_eq!(read, rows, "{text}");
        assert_eq!(counts.malformed, 0, "{text}");
    }
}

/// A written document has one record per line inside its array,
/// and each value is written as its kind in JSON or as a tagged object.
#[test]
fn a_document_writes_one_record_per_line() {
    let rows = vec![
        vec![
            DataValue::string("Alice"),
            DataValue::integer(34),
            DataValue::boolean(true),
        ],
        vec![
            DataValue::iri("http://example.org/bob"),
            DataValue::decimal_from_lexical("1.5").expect("valid"),
            DataValue::language_string("hallo", "de").expect("valid"),
        ],
        vec![
            DataValue::tuple(TupleValue::new(vec![
                DataValue::integer(1),
                DataValue::null(5),
            ])),
            DataValue::map(
                MapValue::new([(DataValue::string("port"), DataValue::integer(80))])
                    .expect("one key"),
            ),
            DataValue::double(2.0),
        ],
    ];

    let (text, _) = write(
        JsonWriterOptions::new(formats(&["term", "term", "term"])),
        &rows,
    );
    assert_eq!(
        text,
        concat!(
            "[\n",
            "[\"Alice\", 34, true],\n",
            "[{\"@iri\": \"http://example.org/bob\"}, {\"@decimal\": \"1.5\"}, ",
            "{\"@langstring\": \"hallo\", \"@language\": \"de\"}],\n",
            "[[1, {\"@null\": 5}], {\"port\": 80}, 2.0]\n",
            "]\n"
        )
    );

    // Without rows, the document is the empty array.
    let (text, _) = write(JsonWriterOptions::new(formats(&["term"])), &[]);
    assert_eq!(text, "[\n]\n");

    // With keys and formats, a record is an object of bare forms, and a skipped column is
    // left out.
    let mut options = JsonWriterOptions::new(formats(&["iri", "skip", "datetime?"]));
    options.lines = true;
    options.columns = Some(vec![
        String::from("who"),
        String::from("x"),
        String::from("when"),
    ]);
    let when = DataValue::datetime_from_lexical("2026-10-18T12:00:00Z").expect("valid");
    let rows = vec![
        vec![
            DataValue::iri("http://example.org/alice"),
            DataValue::integer(1),
            when,
        ],
        vec![
            DataValue::iri("urn:b"),
            DataValue::integer(2),
            DataValue::null(3),
        ],
        vec![
            DataValue::string("no IRI"),
            DataValue::integer(3),
            DataValue::null(4),
        ],
    ];
    assert_eq!(
        write(options, &rows),
        (
            String::from(concat!(
                "{\"who\": \"http://example.org/alice\", \"when\": \"2026-10-18T12:00:00Z\"}\n",
                "{\"who\": \"urn:b\", \"when\": null}\n",
            )),
            1
        )
    );
}

/// The records of another tool are read by the keys and formats of the columns:
/// a key that is not named is ignored, an array is a tuple,
/// and `null` and an absent key are missing values.
#[test]
fn objects_are_read_by_the_keys_of_the_columns() {
    let text = r#"[
        {"id": 1, "name": "Alice", "email": "a@example.org", "tags": ["admin", "dev"]},
        {"id": 2, "name": "Bob", "email": null, "tags": ["ops"]},
        {"id": 3, "name": "Carol", "tags": ["dev"], "address": {"city": "Dresden"}},
        {"id": "4", "name": "Dan", "tags": []},
        ["5", "Eve", null, []]
    ]"#;
    let keys = ["id", "name", "email", "tags"].map(String::from).to_vec();
    let mut options = JsonReaderOptions::new(formats(&["int", "string", "string?", "term"]));
    options.columns = Some(keys.clone());
    let (rows, counts) = read(options, text).expect("the text reads");

    let strings = |texts: &[&str]| {
        DataValue::tuple(TupleValue::new(
            texts
                .iter()
                .map(|&text| DataValue::string(text))
                .collect::<Vec<_>>(),
        ))
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][3], strings(&["admin", "dev"]));
    assert_eq!(rows[0][2], DataValue::string("a@example.org"));
    assert_eq!(rows[1][1..2], [DataValue::string("Bob")]);
    assert!(matches!(rows[1][2].as_ref(), ValueRef::Null(_)));
    assert!(matches!(rows[2][2].as_ref(), ValueRef::Null(_)));
    assert_ne!(
        rows[1][2], rows[2][2],
        "each missing value is a null of its own"
    );

    // The fourth record has a string as its id, and the fifth is no object.
    assert_eq!((counts.malformed, counts.missing), (2, 0));

    // Without the marker, the two records without an email are counted apart.
    let mut options = JsonReaderOptions::new(formats(&["int", "string", "string", "term"]));
    options.columns = Some(keys);
    let (rows, counts) = read(options, text).expect("the text reads");
    assert_eq!((rows.len(), counts.missing, counts.malformed), (1, 2, 2));
}

/// A column with a format holds bare forms, and a number is read from its text.
#[test]
fn typed_columns_hold_bare_forms() {
    let text = r#"[
        [1.5, 1.5, "1.5", true, "2026-10-18"],
        [0.1234567890123456789012345, 2, "x", false, "2026-10-19"],
        ["1.5", 1.5, "x", true, "2026-10-18"],
        [1.5, "1.5", "x", true, "2026-10-18"],
        [1.5, 1.5, 1.5, true, "2026-10-18"],
        [1.5, 1.5, "x", "true", "2026-10-18"],
        [1.5, 1.5, "x", true, "October"],
        [1e3, 1.5, "x", true, "2026-10-18"]
    ]"#;
    let options = JsonReaderOptions::new(formats(&["decimal", "double", "string", "bool", "date"]));
    let (rows, counts) = read(options, text).expect("the text reads");

    assert_eq!((rows.len(), counts.malformed), (2, 6));
    assert_eq!(
        rows[0][0],
        DataValue::decimal_from_lexical("1.5").expect("valid")
    );
    assert_eq!(rows[0][1], DataValue::double(1.5));
    assert_eq!(
        rows[1][0],
        DataValue::decimal_from_lexical("0.1234567890123456789012345").expect("valid")
    );
    assert_eq!(rows[1][1], DataValue::double(2.0));
}

/// In a term column, the spelling of a number decides, and an object with `@` keys is one of
/// the tagged forms or nothing.
#[test]
fn term_cells_say_what_they_are() {
    let cell = |text: &str| {
        let (rows, _) = read(
            JsonReaderOptions::new(formats(&["term"])),
            &format!("[[{text}]]"),
        )
        .expect("the text reads");

        rows.into_iter().next().map(|mut row| row.remove(0))
    };

    assert_eq!(cell("2"), Some(DataValue::integer(2)));
    assert_eq!(cell("2.0"), Some(DataValue::double(2.0)));
    assert_eq!(cell("2e0"), Some(DataValue::double(2.0)));
    assert_eq!(cell("-0"), Some(DataValue::integer(0)));
    assert_eq!(cell(r#"{"@int": "7"}"#), Some(DataValue::integer(7)));
    assert_eq!(cell(r#"{"@iri": "urn:a"}"#), Some(DataValue::iri("urn:a")));
    assert_eq!(
        cell(r#"{"@double": "NaN"}"#),
        Some(DataValue::double(f64::NAN))
    );
    assert_eq!(
        cell(r#"{"@tuple": [1], "@label": "point"}"#),
        Some(DataValue::tuple(TupleValue::labeled(
            "point",
            vec![DataValue::integer(1)]
        )))
    );

    // A label names one null throughout a document.
    let nulls = cell(r#"[{"@null": "x"}, {"@null": "x"}, {"@null": 7}]"#).expect("the cell reads");
    let ValueRef::Tuple(nulls) = nulls.as_ref() else {
        panic!("an array is a tuple: {nulls}");
    };
    let nulls: Vec<ValueRef<'_>> = nulls.elements().collect();
    assert!(matches!(nulls[0], ValueRef::Null(_)));
    assert_eq!(nulls[0], nulls[1]);
    assert_ne!(nulls[0], nulls[2]);

    for text in [
        r#"1e999"#,
        r#"{"@unknown": "x"}"#,
        r#"{"@iri": "urn:a", "extra": 1}"#,
        r#"{"@iri": "urn:a", "@language": "de"}"#,
        r#"{"@iri": 5}"#,
        r#"{"@int": 7}"#,
        r#"{"@int": "seven"}"#,
        r#"{"@term": "alice"}"#,
        r#"{"@langstring": "hallo"}"#,
        r#"{"@map": [[1]]}"#,
        r#"{"a": 1, "a": 2}"#,
        r#"[1, null]"#,
    ] {
        assert_eq!(cell(text), None, "{text}");
    }
}

/// The `path` option names the array of records inside a document.
/// A document that is not JSON fails the read, and so does a place that holds no array.
#[test]
fn the_records_are_an_array_in_the_document() {
    let text = r#"{"message": {"items": [["a"], ["b"]], "total": 2}}"#;
    let mut options = JsonReaderOptions::new(formats(&["term"]));
    options.path = Some(String::from("/message/items"));
    let (rows, _) = read(options.clone(), text).expect("the text reads");
    assert_eq!(rows.len(), 2);

    for (path, text, code) in [
        (Some("/message/total"), text, crate::codes::NOT_RECORDS),
        (Some("/message/none"), text, crate::codes::NOT_RECORDS),
        (None, text, crate::codes::NOT_RECORDS),
        (None, r#"[["a"], ["b"]"#, crate::codes::NOT_JSON),
        (None, r#"[["a"]] x"#, crate::codes::NOT_JSON),
        (None, r#"{"message": "#, crate::codes::NOT_JSON),
        (Some("/message"), r#"{"message": "#, crate::codes::NOT_JSON),
    ] {
        options.path = path.map(String::from);
        let problem = read(options.clone(), text).expect_err(text);
        assert_eq!(problem.code(), code, "{path:?} in {text}");
    }
}

/// With one record per line, a line that is not JSON is one malformed row,
/// an empty line is no record, and the limit counts the well-formed rows.
#[test]
fn lines_are_records_of_their_own() {
    let text = "[1, \"a\"]\n\n[2, \"b\"\n[3]\n[4, \"d\"]\r\n[5, \"e\"]";
    let mut options = JsonReaderOptions::new(formats(&["int", "string"]));
    options.lines = true;
    let (rows, counts) = read(options.clone(), text).expect("the text reads");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2], [DataValue::integer(5), DataValue::string("e")]);
    assert_eq!((counts.malformed, counts.first_malformed), (2, Some(3)));

    options.limit = Some(2);
    let (rows, counts) = read(options, text).expect("the text reads");
    assert_eq!((rows.len(), counts.malformed), (2, 2));
}

/// The first record gives the number of columns of a resource of arrays.
#[test]
fn the_width_is_the_length_of_the_first_record() {
    let width = |text: &str, lines: bool, path: Option<&str>| {
        JsonReader::sniff_width_from(text.as_bytes(), lines, path).expect("the text is JSON")
    };

    assert_eq!(width(r#"[[1, 2, 3], [4]]"#, false, None), Some(3));
    assert_eq!(width(r#"[]"#, false, None), None);
    assert_eq!(width(r#"[{"a": 1}]"#, false, None), None);
    assert_eq!(
        width(r#"{"items": [[1, 2]]}"#, false, Some("/items")),
        Some(2)
    );
    assert_eq!(width("\n[1, 2]\n[3]\n", true, None), Some(2));
    assert_eq!(width("", true, None), None);
}
