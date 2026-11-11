//! Tests of reading and writing delimiter-separated values.

use std::io::Cursor;

use nemo_physical::{dictionary::Dictionary, function::EvaluationContext, value::DataValue};

use crate::{
    load::Reader,
    selection::RowCondition,
    testing::{TempDirectory, greater_than},
    value_format::ValueFormat,
    write::Writer,
};

use super::{DsvOptions, DsvReader, DsvWriter};

/// CSV rows in term syntax are read as their values.
#[test]
fn csv_rows_intern_and_round_trip() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvOptions::csv_terms(3));
    let input = "alice,42,-2.5\nbob,7,1e3\n";
    let loaded = reader
        .read(
            Cursor::new(input),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!(loaded.malformed, 0);
    assert_eq!(
        loaded.decoded(&dictionary),
        vec![
            vec![
                DataValue::iri("alice"),
                DataValue::integer(42),
                DataValue::decimal(-25, -1).expect("representable"),
            ],
            vec![
                DataValue::iri("bob"),
                DataValue::integer(7),
                DataValue::double(1000.0),
            ],
        ]
    );
}

/// A row with a cell that its format rejects, or of the wrong width, is counted as malformed.
#[test]
fn malformed_rows_are_counted_never_silent() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvOptions::csv(vec![
        ValueFormat::String,
        ValueFormat::Integer,
    ]));
    // Line 2 has a bad integer, and line 3 has the wrong width.
    let input = "a,1\nb,oops\nc\nd,4\n";
    let loaded = reader
        .read(
            Cursor::new(input),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!(loaded.rows.len(), 2);
    assert_eq!(loaded.malformed, 2);
    assert_eq!(loaded.first_malformed, Some(2));
}

/// A tab-separated resource is read with its own delimiter.
#[test]
fn delimiters_are_configurable() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]));
    let loaded = reader
        .read(
            Cursor::new("a\t1\n"),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");
    assert_eq!(
        loaded.decoded(&dictionary),
        vec![vec![DataValue::iri("a"), DataValue::integer(1)]]
    );
}

/// An empty resource loads no rows, and no malformed ones.
#[test]
fn empty_sources_load_empty() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvOptions::csv_terms(2));
    let loaded = reader
        .read(Cursor::new(""), &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    assert!(loaded.rows.is_empty());
    assert_eq!(loaded.malformed, 0);
}

/// A writer writes cells in term syntax, quoting a cell as CSV requires.
#[test]
fn writers_serialize_cells_in_the_term_format() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![DataValue::integer(42), DataValue::string("plain")],
        vec![
            DataValue::iri("Foo@en"),
            DataValue::language_string("Foo", "en").expect("the tag is valid"),
        ],
    ];
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvOptions::csv_terms(2))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(written.rows, 2);
    assert_eq!(
        String::from_utf8(target).expect("the output is text"),
        "42,\"\"\"plain\"\"\"\nFoo@en,\"\"\"Foo\"\"@en\"\n",
    );
}

/// A skipped column is left out of every written record.
#[test]
fn skipped_columns_are_left_out_of_the_record() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![DataValue::integer(1), DataValue::string("a")],
        vec![DataValue::integer(2), DataValue::string("b")],
    ];
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvOptions::new(
        b',',
        vec![ValueFormat::Skip, ValueFormat::Term],
    ))
    .write(&mut target, rows.iter().map(Vec::as_slice))
    .expect("the rows write");
    assert_eq!((written.rows, written.dropped), (2, 0));
    assert_eq!(
        String::from_utf8(target).expect("text"),
        "\"\"\"a\"\"\"\n\"\"\"b\"\"\"\n"
    );
}

/// A row with a value that does not fit its column's format is dropped from a write, and
/// counted.
#[test]
fn declared_formats_drop_unfit_rows() {
    let rows: Vec<Vec<DataValue>> = vec![vec![DataValue::integer(1)], vec![DataValue::string("x")]];
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvOptions::new(b',', vec![ValueFormat::Integer]))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!((written.rows, written.dropped), (1, 1));
    assert_eq!(String::from_utf8(target).expect("text"), "1\n");
}

/// The integer and decimal formats write and read numbers beyond 64 bits,
/// and the integer format rejects a fraction.
#[test]
fn numeric_formats_write_and_read_wide_numbers() {
    let big = |lexical: &str| DataValue::decimal_from_lexical(lexical).expect("well-formed");
    let rows: Vec<Vec<DataValue>> = vec![
        vec![big("123456789012345678901"), big("123456789012345678901.5")],
        // A fractional value is not an integer.
        vec![big("123456789012345678901.5"), big("123456789012345678901")],
    ];
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvOptions::new(
        b',',
        vec![ValueFormat::Integer, ValueFormat::Decimal],
    ))
    .write(&mut target, rows.iter().map(Vec::as_slice))
    .expect("the rows write");
    assert_eq!((written.rows, written.dropped), (1, 1));
    assert_eq!(
        String::from_utf8(target).expect("text"),
        "123456789012345678901,123456789012345678901.5\n"
    );

    assert_eq!(
        ValueFormat::Integer.parse("123456789012345678901"),
        Some(big("123456789012345678901"))
    );
    assert_eq!(ValueFormat::Integer.parse("123456789012345678901.5"), None);
}

/// A file written through gzip, as its name asks, is read back as the rows written.
#[test]
fn compressed_writes_read_back() {
    let directory = TempDirectory::new();
    let path = directory.path().join("rows.csv.gz");
    let rows: Vec<Vec<DataValue>> = vec![vec![
        DataValue::integer(7),
        DataValue::string("hello"),
        DataValue::iri("urn:x"),
    ]];
    DsvWriter::new(DsvOptions::new(b',', vec![ValueFormat::Term; 3]))
        .write_path(&path, rows.iter().map(Vec::as_slice))
        .expect("the rows write");

    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvOptions::new(b',', vec![ValueFormat::Term; 3]))
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the file reads back");
    assert_eq!(loaded.decoded(&dictionary), rows);
}

/// Writes rows in term syntax, and returns the rows read back from the result.
fn round_trip(rows: &[Vec<DataValue>]) -> Vec<Vec<DataValue>> {
    let width = rows[0].len();
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvOptions::csv_terms(width))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(
        usize::try_from(written.rows).expect("a row count fits"),
        rows.len()
    );

    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvOptions::csv(vec![ValueFormat::Term; width]))
        .read(target.as_slice(), &mut dictionary, EvaluationContext::now())
        .expect("the output reads back");
    assert_eq!(loaded.malformed, 0, "no row is malformed");
    loaded.decoded(&dictionary)
}

/// Strings with quotes, backslashes and line breaks are read back as written.
#[test]
fn escaped_strings_read_back_as_written() {
    // The escapes are written inside the quotes, and term syntax resolves them
    // in plain strings, language strings and typed literals alike.
    let rows = vec![vec![
        DataValue::string(r#"say "hi""#),
        DataValue::string(r"a\b"),
        DataValue::string("x\ny\r"),
        DataValue::string(r#"a"b"#),
        DataValue::string("plain"),
        DataValue::language_string(r#"he said "x""#, "en").expect("valid tag"),
        DataValue::other("http://example.org/t", "q\"uote\\").expect("well-formed"),
    ]];
    assert_eq!(round_trip(&rows), rows);
}

/// The limit counts well-formed rows before the condition.
#[test]
fn the_limit_counts_rows_before_the_condition() {
    // The first three well-formed rows are 1, 2 and 3, whichever of them
    // the condition keeps. A condition reduces the rows of a read with a
    // limit, and never makes it read further into the resource.
    let mut options = DsvOptions::csv(vec![ValueFormat::Integer]);
    options.condition = Some(greater_than(0, 2));
    options.limit = Some(3);
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(options)
        .read(
            "1\n2\n3\n4\n5\n".as_bytes(),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");
    assert_eq!((loaded.rows.len(), loaded.excluded), (1, 2));
    assert_eq!(
        dictionary.decode(loaded.rows.row(0)[0]).expect("decodes"),
        DataValue::integer(3).as_ref()
    );
}

/// A condition's `NOW()` is the instant of the context the read is given.
#[test]
fn a_condition_reads_the_instant_of_the_context_given() {
    use nemo_physical::{
        function::{OpCode, StackProgram},
        value::{DateTime, TimezoneOffset},
    };

    // The condition keeps a row equal to the year of `NOW()`, which is 1970 only in a
    // context at the epoch.
    let mut options = DsvOptions::csv(vec![ValueFormat::Integer]);
    options.condition = Some(RowCondition::new(
        vec![0],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::Now, OpCode::Year, OpCode::Equal],
            Vec::new(),
            1,
        ),
    ));
    let reader = DsvReader::new(options);
    let kept = |context| {
        reader
            .read("1970\n".as_bytes(), &mut Dictionary::new(), context)
            .expect("the source reads")
            .rows
            .len()
    };
    let epoch = DateTime::from_unix_millis(0, Some(TimezoneOffset::UTC));
    assert_eq!(kept(EvaluationContext::at(epoch)), 1);
    assert_eq!(kept(EvaluationContext::now()), 0);
}

/// The sniffed width is the number of cells in the first record as it is read,
/// with the given quoting and header.
#[test]
fn the_width_is_sniffed_as_the_records_are_read() {
    let sniff = |text: &str, quoting, ignore_headers| {
        DsvOptions::sniff_width_from(text.as_bytes(), b',', quoting, ignore_headers)
            .expect("the record reads")
    };
    assert_eq!(sniff("\"a,b\",c\n", true, false), Some(2));
    assert_eq!(sniff("\"a,b\",c\n", false, false), Some(3));
    assert_eq!(sniff("name\nalice,30\n", false, true), Some(2));
    assert_eq!(sniff("name\n", true, true), None);
}
