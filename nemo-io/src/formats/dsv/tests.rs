//! Tests of reading and writing delimiter-separated values.

use std::io::Cursor;

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::{EvaluationContext, OpCode, StackProgram},
    value::{DataValue, DomainSet, TupleValue, ValueRef},
};

use crate::{
    load::{Loaded, Reader, RowCounts},
    selection::RowCondition,
    testing::{TempDirectory, greater_than},
    value_format::{ColumnFormat, ValueFormat},
    write::Writer,
};

use super::{DsvReader, DsvReaderOptions, DsvWriter, DsvWriterOptions};

/// CSV rows in term syntax are read as their values.
#[test]
fn csv_rows_intern_and_round_trip() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvReaderOptions::csv_terms(3));
    let input = "alice,42,-2.5\nbob,7,1e3\n";
    let loaded = reader
        .read(
            Cursor::new(input),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!(loaded.counts.malformed, 0);
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
    let reader = DsvReader::new(DsvReaderOptions::csv(vec![
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
    assert_eq!(loaded.counts.malformed, 2);
    assert_eq!(loaded.counts.first_malformed, Some(2));
}

/// A tab-separated resource is read with its own delimiter.
#[test]
fn delimiters_are_configurable() {
    let mut dictionary = Dictionary::new();
    let reader = DsvReader::new(DsvReaderOptions::tsv(vec![
        ValueFormat::Term,
        ValueFormat::Term,
    ]));
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
    let reader = DsvReader::new(DsvReaderOptions::csv_terms(2));
    let loaded = reader
        .read(Cursor::new(""), &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    assert!(loaded.rows.is_empty());
    assert_eq!(loaded.counts.malformed, 0);
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
    let written = DsvWriter::new(DsvWriterOptions::csv(vec![ValueFormat::Term; 2]))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(written.rows, 2);
    assert_eq!(
        String::from_utf8(target).expect("the output is text"),
        "42,\"\"\"plain\"\"\"\n<Foo@en>,\"\"\"Foo\"\"@en\"\n",
    );
}

/// A term cell that is not one constant makes its row malformed:
/// a full IRI without brackets, and a text without quotes.
/// An empty cell is a missing value, which is counted apart.
#[test]
fn a_cell_that_is_no_constant_makes_its_row_malformed() {
    let input = [
        "<http://example.org/a>,1",
        "http://example.org/a,2",
        "Alice Smith,3",
        ",4",
        "\"\"\"Alice Smith\"\"\",5",
        "",
    ]
    .join("\n");
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvReaderOptions::csv_terms(2))
        .read(
            Cursor::new(input),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    let counts = &loaded.counts;
    assert_eq!((counts.malformed, counts.first_malformed), (2, Some(2)));
    assert_eq!((counts.missing, counts.first_missing), (1, Some(4)));
    assert_eq!(
        loaded.decoded(&dictionary),
        vec![
            vec![
                DataValue::iri("http://example.org/a"),
                DataValue::integer(1)
            ],
            vec![DataValue::string("Alice Smith"), DataValue::integer(5)],
        ]
    );
}

/// Reads a CSV text with the given column formats.
fn read_csv(formats: &[&str], input: &str) -> (Vec<Vec<DataValue>>, RowCounts) {
    let formats = formats
        .iter()
        .map(|name| ColumnFormat::from_name(name).expect("the format exists"));
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvReaderOptions::csv(formats))
        .read(
            Cursor::new(input.to_owned()),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    (loaded.decoded(&dictionary), loaded.counts)
}

/// An empty cell is a missing value.
/// A column with the marker reads it as a null of its own,
/// and in any other column its row is dropped and counted apart from the malformed rows,
/// unless the column's format reads the empty text.
#[test]
fn a_missing_value_is_a_fresh_null_where_the_column_admits_it() {
    let input = "1,,\n2,,\n3,5,x\nfour,,\n";

    // Without the marker, the rows with an empty `int` cell are dropped.
    let (rows, counts) = read_csv(&["int", "int", "string"], input);
    assert_eq!(rows.len(), 1);
    assert_eq!((counts.missing, counts.first_missing), (2, Some(1)));

    // A row that is malformed elsewhere is counted as malformed only.
    assert_eq!((counts.malformed, counts.first_malformed), (1, Some(4)));

    // With the marker, each empty cell is a null of its own.
    let (rows, counts) = read_csv(&["int", "int?", "string?"], input);
    assert_eq!((rows.len(), counts.missing, counts.malformed), (3, 0, 1));
    let nulls = [&rows[0][1], &rows[0][2], &rows[1][1], &rows[1][2]];
    for (index, null) in nulls.iter().enumerate() {
        assert!(matches!(null.as_ref(), ValueRef::Null(_)), "{null}");
        assert!(
            !nulls[..index].contains(null),
            "{null} is a null of its own"
        );
    }
    assert_eq!(
        rows[2][1..],
        [DataValue::integer(5), DataValue::string("x")]
    );

    // The `string` format reads the empty text, so its empty cell is the empty string.
    let (rows, counts) = read_csv(&["int", "int?", "string"], "1,,\n");
    assert_eq!(rows[0][2], DataValue::string(""));
    assert_eq!(counts.missing, 0);
}

/// A condition sees the null of a missing value,
/// so it can keep only the rows that have the value, without a warning.
#[test]
fn a_condition_can_require_a_value_that_may_be_missing() {
    let is_string = RowCondition::new(
        vec![1],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::InDomains(DomainSet::STRING)],
            Vec::new(),
            1,
        ),
    );
    let formats = ["int", "string?"].map(|name| ColumnFormat::from_name(name).expect("exists"));
    let mut options = DsvReaderOptions::csv(formats);
    options.condition = Some(is_string);
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(options)
        .read(
            Cursor::new("1,a\n2,\n3,c\n"),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!((loaded.counts.excluded, loaded.counts.missing), (1, 0));
    assert_eq!(
        loaded.decoded(&dictionary),
        vec![
            vec![DataValue::integer(1), DataValue::string("a")],
            vec![DataValue::integer(3), DataValue::string("c")],
        ]
    );
}

/// A column with the marker writes a null as an empty cell.
/// A value whose cell would be empty has no form there, so its row is dropped.
#[test]
fn a_null_is_written_as_a_missing_value_where_the_column_admits_it() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![DataValue::integer(1), DataValue::string("a")],
        vec![DataValue::null(7), DataValue::null(8)],
        vec![DataValue::integer(3), DataValue::string("")],
    ];
    let write = |formats: [&str; 2]| {
        let formats = formats.map(|name| ColumnFormat::from_name(name).expect("exists"));
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvWriterOptions::csv(formats))
            .write(&mut target, rows.iter().map(Vec::as_slice))
            .expect("the rows write");

        (
            String::from_utf8(target).expect("the output is text"),
            written.dropped,
        )
    };

    assert_eq!(write(["int?", "string?"]), (String::from("1,a\n,\n"), 1));

    // Without the marker a null has no form, and the empty string is the empty cell.
    assert_eq!(write(["int", "string"]), (String::from("1,a\n3,\n"), 1));

    // A term column keeps the null, and its marker writes the null as a missing value.
    assert_eq!(
        write(["term", "term?"]),
        (String::from("1,\"\"\"a\"\"\"\n_:7,\n3,\"\"\"\"\"\"\n"), 0)
    );
}

/// A label names one null throughout a read and another null in the next read,
/// and a row with a null keeps its place among the other rows.
#[test]
fn a_null_is_a_null_of_its_resource() {
    let input = "_:a,1\nx,2\n\"(_:a,)\",3\n_:b,4\n";
    let mut dictionary = Dictionary::new();
    let mut read = || {
        let loaded = DsvReader::new(DsvReaderOptions::csv_terms(2))
            .read(
                Cursor::new(input),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");
        assert_eq!(loaded.counts.malformed, 0);

        loaded.decoded(&dictionary)
    };
    let first = read();
    let second = read();

    let numbers = |rows: &[Vec<DataValue>]| -> Vec<DataValue> {
        rows.iter().map(|row| row[1].clone()).collect()
    };
    assert_eq!(
        numbers(&first),
        (1..=4).map(DataValue::integer).collect::<Vec<_>>()
    );

    // The tuple of the third row holds the null of the first row.
    let null = first[0][0].clone();
    assert!(matches!(null.as_ref(), ValueRef::Null(_)), "{null}");
    assert_eq!(
        first[2][0],
        DataValue::tuple(TupleValue::new(vec![null.clone()]))
    );
    assert_ne!(first[3][0], null);
    assert_ne!(second[0][0], null);
    assert_eq!(numbers(&second), numbers(&first));
}

/// A row with a null is admitted by the condition and counted by the limit as any other row.
#[test]
fn rows_with_nulls_meet_the_condition_and_the_limit() {
    let read = |options: DsvReaderOptions, input: &str| {
        let mut dictionary = Dictionary::new();
        let loaded = DsvReader::new(options)
            .read(
                Cursor::new(input.to_owned()),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");
        let rows = loaded.decoded(&dictionary);

        (rows, loaded.counts.excluded, loaded.counts.malformed)
    };

    let mut options = DsvReaderOptions::csv_terms(2);
    options.condition = Some(greater_than(1, 2));
    let (rows, excluded, malformed) = read(options, "_:a,1\n_:a,5\nx,7\ny,0\n");
    assert_eq!((rows.len(), excluded, malformed), (2, 2, 0));
    assert_eq!(rows[0][1], DataValue::integer(5));
    assert_eq!(rows[1][0], DataValue::iri("x"));

    // The limit counts the rows with nulls among the well-formed rows.
    let mut options = DsvReaderOptions::csv_terms(2);
    options.limit = Some(2);
    let (rows, _, _) = read(options, "_:a,1\n_:b,2\n_:c,3\nx,4\n");
    assert_eq!(rows.len(), 2);
}

/// A skipped column is left out of every written record.
#[test]
fn skipped_columns_are_left_out_of_the_record() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![DataValue::integer(1), DataValue::string("a")],
        vec![DataValue::integer(2), DataValue::string("b")],
    ];
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvWriterOptions::new(
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
    let written = DsvWriter::new(DsvWriterOptions::new(b',', vec![ValueFormat::Integer]))
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
    let written = DsvWriter::new(DsvWriterOptions::new(
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
        ValueFormat::Integer.parse("123456789012345678901", &mut |_| DataValue::null(0)),
        Some(big("123456789012345678901"))
    );
    assert_eq!(
        ValueFormat::Integer.parse("123456789012345678901.5", &mut |_| DataValue::null(0)),
        None
    );
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
    DsvWriter::new(DsvWriterOptions::new(b',', vec![ValueFormat::Term; 3]))
        .write_path(&path, rows.iter().map(Vec::as_slice))
        .expect("the rows write");

    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvReaderOptions::new(b',', vec![ValueFormat::Term; 3]))
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the file reads back");
    assert_eq!(loaded.decoded(&dictionary), rows);
}

/// Writes rows in term syntax, and returns the rows read back from the result.
fn round_trip(rows: &[Vec<DataValue>]) -> Vec<Vec<DataValue>> {
    let width = rows[0].len();
    let mut target = Vec::new();
    let written = DsvWriter::new(DsvWriterOptions::csv(vec![ValueFormat::Term; width]))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(
        usize::try_from(written.rows).expect("a row count fits"),
        rows.len()
    );

    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(DsvReaderOptions::csv(vec![ValueFormat::Term; width]))
        .read(target.as_slice(), &mut dictionary, EvaluationContext::now())
        .expect("the output reads back");
    assert_eq!(loaded.counts.malformed, 0, "no row is malformed");
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
    let mut options = DsvReaderOptions::csv(vec![ValueFormat::Integer]);
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
    assert_eq!((loaded.rows.len(), loaded.counts.excluded), (1, 2));
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
    let mut options = DsvReaderOptions::csv(vec![ValueFormat::Integer]);
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
        DsvReaderOptions::sniff_width_from(text.as_bytes(), b',', quoting, ignore_headers)
            .expect("the record reads")
    };
    assert_eq!(sniff("\"a,b\",c\n", true, false), Some(2));
    assert_eq!(sniff("\"a,b\",c\n", false, false), Some(3));
    assert_eq!(sniff("name\nalice,30\n", false, true), Some(2));
    assert_eq!(sniff("name\n", true, true), None);
}

/// Reads a CSV text whose first record names its columns.
fn read_named(names: &[&str], formats: &[&str], input: &str) -> Result<Loaded, Diagnostic> {
    let formats = formats
        .iter()
        .map(|name| ColumnFormat::from_name(name).expect("the format exists"));
    let mut options = DsvReaderOptions::csv(formats);
    options.columns = Some(names.iter().map(|&name| name.to_owned()).collect());

    DsvReader::new(options).read(
        Cursor::new(input.to_owned()),
        &mut Dictionary::new(),
        EvaluationContext::now(),
    )
}

/// With named columns, the first record is the header:
/// each column is found by its name, whatever its position,
/// and a column that is not named is not read.
#[test]
fn named_columns_are_found_by_the_header() {
    // The `note` column is not named, so its cells are not read, whatever they hold.
    let input = "age,note,name\n30,\"not, a term\",alice\nold,,bob\n17,x\n";
    let names = ["name", "age"];
    let mut options = DsvReaderOptions::csv([ValueFormat::Term, ValueFormat::Integer]);
    options.columns = Some(names.map(String::from).to_vec());
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(options)
        .read(
            Cursor::new(input),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!(
        loaded.decoded(&dictionary),
        vec![vec![DataValue::iri("alice"), DataValue::integer(30)]]
    );

    // Line 3 has no integer as its age, and line 4 lacks a cell.
    let counts = &loaded.counts;
    assert_eq!((counts.malformed, counts.first_malformed), (2, Some(3)));
}

/// A header that lacks a named column, or has it more than once, fails the read.
#[test]
fn a_header_must_have_each_named_column_once() {
    let problem = read_named(&["name", "age"], &["term", "int"], "name,years\nalice,30\n")
        .expect_err("the header has no `age`");
    assert_eq!(problem.code(), crate::codes::HEADER_LACKS_COLUMN);
    assert_eq!(problem.field("name"), Some("age"));

    let problem = read_named(&["name"], &["term"], "name,age,name\nalice,30,bob\n")
        .expect_err("the header has `name` twice");
    assert_eq!(problem.code(), crate::codes::HEADER_REPEATS_COLUMN);

    // A column that is not named may repeat, and an empty resource has no header.
    assert!(read_named(&["name"], &["term"], "name,age,age\nalice,30,31\n").is_ok());
    let problem = read_named(&["name"], &["term"], "").expect_err("there is no header");
    assert_eq!(problem.code(), crate::codes::HEADER_LACKS_COLUMN);
}

/// A writer with named columns writes them as the header,
/// and a reader finds the columns in the written resource by any order of the names.
#[test]
fn a_written_header_names_the_columns_for_a_reader() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![DataValue::iri("alice"), DataValue::integer(30)],
        vec![DataValue::iri("bob"), DataValue::integer(17)],
    ];
    let mut options = DsvWriterOptions::csv([ValueFormat::Term, ValueFormat::Integer]);
    options.columns = Some(vec![String::from("name"), String::from("age, in years")]);
    let mut target = Vec::new();
    DsvWriter::new(options)
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    let written = String::from_utf8(target).expect("the output is text");
    assert_eq!(written, "name,\"age, in years\"\nalice,30\nbob,17\n");

    let mut options = DsvReaderOptions::csv([ValueFormat::Integer, ValueFormat::Term]);
    options.columns = Some(vec![String::from("age, in years"), String::from("name")]);
    let mut dictionary = Dictionary::new();
    let loaded = DsvReader::new(options)
        .read(
            Cursor::new(written),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");
    assert_eq!(
        loaded.decoded(&dictionary),
        vec![
            vec![DataValue::integer(30), DataValue::iri("alice")],
            vec![DataValue::integer(17), DataValue::iri("bob")],
        ]
    );
}

/// A record whose only cell is empty is written in quotes,
/// so that it is read back as a record with a missing value, and not skipped as an empty line.
#[test]
fn a_record_of_one_missing_value_is_read_back() {
    let rows: Vec<Vec<DataValue>> = vec![vec![DataValue::null(1)], vec![DataValue::string("a")]];
    let format = ColumnFormat::from_name("string?").expect("the format exists");
    let mut target = Vec::new();
    DsvWriter::new(DsvWriterOptions::csv([format]))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    let written = String::from_utf8(target).expect("the output is text");
    assert_eq!(written, "\"\"\na\n");

    let (read, counts) = read_csv(&["string?"], &written);
    assert_eq!((read.len(), counts.malformed), (2, 0));
    assert!(matches!(read[0][0].as_ref(), ValueRef::Null(_)));
    assert_eq!(read[1][0], DataValue::string("a"));
}

/// A sink takes its rows one at a time, and a row that fails ends the write with its error.
#[test]
fn a_write_ends_with_the_error_of_its_rows() {
    let writer = DsvWriter::new(DsvWriterOptions::csv([ValueFormat::Integer]));
    let mut target = Vec::new();
    let outcome = writer.write_compressed_with(&mut target, None, |sink| {
        sink.write_row(&[ValueRef::Boolean(true)])?;
        sink.write_row(&[DataValue::integer(7).as_ref()])?;

        Err(crate::error::IoError::input_unreadable(
            "the table ended early",
        ))
    });

    let problem = outcome.expect_err("the rows fail");
    assert_eq!(problem.code(), crate::codes::INPUT_UNREADABLE);

    // Without the failure, the first row has no form and is counted, and the second is written.
    let mut target = Vec::new();
    let written = writer
        .write_compressed_with(&mut target, None, |sink| {
            sink.write_row(&[ValueRef::Boolean(true)])?;
            sink.write_row(&[DataValue::integer(7).as_ref()])
        })
        .expect("the rows write");
    assert_eq!((written.rows, written.dropped), (1, 1));
    assert_eq!(target, b"7\n");
}
