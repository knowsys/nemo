//! Tests of reading and writing RDF serializations.

use std::io::Read;

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    value::{DataValue, ValueRef},
};

use crate::{file_format::RdfVariant, load::Reader, testing::greater_than, write::Writer};

use super::{DEFAULT_GRAPH, RdfReader, RdfReaderOptions, RdfWriter, RdfWriterOptions};

/// Rows written as Turtle are read back as the same rows,
/// and the output uses the declared prefix.
#[test]
fn written_turtle_reads_back() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![
            DataValue::iri("http://example.org/a"),
            DataValue::iri("http://example.org/p"),
            DataValue::integer(42),
        ],
        vec![
            DataValue::null(7),
            DataValue::iri("http://example.org/p"),
            DataValue::language_string("Foo", "en").expect("valid tag"),
        ],
        // This row has no RDF form, so it is dropped and counted.
        vec![
            DataValue::string("not a subject"),
            DataValue::iri("http://example.org/p"),
            DataValue::integer(0),
        ],
    ];
    let mut options = RdfWriterOptions::new(RdfVariant::Turtle);
    options.prefixes = vec![(String::from("ex"), String::from("http://example.org/"))];
    let mut target = Vec::new();
    let written = RdfWriter::new(options)
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!((written.rows, written.dropped), (2, 1));
    let text = String::from_utf8(target.clone()).expect("text");
    assert!(text.contains("@prefix ex:"), "prefixes are written: {text}");
    assert!(text.contains("ex:a"), "IRIs abbreviate: {text}");

    let mut dictionary = Dictionary::new();
    let loaded = RdfReader::new(RdfReaderOptions::new(RdfVariant::Turtle))
        .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
        .expect("the output reads back");
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2, 0));
    let read = loaded.decoded(&dictionary);
    assert_eq!(read[0][0], DataValue::iri("http://example.org/a"));
    assert_eq!(read[0][2], DataValue::integer(42));
    assert!(
        matches!(read[1][0].as_ref(), ValueRef::Null(_)),
        "the blank node reads back as a null"
    );
}

/// Reads a resource on a thread of its own, and fails the test if the read does not return,
/// as could happen for a stream that keeps failing.
fn read_or_hang(
    variant: RdfVariant,
    source: impl Read + Send + 'static,
) -> Result<usize, Diagnostic> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut dictionary = Dictionary::new();
        let result = RdfReader::new(RdfReaderOptions::new(variant))
            .read(source, &mut dictionary, EvaluationContext::now())
            .map(|loaded| loaded.rows.len());
        sender.send(result).ok();
    });
    receiver
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("the read returns")
}

/// A stream that breaks partway ends the read with an error,
/// rather than an endless series of malformed statements.
///
/// The broken streams are a gzip member cut short, and plain bytes under a gzip decoder.
#[test]
fn a_broken_stream_ends_the_read() {
    use std::io::Write;

    use flate2::read::MultiGzDecoder;

    let mut compressed = Vec::new();
    {
        let mut encoder =
            flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::default());
        for index in 0..2000 {
            writeln!(
                encoder,
                "<http://a/{index}> <http://b> <http://c/{index}> ."
            )
            .expect("writes");
        }

        encoder.finish().expect("finishes");
    }
    let cut = compressed[..compressed.len() / 2].to_vec();
    let problem = read_or_hang(
        RdfVariant::Turtle,
        MultiGzDecoder::new(std::io::Cursor::new(cut)),
    )
    .expect_err("a cut stream is an error");
    assert_eq!(problem.code(), crate::codes::INPUT_UNREADABLE);

    let plain = b"<http://a> <http://b> <http://c> .\n".to_vec();
    let problem = read_or_hang(
        RdfVariant::NTriples,
        MultiGzDecoder::new(std::io::Cursor::new(plain)),
    )
    .expect_err("plain bytes are no gzip stream");
    assert_eq!(problem.code(), crate::codes::INPUT_UNREADABLE);
}

/// A line of N-Triples with a syntax error is one malformed row.
///
/// None of the line's statements is kept, and the error ends with the line.
#[test]
fn a_malformed_line_is_one_malformed_row() {
    let triples = "<http://x/s1> <http://x/p> \"a\" \"b\" .\n\
                       <http://x/s2> <http://x/p> \"unterminated .\n\
                       <http://x/s3> <http://x/p> <http://x/o>\n\
                       <http://x/s4> <http://x/p> <http://x/o> .\n\
                       # a comment\n\
                       \n\
                       <http://x/s5> <http://x/p> <http://x/o> .";
    let mut dictionary = Dictionary::new();
    let loaded = RdfReader::new(RdfReaderOptions::new(RdfVariant::NTriples))
        .read(
            triples.as_bytes(),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the triples read");
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2, 3));
    assert_eq!(loaded.counts.first_malformed, Some(1));
    let subjects: Vec<DataValue> = loaded
        .decoded(&dictionary)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(
        subjects,
        [DataValue::iri("http://x/s4"), DataValue::iri("http://x/s5")]
    );
}

/// The limit counts statements before the condition, so a condition reduces the rows of a
/// read with a limit, and never makes it read further into the resource.
#[test]
fn the_limit_counts_statements_before_the_condition() {
    let text: String = (1..=5)
        .map(|index| format!("<http://a> <http://b> {index} .\n"))
        .collect();
    let mut options = RdfReaderOptions::new(RdfVariant::Turtle);
    options.selection.condition = Some(greater_than(2, 2));
    options.limit = Some(3);
    let mut dictionary = Dictionary::new();
    let loaded = RdfReader::new(options)
        .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    assert_eq!((loaded.rows.len(), loaded.counts.excluded), (1, 2));
    assert_eq!(
        dictionary.decode(loaded.rows.row(0)[2]).expect("decodes"),
        DataValue::integer(3).as_ref()
    );
}

/// Quads are written with the graph column first,
/// and [DEFAULT_GRAPH] becomes the default graph.
#[test]
fn quads_write_with_their_graph() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![
            DataValue::iri(DEFAULT_GRAPH),
            DataValue::iri("http://example.org/a"),
            DataValue::iri("http://example.org/p"),
            DataValue::string("x"),
        ],
        vec![
            DataValue::iri("http://example.org/g"),
            DataValue::iri("http://example.org/a"),
            DataValue::iri("http://example.org/p"),
            DataValue::string("y"),
        ],
    ];
    let mut target = Vec::new();
    let written = RdfWriter::new(RdfWriterOptions::new(RdfVariant::NQuads))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(written.rows, 2);
    let text = String::from_utf8(target).expect("text");
    assert!(
        text.lines()
            .next()
            .is_some_and(|line| !line.contains("<tag:")),
        "the sentinel maps to the default graph: {text}"
    );
    assert!(text.contains("<http://example.org/g>"), "{text}");
}

/// A base that is not an absolute IRI is rejected for every variant,
/// including the line-based ones, which have no use for it.
#[test]
fn every_variant_refuses_a_base_that_is_not_an_iri() {
    for variant in [RdfVariant::NTriples, RdfVariant::NQuads, RdfVariant::Turtle] {
        let mut options = RdfReaderOptions::new(variant);
        options.base = Some(String::from("not an iri"));
        let problem = RdfReader::new(options)
            .read(&b""[..], &mut Dictionary::new(), EvaluationContext::now())
            .expect_err("the base is refused");
        assert_eq!(problem.code(), crate::codes::NOT_A_BASE_IRI, "{variant:?}");
    }
}
