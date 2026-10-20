//! RDF serializations: triples and quads, through one parser and one serializer front-end.

use std::io::{BufRead, BufReader, Read};

use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    value::{DataValue, IriRef, ValueRef},
};

use crate::{
    error::IoError,
    load::{Loaded, Reader},
    selection::RowSelection,
    stream::compression::Compression,
    term::BlankNodes,
    written::{Writer, Written},
};

use crate::file_format::RdfVariant;

/// The IRI naming the default graph of a quad resource.
///
/// SPARQL provides no standard identifier, so this ad-hoc one is used for compatibility.
pub(crate) const DEFAULT_GRAPH: &str = "tag:nemo:defaultgraph";

impl RdfVariant {
    /// Returns the parser format behind the variant.
    fn format(self) -> RdfFormat {
        match self {
            RdfVariant::NTriples => RdfFormat::NTriples,
            RdfVariant::NQuads => RdfFormat::NQuads,
            RdfVariant::Turtle => RdfFormat::Turtle,
            RdfVariant::TriG => RdfFormat::TriG,
            RdfVariant::RdfXml => RdfFormat::RdfXml,
        }
    }
}

/// The shape of an RDF resource.
#[derive(Debug, Clone)]
pub struct RdfOptions {
    /// The serialization variant.
    pub variant: RdfVariant,
    /// The base IRI relative references resolve against (effective for
    /// Turtle, TriG, and RDF/XML, and checked for every variant).
    pub base: Option<String>,
    /// The stream compression.
    pub compression: Compression,
    /// The columns delivered and the rows admitted.
    pub selection: RowSelection,
    /// The number of well-formed statements matching the declared types the reader takes before
    /// it stops.
    ///
    /// The statements are counted before the condition,
    /// so a condition never changes which rows a limited resource yields.
    pub limit: Option<u64>,
}

impl RdfOptions {
    /// Options for a variant, with no base, auto-detected compression,
    /// and every column and row delivered.
    pub fn new(variant: RdfVariant) -> Self {
        Self {
            variant,
            base: None,
            compression: Compression::Auto,
            selection: RowSelection::default(),
            limit: None,
        }
    }
}

/// A reader of RDF statements, decoding them into the physical layer's row buffers.
///
/// Reading streams, and is lenient per statement:
/// a syntax error or a term the dictionary cannot hold skips that statement, counted,
/// and parsing resumes with its state (prefix bindings) intact.
/// A stream that cannot be read, a compressed member cut short or a failing device,
/// ends the read with an error instead.
/// Blank nodes become fresh labeled nulls, one per distinct label per read
/// (document scope, spanning TriG graph blocks).
///
/// # Example
///
/// ```
/// use nemo_io::{
///     file_format::RdfVariant,
///     load::Reader,
///     rdf::{RdfOptions, RdfReader},
/// };
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext, value::DataValue};
///
/// let triples = "<http://example.org/a> <http://example.org/knows> _:b .\n\
///                _:b <http://example.org/name> \"Bob\" .\n";
/// let reader = RdfReader::new(RdfOptions::new(RdfVariant::NTriples));
/// let mut dictionary = Dictionary::new();
/// let loaded = reader
///     .read(triples.as_bytes(), &mut dictionary, EvaluationContext::now())
///     .expect("the triples read");
/// assert_eq!(loaded.rows.len(), 2);
///
/// // The blank node is one fresh null in both statements.
/// let object = dictionary.decode(loaded.rows.row(0)[2]).expect("stored");
/// let subject = dictionary.decode(loaded.rows.row(1)[0]).expect("stored");
/// assert_eq!(object, subject);
/// assert_eq!(
///     dictionary.decode(loaded.rows.row(1)[2]).map(|value| value.to_owned()),
///     Some(DataValue::string("Bob"))
/// );
/// ```
#[derive(Debug, Clone)]
pub struct RdfReader {
    /// The resource shape.
    options: RdfOptions,
}

impl RdfReader {
    /// Creates a reader for resources of the given shape.
    pub fn new(options: RdfOptions) -> Self {
        Self { options }
    }

    /// Returns the parser for the variant,
    /// resolving relative references against the base if one is declared.
    ///
    /// The base is checked for every variant,
    /// although the line-based ones have no relative references and ignore it.
    fn parser(&self) -> Result<RdfParser, Diagnostic> {
        let parser = RdfParser::from_format(self.options.variant.format());
        let Some(base) = &self.options.base else {
            return Ok(parser);
        };
        // The Turtle parser checks a base where the line-based parsers take any text.
        RdfParser::from_format(RdfFormat::Turtle)
            .with_base_iri(base.clone())
            .and_then(|_| parser.with_base_iri(base.clone()))
            .map_err(|error| {
                IoError::NotABaseIri {
                    base: base.clone(),
                    reason: error.to_string(),
                }
                .diagnostic()
            })
    }
}

impl Reader for RdfReader {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Reads all statements of a resource, interning values as they arrive.
    ///
    /// # Errors
    ///
    /// Returns an error if the base is not an IRI or the stream cannot be read.
    fn read(
        &self,
        source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let parser = self.parser()?;
        let mut statements = Statements::new(&self.options, context);
        let limit = self.options.limit.unwrap_or(u64::MAX);
        let unreadable = |error: std::io::Error| {
            IoError::StreamUnreadable {
                reason: error.to_string(),
            }
            .diagnostic()
        };

        if self.options.variant.line_based() {
            // A statement takes one line, so a line with a syntax error is one malformed row,
            // and none of its statements is taken, nor does the error reach the next line.
            let mut source = BufReader::new(source);
            let (mut line, mut quads, mut number) = (Vec::new(), Vec::new(), 0u64);
            while statements.loaded.well_formed < limit {
                line.clear();
                if source.read_until(b'\n', &mut line).map_err(unreadable)? == 0 {
                    break;
                }
                number += 1;
                quads.clear();
                let parsed = parser
                    .clone()
                    .for_slice(&line)
                    .all(|quad| quad.map(|quad| quads.push(quad)).is_ok());
                if !parsed {
                    statements.loaded.count_malformed(number);
                    continue;
                }
                for quad in quads.drain(..) {
                    if statements.loaded.well_formed >= limit {
                        break;
                    }
                    statements.take(quad, dictionary, number);
                }
            }
        } else {
            for quad in parser.for_reader(source) {
                if statements.loaded.well_formed >= limit {
                    break;
                }
                match quad {
                    Ok(quad) => statements.take(quad, dictionary, 0),
                    Err(oxrdfio::RdfParseError::Syntax(_)) => statements.loaded.count_malformed(0),
                    // The stream itself broke: the error would repeat on
                    // every statement from here, so the source ends.
                    Err(oxrdfio::RdfParseError::Io(error)) => return Err(unreadable(error)),
                }
            }
        }

        let loaded = statements.loaded;
        tracing::debug!(
            rows = loaded.rows.len(),
            malformed = loaded.malformed,
            excluded = loaded.excluded,
            errors = ?loaded.errors,
            "read an RDF source"
        );
        Ok(loaded)
    }
}

/// The statements of one read, taken into its rows.
struct Statements<'a> {
    /// The resource shape.
    options: &'a RdfOptions,
    /// The rows so far.
    loaded: Loaded,
    /// The nulls the blank node labels name.
    blanks: BlankNodes,
    /// A statement's terms, as values.
    terms: Vec<Option<DataValue>>,
    /// The delivered values of a statement.
    kept: Vec<DataValue>,
    /// The encoded row of a statement.
    row: Vec<StorageValue>,
}

impl<'a> Statements<'a> {
    /// Creates the empty rows of a read of the given shape,
    /// whose condition evaluates in `context`.
    fn new(options: &'a RdfOptions, context: EvaluationContext) -> Self {
        let arity = options.variant.arity();
        Self {
            options,
            loaded: Loaded::new(options.selection.arity(arity), context),
            blanks: BlankNodes::default(),
            terms: Vec::with_capacity(arity),
            kept: Vec::with_capacity(arity),
            row: Vec::with_capacity(arity),
        }
    }

    /// Takes a statement read on the given line (0 if unknown) into the rows,
    /// or counts it as malformed.
    fn take(&mut self, quad: oxrdf::Quad, dictionary: &mut Dictionary, line: u64) {
        let selection = &self.options.selection;
        statement(
            quad,
            self.options.variant.quads(),
            &mut self.blanks,
            dictionary,
            &mut self.terms,
        );
        // Every column decides whether the statement is well-formed,
        // the ones not delivered included.
        if self.terms.iter().all(Option::is_some) {
            self.kept.clear();
            self.kept.extend(
                self.terms
                    .drain(..)
                    .enumerate()
                    .filter(|(position, _)| selection.keeps(*position))
                    .map(|(_, value)| value.expect("every term has a value")),
            );
            self.loaded
                .take(&self.kept, selection, dictionary, &mut self.row, line);
        } else {
            self.loaded.count_malformed(line);
        }
    }
}

/// Writes a statement's terms onto `terms` as values, the graph first for quad variants.
///
/// IRIs stay as they are, blank nodes become the nulls their labels name,
/// and literals become their [values](literal_value).
/// Returns `None` if a literal has no value.
fn statement(
    quad: oxrdf::Quad,
    quads: bool,
    blanks: &mut BlankNodes,
    dictionary: &mut Dictionary,
    terms: &mut Vec<Option<DataValue>>,
) {
    terms.clear();
    if quads {
        terms.push(Some(match quad.graph_name {
            oxrdf::GraphName::DefaultGraph => DataValue::iri(DEFAULT_GRAPH),
            oxrdf::GraphName::NamedNode(node) => DataValue::iri(node.into_string()),
            oxrdf::GraphName::BlankNode(node) => blanks.null(node.as_str(), dictionary),
        }));
    }
    terms.push(Some(match quad.subject {
        oxrdf::NamedOrBlankNode::NamedNode(node) => DataValue::iri(node.into_string()),
        oxrdf::NamedOrBlankNode::BlankNode(node) => blanks.null(node.as_str(), dictionary),
    }));
    terms.push(Some(DataValue::iri(quad.predicate.into_string())));
    terms.push(match quad.object {
        oxrdf::Term::NamedNode(node) => Some(DataValue::iri(node.into_string())),
        oxrdf::Term::BlankNode(node) => Some(blanks.null(node.as_str(), dictionary)),
        oxrdf::Term::Literal(literal) => literal_value(&literal),
    });
}

/// Returns the value of an RDF literal, or `None` for an invalid lexical.
///
/// Plain and `xsd:string` literals are plain strings, one value as in RDF 1.1.
/// Language-tagged literals keep their tag.
/// Typed literals normalize like [typed_literal](DataValue::typed_literal).
fn literal_value(literal: &oxrdf::Literal) -> Option<DataValue> {
    if let Some(language) = literal.language() {
        return DataValue::language_string(literal.value(), language);
    }
    DataValue::typed_literal(literal.datatype().as_str(), literal.value())
}

/// The shape of an RDF target.
#[derive(Debug, Clone)]
pub struct RdfWriterOptions {
    /// The serialization variant.
    pub variant: RdfVariant,
    /// The base IRI written into serializations that carry one.
    pub base: Option<String>,
    /// Prefix declarations written into the serializations that abbreviate
    /// (Turtle, TriG, RDF/XML).
    pub prefixes: Vec<(String, String)>,
    /// The stream compression.
    pub compression: Compression,
}

impl RdfWriterOptions {
    /// Options for a variant, with no base, no prefixes, and
    /// auto-detected compression.
    pub fn new(variant: RdfVariant) -> Self {
        Self {
            variant,
            base: None,
            prefixes: Vec::new(),
            compression: Compression::Auto,
        }
    }
}

/// A writer of RDF statements, the inverse of [RdfReader].
///
/// Its rows have three columns, or four with the graph *first* for the quad variants.
/// Values map onto terms:
/// IRIs become named nodes, labeled nulls blank nodes (stable within one write),
/// strings literals, and every other scalar a typed literal in its canonical lexical.
/// A row that fits no term (a non-IRI predicate, a tuple, a map) is dropped and counted.
#[derive(Debug, Clone)]
pub struct RdfWriter {
    /// The target shape.
    options: RdfWriterOptions,
}

impl RdfWriter {
    /// Creates a writer for targets of the given shape.
    pub fn new(options: RdfWriterOptions) -> Self {
        Self { options }
    }
}

impl Writer for RdfWriter {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Writes rows to a target.
    ///
    /// # Errors
    ///
    /// Returns an error if the target cannot be written.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the variant's number of columns:
    /// three for triples, four for quads, the graph first.
    fn write<'a>(
        &self,
        target: impl std::io::Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let mut serializer = RdfSerializer::from_format(self.options.variant.format());
        for (name, iri) in &self.options.prefixes {
            // A prefix pair the serialization cannot carry (an
            // abbreviation-free format ignores them wholesale) does
            // not fail the export.
            if let Ok(with) = serializer.clone().with_prefix(name, iri) {
                serializer = with;
            }
        }
        if let Some(base) = &self.options.base
            && let Ok(with) = serializer.clone().with_base_iri(base)
        {
            serializer = with;
        }

        let quads = self.options.variant.quads();
        let arity = self.options.variant.arity();
        let mut writer = serializer.for_writer(target);
        let mut written = Written::default();
        for row in rows {
            assert_eq!(
                row.len(),
                arity,
                "a row of an RDF export has the variant's number of columns"
            );
            let (graph, statement) = row.split_at(if quads { 1 } else { 0 });
            let graph_name = match graph.first() {
                None => Some(oxrdf::GraphName::DefaultGraph),
                Some(graph) => graph_node(graph),
            };
            let quad = graph_name
                .zip(subject_node(&statement[0]))
                .zip(predicate_node(&statement[1]))
                .zip(object_term(&statement[2]))
                .map(|(((graph_name, subject), predicate), object)| oxrdf::Quad {
                    subject,
                    predicate,
                    object,
                    graph_name,
                });
            match quad {
                Some(quad) => {
                    writer
                        .serialize_quad(&quad)
                        .map_err(IoError::stream_unwritable)?;
                    written.rows += 1;
                }
                None => written.dropped += 1,
            }
        }
        writer
            .finish()
            .and_then(|mut target| target.flush())
            .map_err(IoError::stream_unwritable)?;
        Ok(written)
    }
}

/// Returns the node a value is in a subject or graph position:
/// an IRI or a labeled null (a blank node), and `None` for any other value.
fn subject_node(value: &DataValue) -> Option<oxrdf::NamedOrBlankNode> {
    match value.as_ref() {
        ValueRef::Iri(iri) => oxrdf::NamedNode::new(iri.to_string()).map(Into::into).ok(),
        ValueRef::Null(_) => Some(
            oxrdf::BlankNode::new(format!(
                "n{}",
                value.as_ref().null_number().unwrap_or_default()
            ))
            .expect("a numbered label is valid")
            .into(),
        ),
        _ => None,
    }
}

/// Returns the node a value is in the predicate position: an IRI only.
fn predicate_node(value: &DataValue) -> Option<oxrdf::NamedNode> {
    match value.as_ref() {
        ValueRef::Iri(iri) => oxrdf::NamedNode::new(iri.to_string()).ok(),
        _ => None,
    }
}

/// Returns the name a value is in the graph position.
///
/// The [DEFAULT_GRAPH] sentinel maps back to the actual default graph.
fn graph_node(value: &DataValue) -> Option<oxrdf::GraphName> {
    if matches!(
        value.as_ref(),
        ValueRef::Iri(iri) if iri == IriRef::new(DEFAULT_GRAPH)
    ) {
        return Some(oxrdf::GraphName::DefaultGraph);
    }
    subject_node(value).map(|node| match node {
        oxrdf::NamedOrBlankNode::NamedNode(node) => oxrdf::GraphName::NamedNode(node),
        oxrdf::NamedOrBlankNode::BlankNode(node) => oxrdf::GraphName::BlankNode(node),
    })
}

/// Returns the term a value is in the object position:
/// nodes as in the subject position, and every scalar a literal.
///
/// Returns `None` for the structured values, which have no RDF form.
pub(crate) fn object_term(value: &DataValue) -> Option<oxrdf::Term> {
    let typed = |lexical: String, datatype: &str| -> Option<oxrdf::Term> {
        oxrdf::NamedNode::new(datatype)
            .ok()
            .map(|datatype| oxrdf::Literal::new_typed_literal(lexical, datatype).into())
    };
    match value.as_ref() {
        ValueRef::Iri(_) | ValueRef::Null(_) => subject_node(value).map(|node| match node {
            oxrdf::NamedOrBlankNode::NamedNode(node) => oxrdf::Term::NamedNode(node),
            oxrdf::NamedOrBlankNode::BlankNode(node) => oxrdf::Term::BlankNode(node),
        }),
        ValueRef::String(text) => Some(oxrdf::Literal::new_simple_literal(text).into()),
        ValueRef::LanguageString(value) => {
            oxrdf::Literal::new_language_tagged_literal(value.text(), value.language())
                .ok()
                .map(Into::into)
        }
        ValueRef::Boolean(boolean) => typed(boolean.to_string(), native_datatype(value)),
        ValueRef::Number(number) => typed(number.to_string(), native_datatype(value)),
        ValueRef::DateTime(lexical) => typed(lexical.to_string(), native_datatype(value)),
        ValueRef::Date(lexical) => typed(lexical.to_string(), native_datatype(value)),
        ValueRef::Time(lexical) => typed(lexical.to_string(), native_datatype(value)),
        ValueRef::Duration(lexical) => typed(lexical.to_string(), native_datatype(value)),
        ValueRef::Other(literal) => typed(literal.lexical().to_owned(), literal.datatype()),
        ValueRef::Tuple(_) | ValueRef::Map(_) => None,
    }
}

/// Returns the datatype of a value of a native datatype, the one of its domain.
fn native_datatype(value: &DataValue) -> &'static str {
    value
        .domain()
        .datatype_iri()
        .expect("a value of a native datatype has one")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows written as Turtle read back as the same rows,
    /// and carry the declared prefix abbreviation.
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
            // No RDF form: dropped, counted.
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
        let loaded = RdfReader::new(RdfOptions::new(RdfVariant::Turtle))
            .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
            .expect("the output reads back");
        assert_eq!((loaded.rows.len(), loaded.malformed), (2, 0));
        let cell = |row: usize, column: usize| -> DataValue {
            dictionary
                .decode(loaded.rows.row(row)[column])
                .expect("decodes")
                .to_owned()
        };
        assert_eq!(cell(0, 0), DataValue::iri("http://example.org/a"));
        assert_eq!(cell(0, 2), DataValue::integer(42));
        assert!(
            matches!(cell(1, 0).as_ref(), ValueRef::Null(_)),
            "the blank node reads back as a null"
        );
    }

    /// Reads a resource on its own thread, failing the test if the read
    /// never returns, as a stream that keeps erroring could make it.
    fn read_or_hang(
        variant: RdfVariant,
        source: impl Read + Send + 'static,
    ) -> Result<usize, Diagnostic> {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut dictionary = Dictionary::new();
            let result = RdfReader::new(RdfOptions::new(variant))
                .read(source, &mut dictionary, EvaluationContext::now())
                .map(|loaded| loaded.rows.len());
            sender.send(result).ok();
        });
        receiver
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("the read returns")
    }

    /// A stream that breaks partway ends the read with an error rather
    /// than an endless run of malformed statements: a gzip member cut
    /// short, and plain bytes under a gzip decoder.
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

    /// A line of N-Triples with a syntax error is one malformed row:
    /// none of its terms is taken, and the error ends with the line.
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
        let loaded = RdfReader::new(RdfOptions::new(RdfVariant::NTriples))
            .read(
                triples.as_bytes(),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the triples read");
        assert_eq!((loaded.rows.len(), loaded.malformed), (2, 3));
        assert_eq!(loaded.first_malformed, Some(1));
        let subjects: Vec<DataValue> = (0..loaded.rows.len())
            .map(|row| {
                dictionary
                    .decode(loaded.rows.row(row)[0])
                    .expect("stored")
                    .to_owned()
            })
            .collect();
        assert_eq!(
            subjects,
            [DataValue::iri("http://x/s4"), DataValue::iri("http://x/s5")]
        );
    }

    /// The limit counts statements before the condition, so a pushed
    /// condition reduces what a limited resource yields and never reads
    /// further into it.
    #[test]
    fn the_limit_counts_statements_before_the_condition() {
        use nemo_physical::function::{OpCode, StackProgram};

        let text: String = (1..=5)
            .map(|index| format!("<http://a> <http://b> {index} .\n"))
            .collect();
        let mut options = RdfOptions::new(RdfVariant::Turtle);
        options.selection.filter = Some(crate::row_filter::RowFilter::new(
            vec![2],
            StackProgram::new(
                vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
                vec![DataValue::integer(2)],
                1,
            ),
        ));
        options.limit = Some(3);
        let mut dictionary = Dictionary::new();
        let loaded = RdfReader::new(options)
            .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
            .expect("the source reads");
        assert_eq!((loaded.rows.len(), loaded.excluded), (1, 2));
        assert_eq!(
            dictionary.decode(loaded.rows.row(0)[2]).expect("decodes"),
            DataValue::integer(3).as_ref()
        );
    }

    /// Quads write with the graph column first, the sentinel naming the
    /// default graph.
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

    /// A base that is not an absolute IRI is refused for every variant,
    /// the line-based ones included, which have no use for it.
    #[test]
    fn every_variant_refuses_a_base_that_is_not_an_iri() {
        for variant in [RdfVariant::NTriples, RdfVariant::NQuads, RdfVariant::Turtle] {
            let mut options = RdfOptions::new(variant);
            options.base = Some(String::from("not an iri"));
            let problem = RdfReader::new(options)
                .read(&b""[..], &mut Dictionary::new(), EvaluationContext::now())
                .expect_err("the base is refused");
            assert_eq!(problem.code(), crate::codes::NOT_A_BASE_IRI, "{variant:?}");
        }
    }
}
