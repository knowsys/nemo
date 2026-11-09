//! RDF serializations of triples and quads, read and written through `oxrdfio`.
//!
//! A *statement* is one triple, or one quad in the variants with graphs.

use std::io::{BufRead, BufReader, Read};

use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::{Dictionary, LabelScope},
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

/// The IRI that names the default graph in the graph column of quads.
///
/// RDF and SPARQL give the default graph no name,
/// so this IRI stands for it in the rows that are read and written.
pub(crate) const DEFAULT_GRAPH: &str = "tag:nemo:defaultgraph";

impl RdfVariant {
    /// Returns the `oxrdfio` format of the variant.
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

/// The options of an RDF resource for reading.
#[derive(Debug, Clone)]
pub struct RdfOptions {
    /// The serialization variant.
    pub variant: RdfVariant,
    /// The base IRI that relative IRIs are resolved against.
    ///
    /// Turtle, TriG and RDF/XML use it, and it is checked for every variant.
    pub base: Option<String>,
    /// The stream compression.
    pub compression: Compression,
    /// The columns delivered and the rows admitted.
    pub selection: RowSelection,
    /// The largest number of well-formed statements to read, or `None` for no limit.
    ///
    /// The statements are counted before the condition is evaluated,
    /// so the condition never makes a read go further into the resource.
    pub limit: Option<u64>,
}

impl RdfOptions {
    /// Creates options for a variant, with no base, detected compression,
    /// every column delivered, every row admitted, and no limit.
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

/// A reader of RDF statements, which decodes them into row buffers.
///
/// The reader streams, and handles each statement on its own.
/// A statement with a syntax error, a literal that its datatype rejects,
/// or a term that the dictionary cannot hold is counted as malformed and skipped,
/// and parsing resumes with its state, such as the prefix declarations, intact.
/// A stream that cannot be read, such as a compressed stream cut short or a failing device,
/// ends the read with an error instead.
///
/// Blank nodes become nulls, one for each label in the read's label scope.
/// That scope is the read's own, spanning the graph blocks of a TriG document,
/// unless [with_blank_node_scope](Self::with_blank_node_scope) gives a shared one.
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
/// // The blank node is the same fresh null in both statements.
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
    /// The options of the read resources.
    options: RdfOptions,
    /// The scope in which blank node labels name nulls,
    /// or `None` if each read has a scope of its own.
    blank_nodes: Option<LabelScope>,
}

impl RdfReader {
    /// Creates a reader with the given options.
    ///
    /// Each read names its blank nodes in a scope of its own,
    /// unless [with_blank_node_scope](Self::with_blank_node_scope) gives a shared scope.
    pub fn new(options: RdfOptions) -> Self {
        Self {
            options,
            blank_nodes: None,
        }
    }

    /// Returns the reader with its blank nodes named in the given open label scope.
    ///
    /// Reads that share a scope agree on their blank nodes,
    /// as two reads of one document should.
    pub fn with_blank_node_scope(mut self, scope: LabelScope) -> Self {
        self.blank_nodes = Some(scope);
        self
    }

    /// Returns the parser for the variant,
    /// which resolves relative IRIs against the base if one is declared.
    ///
    /// The base is checked for every variant,
    /// although the line-based ones have no relative IRIs and ignore it.
    fn parser(&self) -> Result<RdfParser, Diagnostic> {
        let parser = RdfParser::from_format(self.options.variant.format());
        let Some(base) = &self.options.base else {
            return Ok(parser);
        };
        // The Turtle parser checks a base, while the line-based parsers accept any text.
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
        let blanks = BlankNodes::open(self.blank_nodes, "document", dictionary);
        let mut statements = Statements::new(&self.options, blanks, context);
        let read = self.read_statements(parser, source, &mut statements, dictionary);
        statements.blanks.close(dictionary);
        read?;

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

impl RdfReader {
    /// Reads the statements of a resource into `statements`,
    /// until the [limit](RdfOptions::limit) of well-formed statements is reached.
    fn read_statements(
        &self,
        parser: RdfParser,
        source: impl Read,
        statements: &mut Statements<'_>,
        dictionary: &mut Dictionary,
    ) -> Result<(), Diagnostic> {
        let limit = self.options.limit.unwrap_or(u64::MAX);
        let unreadable = |error: std::io::Error| {
            IoError::StreamUnreadable {
                reason: error.to_string(),
            }
            .diagnostic()
        };

        if self.options.variant.line_based() {
            // A statement fits on one line, so a line with a syntax error is one malformed row.
            // None of the line's statements is kept, and the error does not affect the next line.
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
                    // The stream itself broke, and the error would repeat for
                    // every further statement, so the read ends.
                    Err(oxrdfio::RdfParseError::Io(error)) => return Err(unreadable(error)),
                }
            }
        }
        Ok(())
    }
}

/// The rows of one read, built from its statements.
struct Statements<'a> {
    /// The options of the read resource.
    options: &'a RdfOptions,
    /// The rows loaded so far.
    loaded: Loaded,
    /// The blank nodes of the read.
    blanks: BlankNodes,
    /// The terms of the current statement as values, with `None` for a literal without a value.
    terms: Vec<Option<DataValue>>,
    /// The delivered values of the current statement.
    kept: Vec<DataValue>,
    /// A scratch buffer for the encoded row of the current statement.
    row: Vec<StorageValue>,
}

impl<'a> Statements<'a> {
    /// Creates the empty rows of a read with the given options.
    ///
    /// `blanks` are the read's blank nodes,
    /// and `context` is the evaluation context of the selection's condition.
    fn new(options: &'a RdfOptions, blanks: BlankNodes, context: EvaluationContext) -> Self {
        let arity = options.variant.arity();
        Self {
            options,
            loaded: Loaded::new(options.selection.arity(arity), context),
            blanks,
            terms: Vec::with_capacity(arity),
            kept: Vec::with_capacity(arity),
            row: Vec::with_capacity(arity),
        }
    }

    /// Adds a statement to the rows, or counts it as malformed.
    ///
    /// `line` is the statement's line, or `0` if it is not known.
    fn take(&mut self, quad: oxrdf::Quad, dictionary: &mut Dictionary, line: u64) {
        let selection = &self.options.selection;
        statement(
            quad,
            self.options.variant.quads(),
            &self.blanks,
            dictionary,
            &mut self.terms,
        );
        // Every column decides whether the statement is well-formed,
        // including the columns that are not delivered.
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

/// Writes the terms of a statement into `terms` as values,
/// with the graph first if `quads` is true.
///
/// - IRIs stay as they are, and the default graph becomes [DEFAULT_GRAPH].
/// - Blank nodes become the nulls that their labels name in `blanks`.
/// - Literals become their [values](literal_value),
///   and a literal without a value becomes `None`.
fn statement(
    quad: oxrdf::Quad,
    quads: bool,
    blanks: &BlankNodes,
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

/// Returns the value of an RDF literal, or `None` for an invalid literal.
///
/// - Plain and `xsd:string` literals become plain strings, since RDF 1.1 makes them one value.
/// - Language-tagged literals keep their tag.
/// - Typed literals are normalized as [typed_literal](DataValue::typed_literal) does.
fn literal_value(literal: &oxrdf::Literal) -> Option<DataValue> {
    if let Some(language) = literal.language() {
        return DataValue::language_string(literal.value(), language);
    }
    DataValue::typed_literal(literal.datatype().as_str(), literal.value())
}

/// The options of an RDF resource for writing.
#[derive(Debug, Clone)]
pub struct RdfWriterOptions {
    /// The serialization variant.
    pub variant: RdfVariant,
    /// The base IRI, written into the serializations that can declare one.
    pub base: Option<String>,
    /// Prefix declarations, written into the serializations that abbreviate IRIs:
    /// Turtle, TriG and RDF/XML.
    pub prefixes: Vec<(String, String)>,
    /// The stream compression.
    pub compression: Compression,
}

impl RdfWriterOptions {
    /// Creates options for a variant, with no base, no prefixes, and
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

/// A writer of RDF statements.
///
/// Its rows have three columns, or four for the quad variants, with the graph *first*.
/// Values are written as terms:
/// - IRIs as named nodes, and nulls as blank nodes labeled by their numbers,
/// - strings and language strings as literals,
/// - every other scalar as a typed literal in its canonical lexical form.
///
/// A row with a value that has no term in its position, such as a predicate that is not an IRI,
/// a literal subject, a tuple or a map, is dropped and counted.
#[derive(Debug, Clone)]
pub struct RdfWriter {
    /// The options of the written resources.
    options: RdfWriterOptions,
}

impl RdfWriter {
    /// Creates a writer with the given options.
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
    /// Panics if a row does not have the variant's [arity](RdfVariant::arity).
    fn write<'a>(
        &self,
        target: impl std::io::Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let mut serializer = RdfSerializer::from_format(self.options.variant.format());
        for (name, iri) in &self.options.prefixes {
            // A prefix that the serialization cannot declare does not
            // fail the write. A format without abbreviations ignores
            // all prefixes.
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

/// Returns the node of a value in the subject or graph position.
///
/// An IRI becomes a named node, and a null a blank node labeled by its number.
/// Returns `None` for an invalid IRI and for any other value.
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

/// Returns the node of a value in the predicate position,
/// or `None` if the value is not a valid IRI.
fn predicate_node(value: &DataValue) -> Option<oxrdf::NamedNode> {
    match value.as_ref() {
        ValueRef::Iri(iri) => oxrdf::NamedNode::new(iri.to_string()).ok(),
        _ => None,
    }
}

/// Returns the graph name of a value in the graph position, as [subject_node] does for nodes.
///
/// [DEFAULT_GRAPH] becomes the default graph.
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

/// Returns the term of a value in the object position:
/// a node as in the subject position, or a literal for any other scalar.
///
/// Returns `None` for tuples and maps, which have no RDF form.
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

/// Returns the datatype IRI of a value of a native datatype, which is its domain's datatype.
fn native_datatype(value: &DataValue) -> &'static str {
    value
        .domain()
        .datatype_iri()
        .expect("a value of a native datatype has one")
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Reads a resource on a thread of its own, and fails the test if the read does not return,
    /// as could happen for a stream that keeps failing.
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

    /// The limit counts statements before the condition, so a condition reduces the rows of a
    /// read with a limit, and never makes it read further into the resource.
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
            let mut options = RdfOptions::new(variant);
            options.base = Some(String::from("not an iri"));
            let problem = RdfReader::new(options)
                .read(&b""[..], &mut Dictionary::new(), EvaluationContext::now())
                .expect_err("the base is refused");
            assert_eq!(problem.code(), crate::codes::NOT_A_BASE_IRI, "{variant:?}");
        }
    }
}
