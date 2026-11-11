//! Reading RDF serializations into rows of three or four columns.

use std::io::{BufRead, BufReader, Read};

use oxrdfio::{RdfFormat, RdfParser};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::{Dictionary, LabelScope},
    function::EvaluationContext,
    value::DataValue,
};

use crate::{
    compression::Compression,
    error::IoError,
    file_format::RdfVariant,
    load::{BlankNodes, Loaded, Reader, RowIntake},
    selection::RowSelection,
};

use super::DEFAULT_GRAPH;

/// The options of an RDF resource for reading.
#[derive(Debug, Clone)]
pub struct RdfReaderOptions {
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

impl RdfReaderOptions {
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
///     rdf::{RdfReaderOptions, RdfReader},
/// };
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext, value::DataValue};
///
/// let triples = "<http://example.org/a> <http://example.org/knows> _:b .\n\
///                _:b <http://example.org/name> \"Bob\" .\n";
/// let reader = RdfReader::new(RdfReaderOptions::new(RdfVariant::NTriples));
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
    options: RdfReaderOptions,
    /// The scope in which blank node labels name nulls,
    /// or `None` if each read has a scope of its own.
    blank_nodes: Option<LabelScope>,
}

impl RdfReader {
    /// Creates a reader with the given options.
    ///
    /// Each read names its blank nodes in a scope of its own,
    /// unless [with_blank_node_scope](Self::with_blank_node_scope) gives a shared scope.
    pub fn new(options: RdfReaderOptions) -> Self {
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
        let width = self.options.variant.width();
        let mut statements = Statements {
            quads: self.options.variant.quads(),
            intake: RowIntake::new(width, &self.options.selection, context),
            blanks: BlankNodes::open(self.blank_nodes, "document", dictionary),
            kept: Vec::with_capacity(width),
        };
        let read = self.read_statements(parser, source, &mut statements, dictionary);
        statements.blanks.close(dictionary);
        read?;

        let loaded = statements.intake.finish();
        tracing::debug!(rows = loaded.rows.len(), counts = ?loaded.counts, "read an RDF source");

        Ok(loaded)
    }
}

impl RdfReader {
    /// Reads the statements of a resource into `statements`,
    /// until the [limit](RdfReaderOptions::limit) of well-formed statements is reached.
    fn read_statements(
        &self,
        parser: RdfParser,
        source: impl Read,
        statements: &mut Statements,
        dictionary: &mut Dictionary,
    ) -> Result<(), Diagnostic> {
        let limit = self.options.limit.unwrap_or(u64::MAX);

        if self.options.variant.line_based() {
            // A statement fits on one line, so a line with a syntax error is one malformed row.
            // None of the line's statements is kept, and the error does not affect the next line.
            let mut source = BufReader::new(source);
            let (mut line, mut quads, mut number) = (Vec::new(), Vec::new(), 0u64);
            while statements.intake.well_formed() < limit {
                line.clear();
                let read = source
                    .read_until(b'\n', &mut line)
                    .map_err(IoError::input_unreadable)?;
                if read == 0 {
                    break;
                }

                number += 1;
                quads.clear();
                let parsed = parser
                    .clone()
                    .for_slice(&line)
                    .all(|quad| quad.map(|quad| quads.push(quad)).is_ok());
                if !parsed {
                    statements.intake.count_malformed(number);
                    continue;
                }

                for quad in quads.drain(..) {
                    if statements.intake.well_formed() >= limit {
                        break;
                    }

                    statements.take(quad, dictionary, number);
                }
            }
        } else {
            for quad in parser.for_reader(source) {
                if statements.intake.well_formed() >= limit {
                    break;
                }

                match quad {
                    Ok(quad) => statements.take(quad, dictionary, 0),
                    Err(oxrdfio::RdfParseError::Syntax(_)) => statements.intake.count_malformed(0),
                    // The stream itself broke, and the error would repeat for
                    // every further statement, so the read ends.
                    Err(oxrdfio::RdfParseError::Io(error)) => {
                        return Err(IoError::input_unreadable(error));
                    }
                }
            }
        }

        Ok(())
    }
}

/// The statements of one read, as they become the rows of its load.
struct Statements {
    /// Whether the statements are quads, whose rows start with the graph.
    quads: bool,
    /// The intake of the read's rows.
    intake: RowIntake,
    /// The blank nodes of the read.
    blanks: BlankNodes,
    /// The delivered values of the current statement.
    kept: Vec<DataValue>,
}

impl Statements {
    /// Takes a statement as a row, or counts it as malformed.
    ///
    /// - IRIs stay as they are, and the default graph becomes [DEFAULT_GRAPH].
    /// - Blank nodes become the nulls that their labels name.
    /// - Literals become their [values](literal_value),
    ///   and a literal without a value makes the statement malformed,
    ///   whether or not its column is delivered.
    ///
    /// `line` is the statement's line, or `0` if it is not known.
    fn take(&mut self, quad: oxrdf::Quad, dictionary: &mut Dictionary, line: u64) {
        let object = match quad.object {
            oxrdf::Term::NamedNode(node) => Some(DataValue::iri(node.into_string())),
            oxrdf::Term::BlankNode(node) => Some(self.blanks.null(node.as_str(), dictionary)),
            oxrdf::Term::Literal(literal) => literal_value(&literal),
        };
        let Some(object) = object else {
            self.intake.count_malformed(line);
            return;
        };

        let graph = self.quads.then(|| match quad.graph_name {
            oxrdf::GraphName::DefaultGraph => DataValue::iri(DEFAULT_GRAPH),
            oxrdf::GraphName::NamedNode(node) => DataValue::iri(node.into_string()),
            oxrdf::GraphName::BlankNode(node) => self.blanks.null(node.as_str(), dictionary),
        });
        let subject = match quad.subject {
            oxrdf::NamedOrBlankNode::NamedNode(node) => DataValue::iri(node.into_string()),
            oxrdf::NamedOrBlankNode::BlankNode(node) => self.blanks.null(node.as_str(), dictionary),
        };
        let predicate = DataValue::iri(quad.predicate.into_string());

        let selection = self.intake.selection();
        self.kept.clear();
        self.kept.extend(
            graph
                .into_iter()
                .chain([subject, predicate, object])
                .enumerate()
                .filter(|(position, _)| selection.keeps(*position))
                .map(|(_, value)| value),
        );
        self.intake.take(&self.kept, dictionary, line);
    }
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
