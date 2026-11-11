//! Reading RDF serializations into rows of three or four columns.

use std::io::{BufRead, BufReader, Read};

use oxrdf::vocab::xsd;
use oxrdfio::{RdfFormat, RdfParseError, RdfParser};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::{Dictionary, LabelScope},
    function::EvaluationContext,
    storage::StorageValue,
    value::DataValue,
};

use crate::{
    compression::Compression,
    error::IoError,
    file_format::RdfVariant,
    load::{
        BlankNodes, Loaded, Reader, RowIntake,
        run::{Run, RunPool},
    },
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
/// It reads on the calling thread,
/// while a [batch](super::RdfBatch) parses large resources on several threads.
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

    /// Returns the options of the read resources.
    pub fn options(&self) -> &RdfReaderOptions {
        &self.options
    }

    /// Returns the parser for the variant,
    /// which resolves relative IRIs against the base if one is declared.
    ///
    /// The base is checked for every variant,
    /// although the line-based ones have no relative IRIs and ignore it.
    ///
    /// # Errors
    ///
    /// Returns an error if the base is not an IRI.
    pub(crate) fn parser(&self) -> Result<RdfParser, Diagnostic> {
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

    /// Reads all statements of a resource, interning them as they are parsed.
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
        let runs = RunPool::default();
        let mut load = self.start(dictionary, context);
        let parsed = self.parse(parser, source, &runs, context, &mut |run| {
            load.intern(&run, dictionary, 0);
            runs.give(run);

            true
        });
        let loaded = load.finish(dictionary);
        parsed?;

        tracing::debug!(rows = loaded.rows.len(), counts = ?loaded.counts, "read an RDF source");

        Ok(loaded)
    }
}

impl RdfReader {
    /// Starts the load of a resource, which opens the scope of its blank nodes.
    ///
    /// `context` is the evaluation context of the selection's condition.
    pub(crate) fn start(&self, dictionary: &mut Dictionary, context: EvaluationContext) -> RdfLoad {
        let width = self.options.variant.width();

        RdfLoad {
            intake: RowIntake::new(width, &self.options.selection, context),
            blanks: Some(BlankNodes::open(self.blank_nodes, "document", dictionary)),
            row: Vec::with_capacity(width),
        }
    }

    /// Parses the statements of a resource, or of a part of one that begins and ends at line
    /// ends, into runs taken from `runs`, and passes each run to `emit` once it is full,
    /// until `emit` returns `false`.
    ///
    /// `parser` is the [parser](Self::parser) of the variant,
    /// and `context` is the evaluation context of the selection's condition.
    /// The parse ends at the [limit](RdfReaderOptions::limit) of well-formed statements.
    /// The last run has the number of lines that the parse consumed,
    /// which only the line-based variants count.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be read.
    pub(crate) fn parse(
        &self,
        parser: RdfParser,
        source: impl Read,
        runs: &RunPool<Run>,
        context: EvaluationContext,
        emit: &mut dyn FnMut(Run) -> bool,
    ) -> Result<(), Diagnostic> {
        let limit = self.options.limit.unwrap_or(u64::MAX);
        let arity = self.options.selection.arity(self.options.variant.width());
        let mut run = Run::from_pool(runs, arity);

        // The well-formed statements of the runs passed on so far, which the limit counts.
        let mut passed = 0;

        if self.options.variant.line_based() {
            // A statement fits on one line, so a line with a syntax error is one malformed row.
            // None of the line's statements is kept, and the error does not affect the next line.
            let mut source = BufReader::new(source);
            let (mut line, mut quads, mut number) = (Vec::new(), Vec::new(), 0u64);
            while passed + run.counts.well_formed < limit {
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
                    run.counts.count_malformed(number);
                    continue;
                }

                for quad in quads.drain(..) {
                    if passed + run.counts.well_formed >= limit {
                        break;
                    }

                    self.push_statement(&mut run, context, &quad, number);
                }

                if run.is_full() {
                    passed += run.counts.well_formed;
                    if !emit(std::mem::replace(&mut run, Run::from_pool(runs, arity))) {
                        return Ok(());
                    }
                }
            }

            run.consumed_lines = number;
        } else {
            for quad in parser.for_reader(source) {
                if passed + run.counts.well_formed >= limit {
                    break;
                }

                match quad {
                    Ok(quad) => self.push_statement(&mut run, context, &quad, 0),
                    Err(RdfParseError::Syntax(_)) => run.counts.count_malformed(0),
                    // The stream itself broke, and the error would repeat for
                    // every further statement, so the read ends.
                    Err(RdfParseError::Io(error)) => {
                        return Err(IoError::input_unreadable(error));
                    }
                }

                if run.is_full() {
                    passed += run.counts.well_formed;
                    if !emit(std::mem::replace(&mut run, Run::from_pool(runs, arity))) {
                        return Ok(());
                    }
                }
            }
        }

        emit(run);

        Ok(())
    }

    /// Adds a statement to a run as a row, or counts it as malformed.
    ///
    /// - IRIs stay as they are, and the default graph becomes [DEFAULT_GRAPH].
    /// - Blank nodes become the nulls that their labels name in the resource.
    /// - Literals become their [values](literal_value),
    ///   and a literal without a value makes the statement malformed,
    ///   whether or not its column is delivered.
    ///
    /// `line` is the statement's line, or `0` if it is not known,
    /// and `context` is the evaluation context of the selection's condition.
    fn push_statement(
        &self,
        run: &mut Run,
        context: EvaluationContext,
        quad: &oxrdf::Quad,
        line: u64,
    ) {
        let selection = &self.options.selection;
        let start = run.values.row_start();
        let values = &mut run.values;

        // The rows of quads start with the graph.
        let mut position = 0;
        if self.options.variant.quads() {
            if selection.keeps(position) {
                match &quad.graph_name {
                    oxrdf::GraphName::DefaultGraph => values.push_iri(DEFAULT_GRAPH),
                    oxrdf::GraphName::NamedNode(node) => values.push_iri(node.as_str()),
                    oxrdf::GraphName::BlankNode(node) => values.push_labeled(node.as_str()),
                }
            }

            position += 1;
        }

        if selection.keeps(position) {
            match &quad.subject {
                oxrdf::NamedOrBlankNode::NamedNode(node) => values.push_iri(node.as_str()),
                oxrdf::NamedOrBlankNode::BlankNode(node) => values.push_labeled(node.as_str()),
            }
        }

        if selection.keeps(position + 1) {
            values.push_iri(quad.predicate.as_str());
        }

        let delivered = selection.keeps(position + 2);
        match &quad.object {
            oxrdf::Term::NamedNode(node) if delivered => values.push_iri(node.as_str()),
            oxrdf::Term::BlankNode(node) if delivered => values.push_labeled(node.as_str()),
            oxrdf::Term::NamedNode(_) | oxrdf::Term::BlankNode(_) => {}

            // A plain string is its text, and needs no owned value.
            oxrdf::Term::Literal(literal) if literal.datatype() == xsd::STRING => {
                if delivered {
                    values.push_string(literal.value());
                }
            }
            oxrdf::Term::Literal(literal) => match literal_value(literal) {
                Some(value) if delivered => values.push_value(value),
                Some(_) => {}
                None => {
                    values.take_back(start);
                    run.counts.count_malformed(line);
                    return;
                }
            },
        }

        run.end_row(start, selection.condition.as_ref(), context, line);
    }
}

/// The load of one RDF resource while its runs are interned.
#[derive(Debug)]
pub(crate) struct RdfLoad {
    /// The intake of the resource's rows.
    intake: RowIntake,
    /// The blank nodes of the resource, which are open from the start of the load.
    blanks: Option<BlankNodes>,
    /// A scratch buffer for the encoded values of a row.
    row: Vec<StorageValue>,
}

impl RdfLoad {
    /// Interns the rows of a run, and adds the run's counts to the load.
    ///
    /// `base` is the number of lines before the part of the resource that the run was parsed
    /// from, which is added to the run's line numbers.
    pub(crate) fn intern(&mut self, run: &Run, dictionary: &mut Dictionary, base: u64) {
        run.intern(
            &mut self.intake,
            &mut self.blanks,
            dictionary,
            &mut self.row,
            base,
        );
    }

    /// Ends the load, which closes the scope of the blank nodes if the load opened it,
    /// and returns it.
    pub(crate) fn finish(self, dictionary: &mut Dictionary) -> Loaded {
        if let Some(blanks) = self.blanks {
            blanks.close(dictionary);
        }

        self.intake.finish()
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
