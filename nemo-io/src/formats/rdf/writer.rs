//! Writing rows of three or four columns as RDF serializations.

use std::{fmt, io::Write};

use oxrdfio::{RdfSerializer, WriterQuadSerializer};

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{IriRef, ValueRef},
};

use crate::{
    compression::Compression,
    error::IoError,
    file_format::RdfVariant,
    write::{RowSink, Writer, Written},
};

use super::DEFAULT_GRAPH;

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
    type Sink<W: Write> = RdfSink<W>;

    fn compression(&self) -> Compression {
        self.options.compression
    }

    fn start<W: Write>(&self, target: W) -> Result<RdfSink<W>, Diagnostic> {
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

        Ok(RdfSink {
            quads: self.options.variant.quads(),
            width: self.options.variant.width(),
            writer: serializer.for_writer(target),
            written: Written::default(),
        })
    }
}

/// The sink of the statements of one RDF resource, which writes each row as one statement.
pub struct RdfSink<W: Write> {
    /// Whether the statements are quads, whose rows start with the graph.
    quads: bool,
    /// The number of columns of a row.
    width: usize,
    /// The serializer of the statements.
    writer: WriterQuadSerializer<W>,
    /// The counts of the rows written and dropped so far.
    written: Written,
}

impl<W: Write> fmt::Debug for RdfSink<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RdfSink")
            .field("quads", &self.quads)
            .field("width", &self.width)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl<W: Write> RowSink for RdfSink<W> {
    /// Writes a row as one statement, or drops it if RDF does not allow one of its values in
    /// its position.
    ///
    /// # Panics
    ///
    /// Panics if the row does not have the variant's [width](RdfVariant::width).
    fn write_row(&mut self, row: &[ValueRef<'_>]) -> Result<(), Diagnostic> {
        assert_eq!(
            row.len(),
            self.width,
            "a row of an RDF export has the variant's number of columns"
        );

        let (graph, statement) = row.split_at(usize::from(self.quads));
        let graph_name = match graph.first() {
            None => Some(oxrdf::GraphName::DefaultGraph),
            Some(&graph) => graph_node(graph),
        };
        let quad = graph_name
            .zip(subject_node(statement[0]))
            .zip(predicate_node(statement[1]))
            .zip(object_term(statement[2]))
            .map(|(((graph_name, subject), predicate), object)| oxrdf::Quad {
                subject,
                predicate,
                object,
                graph_name,
            });
        match quad {
            Some(quad) => {
                self.writer
                    .serialize_quad(&quad)
                    .map_err(IoError::output_unwritable)?;
                self.written.rows += 1;
            }
            None => self.written.dropped += 1,
        }

        Ok(())
    }

    fn finish(self) -> Result<Written, Diagnostic> {
        self.writer
            .finish()
            .and_then(|mut target| target.flush())
            .map_err(IoError::output_unwritable)?;

        Ok(self.written)
    }
}

/// Returns the node of a value in the subject or graph position.
///
/// An IRI becomes a named node, and a null a blank node labeled by its number.
/// Returns `None` for an invalid IRI and for any other value.
fn subject_node(value: ValueRef<'_>) -> Option<oxrdf::NamedOrBlankNode> {
    match value {
        ValueRef::Iri(iri) => oxrdf::NamedNode::new(iri.to_string()).map(Into::into).ok(),
        ValueRef::Null(_) => Some(
            oxrdf::BlankNode::new(format!("n{}", value.null_number().unwrap_or_default()))
                .expect("a numbered label is valid")
                .into(),
        ),
        _ => None,
    }
}

/// Returns the node of a value in the predicate position,
/// or `None` if the value is not a valid IRI.
fn predicate_node(value: ValueRef<'_>) -> Option<oxrdf::NamedNode> {
    match value {
        ValueRef::Iri(iri) => oxrdf::NamedNode::new(iri.to_string()).ok(),
        _ => None,
    }
}

/// Returns the graph name of a value in the graph position, as [subject_node] does for nodes.
///
/// [DEFAULT_GRAPH] becomes the default graph.
fn graph_node(value: ValueRef<'_>) -> Option<oxrdf::GraphName> {
    if matches!(value, ValueRef::Iri(iri) if iri == IriRef::new(DEFAULT_GRAPH)) {
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
pub(crate) fn object_term(value: ValueRef<'_>) -> Option<oxrdf::Term> {
    let typed = |lexical: String, datatype: &str| -> Option<oxrdf::Term> {
        oxrdf::NamedNode::new(datatype)
            .ok()
            .map(|datatype| oxrdf::Literal::new_typed_literal(lexical, datatype).into())
    };
    match value {
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
fn native_datatype(value: ValueRef<'_>) -> &'static str {
    value
        .domain()
        .datatype_iri()
        .expect("a value of a native datatype has one")
}
