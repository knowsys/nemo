//! Writing rows of three or four columns as RDF serializations.

use std::io::Write;

use oxrdfio::RdfSerializer;

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{DataValue, IriRef, ValueRef},
};

use crate::{
    compression::Compression,
    error::IoError,
    file_format::RdfVariant,
    write::{Writer, Written},
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
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Writes each row as one statement.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the variant's [arity](RdfVariant::width).
    fn write<'a>(
        &self,
        target: impl Write,
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
        let arity = self.options.variant.width();
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
                        .map_err(IoError::output_unwritable)?;
                    written.rows += 1;
                }
                None => written.dropped += 1,
            }
        }

        writer
            .finish()
            .and_then(|mut target| target.flush())
            .map_err(IoError::output_unwritable)?;

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
