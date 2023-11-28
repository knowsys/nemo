//! Reading of RDF 1.1 triples files (N-Triples, Turtle, RDF/XML)
use std::io::BufReader;

use thiserror::Error;

use nemo_physical::datasources::{TableProvider, TableWriter};

use nemo_physical::{
    datavalues::{AnyDataValue, DataValueCreationError},
    table_reader::Resource,
};
use oxiri::Iri;
use rio_api::{
    model::{BlankNode, Literal, NamedNode, Subject, Term, Triple},
    parser::TriplesParser,
};
use rio_turtle::{NTriplesParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::{
    io::{formats::PROGRESS_NOTIFY_INCREMENT, resource_providers::ResourceProviders},
    model::RdfFile,
};

/// Errors that can occur when reading RDF resources and converting them
/// to [`AnyDataValue`]s.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum RdfReadingError {
    /// A problem occurred in converting an RDF term to a data value.
    #[error(transparent)]
    DataValueConversion(#[from] DataValueCreationError),
    /// Error of encountering RDF* features in data
    #[error("RDF* terms are not supported")]
    RdfStarUnsupported,
    /// Error in Rio's Turtle parser
    #[error(transparent)]
    RioTurtle(#[from] rio_turtle::TurtleError),
    /// Error in Rio's RDF/XML parser
    #[error(transparent)]
    RioXML(#[from] rio_xml::RdfXmlError),
}

/// A [`TableReader`] for RDF 1.1 files containing triples.
#[derive(Debug, Clone)]
pub struct RDFTriplesReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    base: Option<Iri<String>>,
}

impl RDFTriplesReader {
    /// Create a new [`RDFTriplesReader`]
    pub fn new(resource_providers: ResourceProviders, rdf_file: &RdfFile) -> Self {
        Self {
            resource_providers,
            resource: rdf_file.resource.clone(),
            base: rdf_file
                .base
                .as_ref()
                .cloned()
                .map(|iri| Iri::parse(iri).expect("should be a valid IRI.")),
        }
    }

    fn datavalue_from_named_node(value: NamedNode) -> AnyDataValue {
        AnyDataValue::new_iri(value.iri.to_string())
    }

    /// FIXME: BNode handling must be more sophisticated to be correct.
    fn datavalue_from_blank_node(value: BlankNode) -> AnyDataValue {
        AnyDataValue::new_iri(value.to_string())
    }

    /// Create [`AnyDataValue`] from a [`Literal`].
    pub(crate) fn datavalue_from_literal(
        value: Literal<'_>,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match value {
            Literal::Simple { value } => Ok(AnyDataValue::new_string(value.to_string())),
            Literal::LanguageTaggedString { value, language } => Ok(
                AnyDataValue::new_language_tagged_string(value.to_string(), language.to_string()),
            ),
            Literal::Typed { value, datatype } => Ok(AnyDataValue::new_from_typed_literal(
                value.to_string(),
                datatype.iri.to_string(),
            )?),
        }
    }

    fn datavalue_from_subject(value: Subject<'_>) -> Result<AnyDataValue, RdfReadingError> {
        match value {
            Subject::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Subject::BlankNode(bn) => Ok(Self::datavalue_from_blank_node(bn)),
            Subject::Triple(_t) => Err(RdfReadingError::RdfStarUnsupported),
        }
    }

    fn datavalue_from_term(value: Term<'_>) -> Result<AnyDataValue, RdfReadingError> {
        match value {
            Term::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Term::BlankNode(bn) => Ok(Self::datavalue_from_blank_node(bn)),
            Term::Literal(lit) => Ok(Self::datavalue_from_literal(lit)?),
            Term::Triple(_t) => Err(RdfReadingError::RdfStarUnsupported),
        }
    }

    /// Read the RDF triples from a parser.
    fn read_with_parser<Parser>(
        &self,
        table_writer: &mut TableWriter,
        make_parser: impl FnOnce() -> Parser,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Parser: TriplesParser,
    {
        assert_eq!(table_writer.column_number(), 3);

        let mut triple_count = 0;

        let mut on_triple = |triple: Triple| {
            let subject = Self::datavalue_from_subject(triple.subject)?;
            let predicate = Self::datavalue_from_named_node(triple.predicate);
            let object = Self::datavalue_from_term(triple.object)?;

            table_writer.next_value(subject);
            table_writer.next_value(predicate);
            table_writer.next_value(object);

            triple_count += 1;
            if triple_count % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {triple_count} triples")
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser();

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed triple: {e}");
            }
        }

        log::info!("Finished loading: processed {triple_count} triples");

        Ok(())
    }
}

impl TableProvider for RDFTriplesReader {
    fn provide_table_data(
        self: Box<Self>,
        table_writer: &mut TableWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let reader = BufReader::new(reader);

        if self.resource.ends_with(".ttl.gz") || self.resource.ends_with(".ttl") {
            self.read_with_parser(table_writer, || {
                TurtleParser::new(reader, self.base.clone())
            })
        } else if self.resource.ends_with(".rdf.gz") || self.resource.ends_with(".rdf") {
            self.read_with_parser(table_writer, || {
                RdfXmlParser::new(reader, self.base.clone())
            })
        } else {
            self.read_with_parser(table_writer, || NTriplesParser::new(reader))
        }
    }
}

#[cfg(test)]
mod test {
    use super::RDFTriplesReader;
    use crate::{io::resource_providers::ResourceProviders, model::RdfFile};

    use nemo_physical::{datasources::TableWriter, management::database::Dict};
    use rio_turtle::{NTriplesParser, TurtleParser};
    use std::cell::RefCell;
    use test_log::test;

    #[test]
    fn parse_triples_nt() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object1> . # comments here
        # or on a line by themselves
        _:bnode1 <http://an.example/predicate1> "string1" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" . # Note that duplicate triples are not eliminated here yet
        "#.as_bytes();

        let reader = RDFTriplesReader::new(ResourceProviders::empty(), &RdfFile::new("", None));
        let dict = RefCell::new(Dict::default());
        let mut table_writer = TableWriter::new(&dict, 3);
        let result = reader.read_with_parser(&mut table_writer, || NTriplesParser::new(data));
        assert!(result.is_ok());
        assert_eq!(table_writer.size(), 4);
    }

    #[test]
    fn parse_triples_turtle() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1>
            [ <http://example.org/predicate2> 42, "test" ] .
        "#
        .as_bytes();

        let reader = RDFTriplesReader::new(ResourceProviders::empty(), &RdfFile::new("", None));
        let dict = RefCell::new(Dict::default());
        let mut table_writer = TableWriter::new(&dict, 3);
        let result = reader.read_with_parser(&mut table_writer, || TurtleParser::new(data, None));
        assert!(result.is_ok());
        assert_eq!(table_writer.size(), 3);
    }

    #[test]
    fn parse_triples_rollback() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ok
        <http://example.org/subject1> <http://an.example/predicate1> "malformed"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        <http://example.org/subject1> malformed "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        malformed <http://an.example/predicate2> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        "#.as_bytes();

        let reader = RDFTriplesReader::new(ResourceProviders::empty(), &RdfFile::new("", None));
        let dict = RefCell::new(Dict::default());
        let mut table_writer = TableWriter::new(&dict, 3);
        let result = reader.read_with_parser(&mut table_writer, || NTriplesParser::new(data));
        assert!(result.is_ok());
        assert_eq!(table_writer.size(), 1);
    }
}
