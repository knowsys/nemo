//! Reading of RDF 1.1 triples files (N-Triples, Turtle, RDF/XML) and
//! RDF 1.1 quads files (N-Quads, TriG)
use std::{collections::HashSet, io::BufReader, str::FromStr};

use nemo_physical::{
    datasources::{TableProvider, TupleBuffer},
    datavalues::{AnyDataValue, DataValueCreationError},
    error::ReadingError,
    resource::Resource,
};

use oxiri::Iri;
use rio_api::{
    model::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term, Triple},
    parser::{QuadsParser, TriplesParser},
};
use rio_turtle::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::{
    error::Error,
    io::{
        formats::{
            types::{
                attributes::RESOURCE, Direction, ExportSpec, FileFormat, FileFormatError,
                FileFormatMeta, ImportExportSpec, ImportSpec, PathWithFormatSpecificExtension,
                TableWriter,
            },
            PROGRESS_NOTIFY_INCREMENT,
        },
        resource_providers::ResourceProviders,
    },
    model::{Constant, Identifier, Key, Map, TupleConstraint},
};

use thiserror::Error;

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

const DEFAULT_GRAPH: &str = "__DEFAULT_GRAPH__";

fn is_turtle(resource: &Resource) -> bool {
    resource.ends_with(".ttl.gz") || resource.ends_with(".ttl")
}

fn is_rdf_xml(resource: &Resource) -> bool {
    resource.ends_with(".rdf.gz") || resource.ends_with(".rdf")
}

fn is_nt(resource: &Resource) -> bool {
    resource.ends_with(".nt.gz") || resource.ends_with(".nt")
}

fn is_nq(resource: &Resource) -> bool {
    resource.ends_with(".nq.gz") || resource.ends_with(".nq")
}

fn is_trig(resource: &Resource) -> bool {
    resource.ends_with(".trig.gz") || resource.ends_with(".trig")
}

/// A [`TableProvider`] for RDF 1.1 files containing triples.
#[derive(Debug, Clone)]
pub(crate) struct RDFReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    base: Option<Iri<String>>,
    variant: RDFVariant,
}

impl RDFReader {
    /// Create a new [`RDFReader`]
    #[allow(dead_code)]
    pub fn new(
        resource_providers: ResourceProviders,
        resource: Resource,
        base: Option<String>,
    ) -> Self {
        Self::with_variant(RDFVariant::Unspecified, resource_providers, resource, base)
    }

    /// Create a new [`RDFReader`] with the given [format
    /// variant][RDFVariant].
    pub fn with_variant(
        variant: RDFVariant,
        resource_providers: ResourceProviders,
        resource: Resource,
        base: Option<String>,
    ) -> Self {
        Self {
            resource_providers,
            resource,
            base: base.map(|iri| Iri::parse(iri).expect("should be a valid IRI.")),
            variant,
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

    fn datavalue_from_graph_name(
        value: Option<GraphName<'_>>,
    ) -> Result<AnyDataValue, RdfReadingError> {
        match value {
            None => {
                assert_eq!(
                    Iri::parse(DEFAULT_GRAPH.to_string()).unwrap().to_string(),
                    DEFAULT_GRAPH.to_string()
                );
                Ok(AnyDataValue::new_iri(DEFAULT_GRAPH.to_string()))
            }
            Some(GraphName::NamedNode(nn)) => Ok(Self::datavalue_from_named_node(nn)),
            Some(GraphName::BlankNode(bn)) => Ok(Self::datavalue_from_blank_node(bn)),
        }
    }

    /// Read the RDF triples from a parser.
    fn read_triples_with_parser<Parser>(
        &self,
        tuple_buffer: &mut TupleBuffer,
        make_parser: impl FnOnce() -> Parser,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Parser: TriplesParser,
    {
        assert_eq!(tuple_buffer.column_number(), 3);

        let mut triple_count = 0;

        let mut on_triple = |triple: Triple| {
            let subject = Self::datavalue_from_subject(triple.subject)?;
            let predicate = Self::datavalue_from_named_node(triple.predicate);
            let object = Self::datavalue_from_term(triple.object)?;

            tuple_buffer.next_value(subject);
            tuple_buffer.next_value(predicate);
            tuple_buffer.next_value(object);

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

    /// Read the RDF quads from a parser.
    fn read_quads_with_parser<Parser>(
        &self,
        tuple_buffer: &mut TupleBuffer,
        make_parser: impl FnOnce() -> Parser,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Parser: QuadsParser,
    {
        assert_eq!(tuple_buffer.column_number(), 4);

        let mut quad_count = 0;

        let mut on_triple = |quad: Quad| {
            let subject = Self::datavalue_from_subject(quad.subject)?;
            let predicate = Self::datavalue_from_named_node(quad.predicate);
            let object = Self::datavalue_from_term(quad.object)?;
            let graph_name = Self::datavalue_from_graph_name(quad.graph_name)?;

            tuple_buffer.next_value(subject);
            tuple_buffer.next_value(predicate);
            tuple_buffer.next_value(object);
            tuple_buffer.next_value(graph_name);

            quad_count += 1;
            if quad_count % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {quad_count} triples")
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser();

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed quad: {e}");
            }
        }

        log::info!("Finished loading: processed {quad_count} quad");

        Ok(())
    }
}

impl TableProvider for RDFReader {
    fn provide_table_data(
        self: Box<Self>,
        tuple_buffer: &mut TupleBuffer,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let reader = BufReader::new(reader);

        match self.variant.refine_with_resource(&self.resource) {
            RDFVariant::NTriples => {
                self.read_triples_with_parser(tuple_buffer, || NTriplesParser::new(reader))
            }
            RDFVariant::NQuads => {
                self.read_quads_with_parser(tuple_buffer, || NQuadsParser::new(reader))
            }
            RDFVariant::Turtle => self.read_triples_with_parser(tuple_buffer, || {
                TurtleParser::new(reader, self.base.clone())
            }),
            RDFVariant::RDFXML => self.read_triples_with_parser(tuple_buffer, || {
                RdfXmlParser::new(reader, self.base.clone())
            }),
            RDFVariant::TriG => self.read_quads_with_parser(tuple_buffer, || {
                TriGParser::new(reader, self.base.clone())
            }),
            RDFVariant::Unspecified => {
                // TODO Return a better error. Should not come from physical layer!
                Err(Box::new(ReadingError::UnknownRDFFormatVariant(
                    self.resource.clone(),
                )))
            }
        }
    }
}

const BASE: &str = "base";

/// The different supported variants of the RDF format.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RDFVariant {
    /// An unspecified format, using the resource name as a heuristic.
    #[default]
    Unspecified,
    /// RDF 1.1 N-Triples
    NTriples,
    /// RDF 1.1 N-Quads
    NQuads,
    /// RDF 1.1 Turtle
    Turtle,
    /// RDF 1.1 RDF/XML
    RDFXML,
    /// RDF 1.1 TriG
    TriG,
}

impl RDFVariant {
    /// Create an appropriate format variant from the given resource,
    /// based on the file extension.
    pub fn from_resource(resource: &String) -> RDFVariant {
        match resource {
            resource if is_turtle(resource) => RDFVariant::Turtle,
            resource if is_rdf_xml(resource) => RDFVariant::RDFXML,
            resource if is_nt(resource) => RDFVariant::NTriples,
            resource if is_nq(resource) => RDFVariant::NQuads,
            resource if is_trig(resource) => RDFVariant::TriG,
            _ => RDFVariant::Unspecified,
        }
    }

    fn refine_with_resource(self, resource: &String) -> RDFVariant {
        match self {
            Self::Unspecified => Self::from_resource(resource),
            _ => self,
        }
    }
}

impl PathWithFormatSpecificExtension for RDFVariant {
    fn extension(&self) -> Option<&str> {
        match self {
            Self::NTriples => Some("nt"),
            Self::NQuads => Some("nq"),
            Self::Turtle => Some("ttl"),
            Self::RDFXML => Some("rdf"),
            Self::TriG => Some("trig"),
            Self::Unspecified => None,
        }
    }
}

impl std::fmt::Display for RDFVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NTriples => write!(f, "RDF N-Triples"),
            Self::NQuads => write!(f, "RDF N-Quads"),
            Self::Turtle => write!(f, "RDF Turtle"),
            Self::RDFXML => write!(f, "RDF/XML"),
            Self::TriG => write!(f, "RDF TriG"),
            Self::Unspecified => write!(f, "RDF"),
        }
    }
}

impl FromStr for RDFVariant {
    type Err = FileFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let canonicalised = s.to_uppercase();

        match canonicalised.as_str() {
            "NTRIPLES" => Ok(Self::NTriples),
            "NQUADS" => Ok(Self::NQuads),
            "TURTLE" => Ok(Self::Turtle),
            "RDFXML" => Ok(Self::RDFXML),
            "TRIG" => Ok(Self::TriG),
            "RDF" => Ok(Self::Unspecified),
            _ => Err(FileFormatError::UnknownFormat(s.to_string())),
        }
    }
}

/// File formats for RDF.
#[derive(Debug, Default, Clone)]
pub struct RDFFormat {
    resource: Option<Resource>,
    variant: RDFVariant,
}

impl RDFFormat {
    /// Construct new RDF file format metadata.
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct a new RDF file format metadata for the given [format
    /// variant][RDFVariant].
    pub fn with_variant(variant: RDFVariant) -> Self {
        Self {
            variant,
            ..Default::default()
        }
    }

    /// Construct a new RDF N-Triples file format metadata.
    pub fn ntriples() -> Self {
        Self::with_variant(RDFVariant::NTriples)
    }

    /// Construct a new RDF N-Quads file format metadata.
    pub fn nquads() -> Self {
        Self::with_variant(RDFVariant::NQuads)
    }

    /// Construct a new RDF Turtle file format metadata.
    pub fn turtle() -> Self {
        Self::with_variant(RDFVariant::Turtle)
    }

    /// Construct a new RDF RDF/XML file format metadata.
    pub fn rdf_xml() -> Self {
        Self::with_variant(RDFVariant::RDFXML)
    }

    fn try_into_import_export(
        mut self,
        direction: Direction,
        resource: Resource,
        base: Option<String>,
        predicate: Identifier,
        declared_types: TupleConstraint,
    ) -> Result<ImportExportSpec, FileFormatError> {
        let mut attributes = Map::singleton(
            Key::identifier_from_str(RESOURCE),
            Constant::StringLiteral(resource),
        );

        if let Some(base) = base {
            attributes.pairs.insert(
                Key::identifier_from_str(BASE),
                Constant::Abstract(Identifier(base)),
            );
        }

        let constraint =
            self.validated_and_refined_type_declaration(direction, &attributes, declared_types)?;

        Ok(ImportExportSpec {
            predicate,
            constraint,
            attributes,
            format: Box::new(self),
        })
    }

    /// Obtain an [ImportSpec] for this format.
    pub fn try_into_import(
        self,
        resource: Resource,
        predicate: Identifier,
        declared_types: TupleConstraint,
        base: Option<String>,
    ) -> Result<ImportSpec, FileFormatError> {
        Ok(ImportSpec::from(self.try_into_import_export(
            Direction::Reading,
            resource,
            base,
            predicate,
            declared_types,
        )?))
    }

    /// Obtain an [ExportSpec] for this format.
    pub fn try_into_export(
        self,
        resource: Resource,
        predicate: Identifier,
        declared_types: TupleConstraint,
        base: Option<String>,
    ) -> Result<ExportSpec, FileFormatError> {
        Ok(ExportSpec::from(self.try_into_import_export(
            Direction::Writing,
            resource,
            base,
            predicate,
            declared_types,
        )?))
    }
}

impl FileFormatMeta for RDFFormat {
    fn file_format(&self) -> FileFormat {
        FileFormat::RDF(self.variant)
    }

    fn reader(
        &self,
        attributes: &Map,
        _declared_types: &TupleConstraint,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error> {
        let base = attributes
            .pairs
            .get(&Key::identifier_from_str(BASE))
            .map(|term| term.as_abstract().expect("must be an identifier").name());

        Ok(Box::new(RDFReader::with_variant(
            self.variant,
            resource_providers,
            self.resource.clone().expect("is a required attribute"),
            base,
        )))
    }

    fn writer(&self, _attributes: &Map) -> Result<Box<dyn TableWriter>, Error> {
        Err(FileFormatError::UnsupportedWrite(self.file_format()).into())
    }

    fn resources(&self, attributes: &Map) -> Vec<Resource> {
        vec![self.resource.clone().unwrap_or(
            attributes
                .pairs
                .get(&Key::identifier_from_str(RESOURCE))
                .expect("is a required attribute")
                .as_resource()
                .expect("must be a string or an IRI")
                .to_string(),
        )]
    }

    fn optional_attributes(&self, _direction: Direction) -> HashSet<Key> {
        let mut attributes = HashSet::new();

        attributes.insert(BASE);

        attributes
            .into_iter()
            .map(Key::identifier_from_str)
            .collect()
    }

    fn required_attributes(&self, _direction: Direction) -> HashSet<Key> {
        let mut attributes = HashSet::new();

        attributes.insert(RESOURCE);

        attributes
            .into_iter()
            .map(Key::identifier_from_str)
            .collect()
    }

    fn validate_attribute_values(
        &mut self,
        _direction: Direction,
        attributes: &Map,
    ) -> Result<(), FileFormatError> {
        let resource = attributes
            .pairs
            .get(&Key::identifier_from_str(RESOURCE))
            .expect("is a required attribute");
        match resource.as_resource() {
            Some(resource) => {
                self.variant = RDFVariant::from_resource(resource);
                self.resource = Some(resource.clone());
            }
            None => {
                return Err(FileFormatError::InvalidAttributeValue {
                    value: resource.clone(),
                    attribute: Key::identifier_from_str(RESOURCE),
                    description: "Resource should be a string literal or an IRI".to_string(),
                })
            }
        }

        Ok(())
    }

    fn validate_and_refine_type_declaration(
        &mut self,
        declared_types: TupleConstraint,
    ) -> Result<TupleConstraint, FileFormatError> {
        let required = match self.variant {
            RDFVariant::NQuads | RDFVariant::TriG => 4,
            _ => 3,
        };

        if declared_types.arity() != required {
            return Err(FileFormatError::InvalidArityExact {
                arity: declared_types.arity(),
                required,
                format: self.file_format(),
            });
        }

        if declared_types.clone().into_flat_primitive().is_none() {
            return Err(FileFormatError::UnsupportedComplexTypes {
                format: self.file_format(),
            });
        }

        Ok(declared_types)
    }
}

#[cfg(test)]
mod test {
    use super::RDFReader;
    use std::cell::RefCell;

    use nemo_physical::{datasources::TupleBuffer, management::database::Dict};
    use rio_turtle::{NTriplesParser, TurtleParser};
    use test_log::test;

    use crate::io::resource_providers::ResourceProviders;

    #[test]
    fn parse_triples_nt() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object1> . # comments here
        # or on a line by themselves
        _:bnode1 <http://an.example/predicate1> "string1" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" . # Note that duplicate triples are not eliminated here yet
        "#.as_bytes();

        let reader = RDFReader::new(ResourceProviders::empty(), String::new(), None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_buffer = TupleBuffer::new(&dict, 3);
        let result =
            reader.read_triples_with_parser(&mut tuple_buffer, || NTriplesParser::new(data));
        assert!(result.is_ok());
        assert_eq!(tuple_buffer.size(), 4);
    }

    #[test]
    fn parse_triples_turtle() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1>
            [ <http://example.org/predicate2> 42, "test" ] .
        "#
        .as_bytes();

        let reader = RDFReader::new(ResourceProviders::empty(), String::new(), None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_buffer = TupleBuffer::new(&dict, 3);
        let result =
            reader.read_triples_with_parser(&mut tuple_buffer, || TurtleParser::new(data, None));
        assert!(result.is_ok());
        assert_eq!(tuple_buffer.size(), 3);
    }

    #[test]
    fn parse_triples_rollback() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ok
        <http://example.org/subject1> <http://an.example/predicate1> "malformed"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        <http://example.org/subject1> malformed "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        malformed <http://an.example/predicate2> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        "#.as_bytes();

        let reader = RDFReader::new(ResourceProviders::empty(), String::new(), None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_buffer = TupleBuffer::new(&dict, 3);
        let result =
            reader.read_triples_with_parser(&mut tuple_buffer, || NTriplesParser::new(data));
        assert!(result.is_ok());
        assert_eq!(tuple_buffer.size(), 1);
    }

    // #[test]
    // fn example_1() {
    //     macro_rules! parse_example_with_rdf_parser {
    //         ($data:tt, $make_parser:expr) => {
    //             let $data = r#"<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here
    //                   # or on a line by themselves
    //                   _:subject1 <http://an.example/predicate1> "object1" .
    //                   _:subject2 <http://an.example/predicate2> "object2" .
    //                   "#.as_bytes();

    //             let dict = RefCell::new(Dict::default());
    //             let mut builders = vec![
    //                 PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //                 PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //                 PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             ];
    //             let reader = RDFReader::new(ResourceProviders::empty(), String::new(), None, vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any]);

    //             let result = reader.read_triples_with_parser(&mut builders, $make_parser);
    //             assert!(result.is_ok());

    //             let columns = builders
    //                 .into_iter()
    //                 .map(|builder| match builder {
    //                     PhysicalBuilderProxyEnum::String(b) => b.finalize(),
    //                     _ => unreachable!("only string columns here"),
    //                 })
    //                 .collect::<Vec<_>>();

    //             log::debug!("columns: {columns:?}");
    //             let triples = (0..=2)
    //                 .map(|idx| {
    //                     columns
    //                         .iter()
    //                         .map(|column| {
    //                             column
    //                                 .get(idx)
    //                                 .and_then(|value| value.try_into().ok())
    //                                 .and_then(|u64: u64| usize::try_from(u64).ok())
    //                                 .and_then(|usize| dict.borrow_mut().get(usize))
    //                                 .unwrap()
    //                         })
    //                         .map(PhysicalString::from)
    //                         .collect::<Vec<_>>()
    //                 })
    //                 .collect::<Vec<_>>();
    //             log::debug!("triple: {triples:?}");
    //             for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[0].iter().cloned()))).zip(vec!["http://one.example/subject1", "http://one.example/predicate1", "http://one.example/object1"]) {
    //                 assert_eq!(value, expected);
    //             }
    //             for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[1].iter().cloned()))).zip(vec!["_:subject1", "http://an.example/predicate1", r#""object1""#]) {
    //                 assert_eq!(value, expected);
    //             }
    //             for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[2].iter().cloned()))).zip(vec!["_:subject2", "http://an.example/predicate2", r#""object2""#]) {
    //                 assert_eq!(value, expected);
    //             }
    //         };
    //     }

    //     parse_example_with_rdf_parser!(reader, || NTriplesParser::new(reader));
    //     parse_example_with_rdf_parser!(reader, || TurtleParser::new(reader, None));
    // }

    // #[test]
    // fn rollback() {
    //     let data = r#"<http://example.org/> <http://example.org/> <http://example.org/> .
    //                       malformed <http://example.org/> <http://example.org/>
    //                       <http://example.org/> malformed <http://example.org/> .
    //                       <http://example.org/> <http://example.org/> malformed .
    //                       <http://example.org/> <http://example.org/> "123"^^<http://www.w3.org/2001/XMLSchema#integer> .
    //                       <http://example.org/> <http://example.org/> "123.45"^^<http://www.w3.org/2001/XMLSchema#integer> .
    //                       <http://example.org/> <http://example.org/> "123.45"^^<http://www.w3.org/2001/XMLSchema#decimal> .
    //                       <http://example.org/> <http://example.org/> "123.45a"^^<http://www.w3.org/2001/XMLSchema#decimal> .
    //                       <https://example.org/> <https://example.org/> <https://example.org/> .
    //                   "#
    //     .as_bytes();

    //     let dict = RefCell::new(Dict::default());
    //     let mut builders = vec![
    //         PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //         PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //         PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //     ];
    //     let reader = RDFReader::new(
    //         ResourceProviders::empty(),
    //         String::new(),
    //         None,
    //         vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any],
    //     );

    //     let result = reader.read_triples_with_parser(&mut builders, || NTriplesParser::new(data));
    //     assert!(result.is_ok());

    //     let columns = builders
    //         .into_iter()
    //         .map(|builder| match builder {
    //             PhysicalBuilderProxyEnum::String(b) => b.finalize(),
    //             _ => unreachable!("only string columns here"),
    //         })
    //         .collect::<Vec<_>>();

    //     assert_eq!(columns.len(), 3);
    //     assert_eq!(columns[0].len(), 4);
    //     assert_eq!(columns[1].len(), 4);
    //     assert_eq!(columns[2].len(), 4);
    // }
}
