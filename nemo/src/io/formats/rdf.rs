//! Reading of RDF 1.1 triples files (N-Triples, Turtle, RDF/XML) and
//! RDF 1.1 quads files (N-Quads, TriG)
use std::{collections::HashSet, io::BufReader, str::FromStr};

use nemo_physical::{
    builder_proxy::{ColumnBuilderProxy, PhysicalBuilderProxyEnum},
    error::ReadingError,
    table_reader::{Resource, TableReader},
};
use oxiri::Iri;
use rio_api::{
    model::{BlankNode, NamedNode, Quad, Subject, Triple},
    parser::{QuadsParser, TriplesParser},
};
use rio_turtle::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::{
    builder_proxy::LogicalColumnBuilderProxyT,
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
    model::{
        types::primitive_types::PrimitiveType, Constant, Identifier, InvalidRdfLiteral, Key, Map,
        RdfLiteral, TupleConstraint,
    },
};

impl From<NamedNode<'_>> for Constant {
    fn from(value: NamedNode) -> Self {
        Constant::Abstract(value.iri.to_string().into())
    }
}

impl From<BlankNode<'_>> for Constant {
    fn from(value: BlankNode) -> Self {
        Constant::Abstract(value.to_string().into())
    }
}

impl TryFrom<rio_api::model::Literal<'_>> for Constant {
    type Error = InvalidRdfLiteral;

    fn try_from(value: rio_api::model::Literal<'_>) -> Result<Self, Self::Error> {
        match value {
            rio_api::model::Literal::Simple { value } => {
                Ok(Constant::StringLiteral(value.to_string()))
            }
            rio_api::model::Literal::LanguageTaggedString { value, language } => {
                Constant::try_from(RdfLiteral::LanguageString {
                    value: value.to_string(),
                    tag: language.to_string(),
                })
            }
            rio_api::model::Literal::Typed { value, datatype } => {
                Constant::try_from(RdfLiteral::DatatypeValue {
                    value: value.to_string(),
                    datatype: datatype.iri.to_string(),
                })
            }
        }
    }
}

impl TryFrom<Subject<'_>> for Constant {
    type Error = ReadingError;

    fn try_from(value: Subject<'_>) -> Result<Self, Self::Error> {
        match value {
            Subject::NamedNode(nn) => Ok(nn.into()),
            Subject::BlankNode(bn) => Ok(bn.into()),
            Subject::Triple(_t) => Err(ReadingError::RdfStarUnsupported),
        }
    }
}

impl TryFrom<rio_api::model::Term<'_>> for Constant {
    type Error = ReadingError;

    fn try_from(value: rio_api::model::Term<'_>) -> Result<Self, Self::Error> {
        match value {
            rio_api::model::Term::NamedNode(nn) => Ok(nn.into()),
            rio_api::model::Term::BlankNode(bn) => Ok(bn.into()),
            rio_api::model::Term::Literal(lit) => lit.try_into().map_err(Into::into),
            rio_api::model::Term::Triple(_t) => Err(ReadingError::RdfStarUnsupported),
        }
    }
}

const DEFAULT_GRAPH: &str = "__DEFAULT_GRAPH__";

impl TryFrom<Option<rio_api::model::GraphName<'_>>> for Constant {
    type Error = ReadingError;

    fn try_from(value: Option<rio_api::model::GraphName<'_>>) -> Result<Self, Self::Error> {
        match value {
            None => Ok(Self::Abstract(Identifier(DEFAULT_GRAPH.to_string()))),
            Some(rio_api::model::GraphName::NamedNode(nn)) => Ok(nn.into()),
            Some(rio_api::model::GraphName::BlankNode(bn)) => Ok(bn.into()),
        }
    }
}

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

macro_rules! constant_column_builder {
    (::$method:ident) => {
        <LogicalColumnBuilderProxyT as ColumnBuilderProxy<Constant>>::$method
    };
}

/// A [`TableReader`] for RDF 1.1 files containing triples.
#[derive(Debug, Clone)]
pub(crate) struct RDFReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    base: Option<Iri<String>>,
    logical_types: Vec<PrimitiveType>,
    variant: RDFVariant,
}

impl RDFReader {
    /// Create a new [`RDFReader`]
    #[allow(dead_code)]
    pub fn new(
        resource_providers: ResourceProviders,
        resource: Resource,
        base: Option<String>,
        logical_types: Vec<PrimitiveType>,
    ) -> Self {
        Self::with_variant(
            RDFVariant::Unspecified,
            resource_providers,
            resource,
            base,
            logical_types,
        )
    }

    /// Create a new [`RDFReader`] with the given [format
    /// variant][RDFVariant].
    pub fn with_variant(
        variant: RDFVariant,
        resource_providers: ResourceProviders,
        resource: Resource,
        base: Option<String>,
        logical_types: Vec<PrimitiveType>,
    ) -> Self {
        Self {
            resource_providers,
            resource,
            base: base.map(|iri| Iri::parse(iri).expect("should be a valid IRI.")),
            logical_types,
            variant,
        }
    }

    fn read_triples_with_parser<Parser>(
        &self,
        physical_builder_proxies: &mut [PhysicalBuilderProxyEnum<'_>],
        make_parser: impl FnOnce() -> Parser,
    ) -> Result<(), ReadingError>
    where
        Parser: TriplesParser,
        ReadingError: From<<Parser as TriplesParser>::Error>,
    {
        let mut builders = physical_builder_proxies
            .iter_mut()
            .zip(self.logical_types.clone())
            .map(|(bp, lt)| lt.wrap_physical_column_builder(bp))
            .collect::<Vec<_>>();

        assert!(builders.len() == 3);

        let mut triples = 0;
        let mut on_triple = |triple: Triple| {
            let subject: Constant = triple.subject.try_into()?;
            let predicate: Constant = triple.predicate.into();
            let object: Constant = triple.object.try_into()?;

            constant_column_builder!(::add)(&mut builders[0], subject)?;
            if let Err(e) = constant_column_builder!(::add)(&mut builders[1], predicate) {
                constant_column_builder!(::forget)(&mut builders[0]);
                return Err(e);
            }
            if let Err(e) = constant_column_builder!(::add)(&mut builders[2], object) {
                constant_column_builder!(::forget)(&mut builders[0]);
                constant_column_builder!(::forget)(&mut builders[1]);
                return Err(e);
            }

            triples += 1;
            if triples % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {triples} triples")
            }

            Ok::<_, ReadingError>(())
        };

        let mut parser = make_parser();

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed triple: {e}");
            }
        }

        log::info!("Finished loading: processed {triples} triples");

        Ok(())
    }

    fn read_quads_with_parser<Parser>(
        &self,
        physical_builder_proxies: &mut [PhysicalBuilderProxyEnum<'_>],
        make_parser: impl FnOnce() -> Parser,
    ) -> Result<(), ReadingError>
    where
        Parser: QuadsParser,
        ReadingError: From<<Parser as QuadsParser>::Error>,
    {
        let mut builders = physical_builder_proxies
            .iter_mut()
            .zip(self.logical_types.clone())
            .map(|(bp, lt)| lt.wrap_physical_column_builder(bp))
            .collect::<Vec<_>>();

        assert!(builders.len() == 4);

        let mut quads = 0;
        let mut on_quad = |quad: Quad| {
            let subject = Constant::try_from(quad.subject)?;
            let predicate = Constant::from(quad.predicate);
            let object = Constant::try_from(quad.object)?;
            let graph_name = Constant::try_from(quad.graph_name)?;

            constant_column_builder!(::add)(&mut builders[0], subject)?;
            if let Err(e) = constant_column_builder!(::add)(&mut builders[1], predicate) {
                constant_column_builder!(::forget)(&mut builders[0]);
                return Err(e);
            }
            if let Err(e) = constant_column_builder!(::add)(&mut builders[2], object) {
                constant_column_builder!(::forget)(&mut builders[0]);
                constant_column_builder!(::forget)(&mut builders[1]);
                return Err(e);
            }
            if let Err(e) = constant_column_builder!(::add)(&mut builders[3], graph_name) {
                constant_column_builder!(::forget)(&mut builders[0]);
                constant_column_builder!(::forget)(&mut builders[1]);
                constant_column_builder!(::forget)(&mut builders[2]);
                return Err(e);
            }

            quads += 1;
            if quads % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {quads} quads")
            }

            Ok::<_, ReadingError>(())
        };

        let mut parser = make_parser();

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_quad) {
                log::info!("Ignoring malformed quad: {e}");
            }
        }

        log::info!("Finished loading: processed {quads} quads");

        Ok(())
    }
}

impl TableReader for RDFReader {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        self: Box<Self>,
        builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let reader = BufReader::new(reader);

        match self.variant.refine_with_resource(&self.resource) {
            RDFVariant::NTriples => {
                self.read_triples_with_parser(builder_proxies, || NTriplesParser::new(reader))
            }
            RDFVariant::NQuads => {
                self.read_quads_with_parser(builder_proxies, || NQuadsParser::new(reader))
            }
            RDFVariant::Turtle => self.read_triples_with_parser(builder_proxies, || {
                TurtleParser::new(reader, self.base.clone())
            }),
            RDFVariant::RDFXML => self.read_triples_with_parser(builder_proxies, || {
                RdfXmlParser::new(reader, self.base.clone())
            }),
            RDFVariant::TriG => self.read_quads_with_parser(builder_proxies, || {
                TriGParser::new(reader, self.base.clone())
            }),
            RDFVariant::Unspecified => {
                Err(ReadingError::UnknownRDFFormatVariant(self.resource.clone()))
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
        inferred_types: &TupleConstraint,
    ) -> Result<Box<dyn TableReader>, Error> {
        let inferred_types = inferred_types
            .clone()
            .into_flat_primitive()
            .expect("must be flat and primitive");
        let base = attributes
            .pairs
            .get(&Key::identifier_from_str(BASE))
            .map(|term| term.as_abstract().expect("must be an identifier").name());

        Ok(Box::new(RDFReader::with_variant(
            self.variant,
            resource_providers,
            self.resource.clone().expect("is a required attribute"),
            base,
            inferred_types,
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
    use std::cell::RefCell;

    use nemo_physical::{
        builder_proxy::{PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy},
        datatypes::data_value::{DataValueIteratorT, PhysicalString},
        dictionary::Dictionary,
        management::database::Dict,
    };
    use rio_turtle::TurtleParser;
    use test_log::test;

    use super::*;

    #[test]
    fn example_1() {
        macro_rules! parse_example_with_rdf_parser {
            ($data:tt, $make_parser:expr) => {
                let $data = r#"<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here
                      # or on a line by themselves
                      _:subject1 <http://an.example/predicate1> "object1" .
                      _:subject2 <http://an.example/predicate2> "object2" .
                      "#.as_bytes();

                let dict = RefCell::new(Dict::default());
                let mut builders = vec![
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                ];
                let reader = RDFReader::new(ResourceProviders::empty(), String::new(), None, vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any]);

                let result = reader.read_triples_with_parser(&mut builders, $make_parser);
                assert!(result.is_ok());

                let columns = builders
                    .into_iter()
                    .map(|builder| match builder {
                        PhysicalBuilderProxyEnum::String(b) => b.finalize(),
                        _ => unreachable!("only string columns here"),
                    })
                    .collect::<Vec<_>>();

                log::debug!("columns: {columns:?}");
                let triples = (0..=2)
                    .map(|idx| {
                        columns
                            .iter()
                            .map(|column| {
                                column
                                    .get(idx)
                                    .and_then(|value| value.try_into().ok())
                                    .and_then(|u64: u64| usize::try_from(u64).ok())
                                    .and_then(|usize| dict.borrow_mut().get(usize))
                                    .unwrap()
                            })
                            .map(PhysicalString::from)
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                log::debug!("triple: {triples:?}");
                for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[0].iter().cloned()))).zip(vec!["<http://one.example/subject1>", "<http://one.example/predicate1>", "<http://one.example/object1>"]) {
                    assert_eq!(value, expected);
                }
                for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[1].iter().cloned()))).zip(vec!["_:subject1", "<http://an.example/predicate1>", r#""object1""#]) {
                    assert_eq!(value, expected);
                }
                for (value, expected) in PrimitiveType::Any.serialize_output(DataValueIteratorT::String(Box::new(triples[2].iter().cloned()))).zip(vec!["_:subject2", "<http://an.example/predicate2>", r#""object2""#]) {
                    assert_eq!(value, expected);
                }
            };
        }

        parse_example_with_rdf_parser!(reader, || NTriplesParser::new(reader));
        parse_example_with_rdf_parser!(reader, || TurtleParser::new(reader, None));
    }

    #[test]
    fn rollback() {
        let data = r#"<http://example.org/> <http://example.org/> <http://example.org/> .
                          malformed <http://example.org/> <http://example.org/>
                          <http://example.org/> malformed <http://example.org/> .
                          <http://example.org/> <http://example.org/> malformed .
                          <http://example.org/> <http://example.org/> "123"^^<http://www.w3.org/2001/XMLSchema#integer> .
                          <http://example.org/> <http://example.org/> "123.45"^^<http://www.w3.org/2001/XMLSchema#integer> .
                          <http://example.org/> <http://example.org/> "123.45"^^<http://www.w3.org/2001/XMLSchema#decimal> .
                          <http://example.org/> <http://example.org/> "123.45a"^^<http://www.w3.org/2001/XMLSchema#decimal> .
                          <https://example.org/> <https://example.org/> <https://example.org/> .
                      "#
        .as_bytes();

        let dict = RefCell::new(Dict::default());
        let mut builders = vec![
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
        ];
        let reader = RDFReader::new(
            ResourceProviders::empty(),
            String::new(),
            None,
            vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any],
        );

        let result = reader.read_triples_with_parser(&mut builders, || NTriplesParser::new(data));
        assert!(result.is_ok());

        let columns = builders
            .into_iter()
            .map(|builder| match builder {
                PhysicalBuilderProxyEnum::String(b) => b.finalize(),
                _ => unreachable!("only string columns here"),
            })
            .collect::<Vec<_>>();

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].len(), 4);
        assert_eq!(columns[1].len(), 4);
        assert_eq!(columns[2].len(), 4);
    }
}
