//! Reading of RDF 1.1 triples files (N-Triples, Turtle, RDF/XML) and
//! RDF 1.1 quads files (N-Quads, TriG)
use std::{
    io::{BufRead, Write},
    mem::size_of,
};

use bytesize::ByteSize;
use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    dictionary::string_map::NullMap,
    management::bytesized::ByteSized,
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
    io::formats::{
        types::{Direction, TableWriter},
        PROGRESS_NOTIFY_INCREMENT,
    },
    model::{
        Constant, FileFormat, Identifier, Map, RdfVariant, PARAMETER_NAME_BASE,
        PARAMETER_NAME_RESOURCE,
    },
};

use thiserror::Error;

use super::import_export::{ImportExportError, ImportExportHandler, ImportExportHandlers};

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
    RioXml(#[from] rio_xml::RdfXmlError),
    /// Unable to determine RDF format.
    #[error("Could not determine which RDF parser to use for resource {0}")]
    UnknownRdfFormat(Resource),
}

const DEFAULT_GRAPH: &str = "__DEFAULT_GRAPH__";

/// A [`TableProvider`] for RDF 1.1 files containing triples.
pub(crate) struct RdfReader {
    read: Box<dyn BufRead>,
    variant: RdfVariant,
    base: Option<Iri<String>>,
    /// Map to store how nulls relate to blank nodes.
    ///
    /// TODO: An RdfReader is specific to one BufRead, which it consumes when reading.
    /// There is little point in storing this map (which is only used during this step).
    /// To support bnode contexts that span several files, another architecture would be needed,
    /// where the NullMap survives the RdfReader.
    bnode_map: NullMap,
}

impl RdfReader {
    /// Create a new [`RDFReader`]
    #[allow(dead_code)]
    pub fn new(read: Box<dyn BufRead>, variant: RdfVariant, base: Option<Iri<String>>) -> Self {
        Self {
            read,
            variant,
            base,
            bnode_map: Default::default(),
        }
    }

    fn datavalue_from_named_node(value: NamedNode) -> AnyDataValue {
        AnyDataValue::new_iri(value.iri.to_string())
    }

    fn datavalue_from_blank_node(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: BlankNode,
    ) -> AnyDataValue {
        if let Some(null) = bnode_map.get(value.to_string().as_str()) {
            AnyDataValue::from(null.clone())
        } else {
            let null = tuple_writer.fresh_null();
            bnode_map.insert(value.to_string().as_str(), null.clone());
            AnyDataValue::from(null)
        }
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

    fn datavalue_from_subject(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Subject<'_>,
    ) -> Result<AnyDataValue, RdfReadingError> {
        match value {
            Subject::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Subject::BlankNode(bn) => {
                Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn))
            }
            Subject::Triple(_t) => Err(RdfReadingError::RdfStarUnsupported),
        }
    }

    fn datavalue_from_term(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Term<'_>,
    ) -> Result<AnyDataValue, RdfReadingError> {
        match value {
            Term::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Term::BlankNode(bn) => Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn)),
            Term::Literal(lit) => Ok(Self::datavalue_from_literal(lit)?),
            Term::Triple(_t) => Err(RdfReadingError::RdfStarUnsupported),
        }
    }

    fn datavalue_from_graph_name(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
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
            Some(GraphName::BlankNode(bn)) => {
                Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn))
            }
        }
    }

    /// Read the RDF triples from a parser.
    fn read_triples_with_parser<Parser>(
        mut self,
        tuple_writer: &mut TupleWriter,
        make_parser: impl Fn(Box<dyn BufRead>) -> Parser,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Parser: TriplesParser,
    {
        assert_eq!(tuple_writer.column_number(), 3);

        let mut triple_count = 0;

        let mut on_triple = |triple: Triple| {
            let subject =
                Self::datavalue_from_subject(&mut self.bnode_map, tuple_writer, triple.subject)?;
            let predicate = Self::datavalue_from_named_node(triple.predicate);
            let object =
                Self::datavalue_from_term(&mut self.bnode_map, tuple_writer, triple.object)?;

            tuple_writer.add_tuple_value(subject);
            tuple_writer.add_tuple_value(predicate);
            tuple_writer.add_tuple_value(object);

            triple_count += 1;
            if triple_count % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {triple_count} triples")
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser(self.read);

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
        mut self,
        tuple_writer: &mut TupleWriter,
        make_parser: impl Fn(Box<dyn BufRead>) -> Parser,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Parser: QuadsParser,
    {
        assert_eq!(tuple_writer.column_number(), 4);

        let mut quad_count = 0;

        let mut on_triple = |quad: Quad| {
            let subject =
                Self::datavalue_from_subject(&mut self.bnode_map, tuple_writer, quad.subject)?;
            let predicate = Self::datavalue_from_named_node(quad.predicate);
            let object = Self::datavalue_from_term(&mut self.bnode_map, tuple_writer, quad.object)?;
            let graph_name = Self::datavalue_from_graph_name(
                &mut self.bnode_map,
                tuple_writer,
                quad.graph_name,
            )?;

            tuple_writer.add_tuple_value(subject);
            tuple_writer.add_tuple_value(predicate);
            tuple_writer.add_tuple_value(object);
            tuple_writer.add_tuple_value(graph_name);

            quad_count += 1;
            if quad_count % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("Loading: processed {quad_count} triples")
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser(self.read);

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed quad: {e}");
            }
        }

        log::info!("Finished loading: processed {quad_count} quad");

        Ok(())
    }
}

impl TableProvider for RdfReader {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base_iri = self.base.clone();

        match self.variant {
            RdfVariant::NTriples => {
                self.read_triples_with_parser(tuple_writer, |read| NTriplesParser::new(read))
            }
            RdfVariant::NQuads => {
                self.read_quads_with_parser(tuple_writer, |read| NQuadsParser::new(read))
            }
            RdfVariant::Turtle => self.read_triples_with_parser(tuple_writer, |read| {
                TurtleParser::new(read, base_iri.clone())
            }),
            RdfVariant::RDFXML => self.read_triples_with_parser(tuple_writer, |read| {
                RdfXmlParser::new(read, base_iri.clone())
            }),
            RdfVariant::TriG => self.read_quads_with_parser(tuple_writer, |read| {
                TriGParser::new(read, base_iri.clone())
            }),
            RdfVariant::Unspecified => unreachable!(
                "the reader should not be instantiated with unknown format by the handler"
            ),
        }
    }
}

impl std::fmt::Debug for RdfReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdfReader")
            .field("read", &"<unspecified std::io::Read>")
            .field("variant", &self.variant)
            .field("base", &self.base)
            .finish()
    }
}

impl ByteSized for RdfReader {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64)
    }
}

/// An [ImportExportHandler] for RDF formats.
#[derive(Debug, Default, Clone)]
pub struct RdfHandler {
    /// The resource to write to/read from.
    /// This can be `None` for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so the predicate is setting there.
    resource: Option<Resource>,
    /// Base IRI, if given.
    base: Option<Iri<String>>,
    /// The specific RDF format to be used.
    variant: RdfVariant,
}

impl RdfHandler {
    /// Construct an RDF handler of the given variant.
    pub(crate) fn try_new(
        variant: RdfVariant,
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // Basic checks for unsupported attributes:
        ImportExportHandlers::check_attributes(
            attributes,
            &vec![PARAMETER_NAME_RESOURCE, PARAMETER_NAME_BASE],
        )?;

        let resource = ImportExportHandlers::extract_resource(attributes, direction)?;

        let base: Option<Iri<String>>;
        if let Some(base_string) =
            ImportExportHandlers::extract_iri(attributes, PARAMETER_NAME_BASE, true)?
        {
            if let Ok(b) = Iri::parse(base_string.clone()) {
                base = Some(b);
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_BASE,
                    Constant::Abstract(Identifier::new(base_string.clone())),
                    "must be a valid IRI",
                ));
            }
        } else {
            base = None;
        }

        let refined_variant: RdfVariant;
        if variant == RdfVariant::Unspecified {
            if let Some(ref res) = resource {
                refined_variant = RdfVariant::from_resource(res);
            } else {
                refined_variant = variant;
            }
        } else {
            refined_variant = variant;
        }

        Ok(Box::new(Self {
            resource: resource,
            base: base,
            variant: refined_variant,
        }))
    }
}

impl ImportExportHandler for RdfHandler {
    fn file_format(&self) -> FileFormat {
        FileFormat::RDF(self.variant)
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        arity: usize,
    ) -> Result<Box<dyn TableProvider>, Error> {
        // TODO: Arity is ignored. It is only relevant if the RDF variant was unspecified, since it could help to guess the format
        // in this case. But we do not yet do this.
        Ok(Box::new(RdfReader::new(
            read,
            self.variant,
            self.base.clone(),
        )))
    }

    fn writer(&self, _writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Err(ImportExportError::UnsupportedWrite(self.file_format()).into())
    }

    fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn arity(&self) -> Option<usize> {
        match self.variant {
            RdfVariant::Unspecified => None,
            RdfVariant::NTriples | RdfVariant::Turtle | RdfVariant::RDFXML => Some(3),
            RdfVariant::NQuads | RdfVariant::TriG => Some(4),
        }
    }

    fn file_extension(&self) -> Option<String> {
        match self.variant {
            RdfVariant::Unspecified => None,
            RdfVariant::NTriples => Some("nt".to_string()),
            RdfVariant::NQuads => Some("nq".to_string()),
            RdfVariant::Turtle => Some("ttl".to_string()),
            RdfVariant::RDFXML => Some("rdf".to_string()),
            RdfVariant::TriG => Some("trig".to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::RdfReader;
    use std::cell::RefCell;

    use nemo_physical::{datasources::tuple_writer::TupleWriter, management::database::Dict};
    use rio_turtle::{NTriplesParser, TurtleParser};
    use test_log::test;

    use crate::model::RdfVariant;

    #[test]
    fn parse_triples_nt() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object1> . # comments here
        # or on a line by themselves
        _:bnode1 <http://an.example/predicate1> "string1" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" . # Note that duplicate triples are not eliminated here yet
        "#.as_bytes();

        let reader = RdfReader::new(Box::new(data), RdfVariant::NTriples, None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result =
            reader.read_triples_with_parser(&mut tuple_writer, |read| NTriplesParser::new(read));
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 4);
    }

    #[test]
    fn parse_triples_turtle() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1>
            [ <http://example.org/predicate2> 42, "test" ] .
        "#
        .as_bytes();

        let reader = RdfReader::new(Box::new(data), RdfVariant::Turtle, None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader
            .read_triples_with_parser(&mut tuple_writer, |read| TurtleParser::new(read, None));
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 3);
    }

    #[test]
    fn parse_triples_rollback() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ok
        <http://example.org/subject1> <http://an.example/predicate1> "malformed"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        <http://example.org/subject1> malformed "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        malformed <http://an.example/predicate2> "12"^^<http://www.w3.org/2001/XMLSchema#int> . # ignored
        "#.as_bytes();

        let reader = RdfReader::new(Box::new(data), RdfVariant::NTriples, None);
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result =
            reader.read_triples_with_parser(&mut tuple_writer, |read| NTriplesParser::new(read));
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 1);
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
