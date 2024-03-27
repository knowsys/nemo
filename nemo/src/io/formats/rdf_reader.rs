//! Reader for various RDF formats, which supports triples files (N-Triples, Turtle, RDF/XML) and
//! quads files (N-Quads, TriG).
use bytesize::ByteSize;
use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    dictionary::string_map::NullMap,
    management::bytesized::ByteSized,
};
use std::{cell::Cell, io::BufRead, mem::size_of};

use oxiri::Iri;
use rio_api::{
    model::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term, Triple},
    parser::{QuadsParser, TriplesParser},
};
use rio_turtle::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::{io::formats::PROGRESS_NOTIFY_INCREMENT, model::RdfVariant};

use super::rdf::{RdfFormatError, RdfValueFormat};

/// IRI to be used for the default graph used by Nemo when loading RDF data with
/// named graphs (quads).
///
/// SPARQL 1.1 has failed to provide any standard identifier for this purpose.
/// If future SPARQL or RDF versions are adding this, we could align accordingly.
const DEFAULT_GRAPH: &str = "tag:nemo:defaultgraph";

/// A [TableProvider] for RDF 1.1 files containing triples.
pub(super) struct RdfReader {
    read: Box<dyn BufRead>,
    variant: RdfVariant,
    base: Option<Iri<String>>,
    value_formats: Vec<RdfValueFormat>,
    limit: Option<u64>,
    /// Map to store how nulls relate to blank nodes.
    ///
    /// TODO: An RdfReader is specific to one BufRead, which it consumes when reading.
    /// There is little point in storing this map (which is only used during this step).
    /// To support bnode contexts that span several files, another architecture would be needed,
    /// where the NullMap survives the RdfReader.
    bnode_map: NullMap,
}

impl RdfReader {
    /// Create a new [RDFReader]
    pub(super) fn new(
        read: Box<dyn BufRead>,
        variant: RdfVariant,
        base: Option<Iri<String>>,
        value_formats: Vec<RdfValueFormat>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            read,
            variant,
            base,
            value_formats,
            limit,
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
            AnyDataValue::from(*null)
        } else {
            let null = tuple_writer.fresh_null();
            bnode_map.insert(value.to_string().as_str(), null);
            AnyDataValue::from(null)
        }
    }

    /// Create [AnyDataValue] from a [Literal].
    fn datavalue_from_literal(value: Literal<'_>) -> Result<AnyDataValue, DataValueCreationError> {
        match value {
            Literal::Simple { value } => Ok(AnyDataValue::new_plain_string(value.to_string())),
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
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            Subject::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Subject::BlankNode(bn) => {
                Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn))
            }
            Subject::Triple(_t) => Err(RdfFormatError::RdfStarUnsupported),
        }
    }

    fn datavalue_from_term(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Term<'_>,
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            Term::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Term::BlankNode(bn) => Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn)),
            Term::Literal(lit) => Ok(Self::datavalue_from_literal(lit)?),
            Term::Triple(_t) => Err(RdfFormatError::RdfStarUnsupported),
        }
    }

    fn datavalue_from_graph_name(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Option<GraphName<'_>>,
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            None => Ok(AnyDataValue::new_iri(DEFAULT_GRAPH.to_string())),
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
        log::info!("Starting RDF import (format {})", self.variant);

        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::Skip)
            .collect();

        assert_eq!(skip.len(), 3);
        assert_eq!(
            tuple_writer.column_number(),
            skip.iter().fold(0, |acc: usize, b| {
                if *b {
                    acc
                } else {
                    acc + 1
                }
            })
        );

        let stop_limit = self.limit.unwrap_or(u64::MAX);
        let triple_count = Cell::new(0);

        let mut on_triple = |triple: Triple| {
            // This is needed since a parser might process several RDF statements
            // before giving us a chance to stop in the outer loop.
            if triple_count.get() == stop_limit {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }

            if !skip[0] {
                let subject = Self::datavalue_from_subject(
                    &mut self.bnode_map,
                    tuple_writer,
                    triple.subject,
                )?;
                tuple_writer.add_tuple_value(subject);
            }
            if !skip[1] {
                let predicate = Self::datavalue_from_named_node(triple.predicate);
                tuple_writer.add_tuple_value(predicate);
            }
            if !skip[2] {
                let object =
                    Self::datavalue_from_term(&mut self.bnode_map, tuple_writer, triple.object)?;
                tuple_writer.add_tuple_value(object);
            }

            triple_count.set(triple_count.get() + 1);
            if triple_count.get() % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("... processed {} triples", triple_count.get())
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser(self.read);

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed RDF: {e}");
            }
            if triple_count.get() == stop_limit {
                break;
            }
        }

        log::info!("Finished import: processed {} triples", triple_count.get());

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
        log::info!("Starting RDF import (format {})", self.variant);

        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::Skip)
            .collect();

        assert_eq!(skip.len(), 4);
        assert_eq!(
            tuple_writer.column_number(),
            skip.iter().fold(0, |acc: usize, b| {
                if *b {
                    acc
                } else {
                    acc + 1
                }
            })
        );

        let stop_limit = self.limit.unwrap_or(u64::MAX);
        let quad_count = Cell::new(0);

        let mut on_quad = |quad: Quad| {
            // This is needed since a parser might process several RDF statements
            // before giving us a chance to stop in the outer loop.
            if quad_count.get() == stop_limit {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }

            if !skip[0] {
                let graph_name = Self::datavalue_from_graph_name(
                    &mut self.bnode_map,
                    tuple_writer,
                    quad.graph_name,
                )?;
                tuple_writer.add_tuple_value(graph_name);
            }
            if !skip[1] {
                let subject =
                    Self::datavalue_from_subject(&mut self.bnode_map, tuple_writer, quad.subject)?;
                tuple_writer.add_tuple_value(subject);
            }
            if !skip[2] {
                let predicate = Self::datavalue_from_named_node(quad.predicate);
                tuple_writer.add_tuple_value(predicate);
            }
            if !skip[3] {
                let object =
                    Self::datavalue_from_term(&mut self.bnode_map, tuple_writer, quad.object)?;
                tuple_writer.add_tuple_value(object);
            }

            quad_count.set(quad_count.get() + 1);
            if quad_count.get() % PROGRESS_NOTIFY_INCREMENT == 0 {
                log::info!("... processed {} triples", quad_count.get())
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let mut parser = make_parser(self.read);

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_quad) {
                log::info!("Ignoring malformed RDF: {e}");
            }
            if quad_count.get() == stop_limit {
                break;
            }
        }

        log::info!("Finished import: processed {} quads", quad_count.get());

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
                self.read_triples_with_parser(tuple_writer, NTriplesParser::new)
            }
            RdfVariant::NQuads => self.read_quads_with_parser(tuple_writer, NQuadsParser::new),
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

#[cfg(test)]
mod test {
    use super::{RdfReader, DEFAULT_GRAPH};
    use std::cell::RefCell;

    use nemo_physical::{
        datasources::tuple_writer::TupleWriter, datavalues::AnyDataValue,
        dictionary::string_map::NullMap, management::database::Dict,
    };
    use oxiri::Iri;
    use rio_turtle::{NTriplesParser, TurtleParser};
    use test_log::test;

    use crate::{io::formats::rdf::RdfValueFormat, model::RdfVariant};

    #[test]
    fn parse_triples_nt() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1> <http://example.org/object1> . # comments here
        # or on a line by themselves
        _:bnode1 <http://an.example/predicate1> "string1" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" .
        <http://example.org/subject1> <http://an.example/predicate2> "string2" . # Note that duplicate triples are not eliminated here yet
        "#.as_bytes();

        let reader = RdfReader::new(
            Box::new(data),
            RdfVariant::NTriples,
            None,
            vec![
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
            ],
            None,
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader.read_triples_with_parser(&mut tuple_writer, NTriplesParser::new);
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 4);
    }

    #[test]
    fn parse_triples_turtle() {
        let data = r#"<http://example.org/subject1> <http://example.org/predicate1>
            [ <http://example.org/predicate2> 42, "test" ] .
        "#
        .as_bytes();

        let reader = RdfReader::new(
            Box::new(data),
            RdfVariant::Turtle,
            None,
            vec![
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
            ],
            None,
        );
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

        let reader = RdfReader::new(
            Box::new(data),
            RdfVariant::NTriples,
            None,
            vec![
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
                RdfValueFormat::Anything,
            ],
            None,
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader.read_triples_with_parser(&mut tuple_writer, NTriplesParser::new);
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 1);
    }

    #[test]
    fn default_graph_name() {
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let mut null_map = NullMap::default();
        let graph_dv = AnyDataValue::new_iri(DEFAULT_GRAPH.to_string());

        // check that we use our own default graph IRI
        assert_eq!(
            RdfReader::datavalue_from_graph_name(&mut null_map, &mut tuple_writer, None).expect(""),
            graph_dv
        );
        // check that our default graph is a valid IRI in the first place
        assert_eq!(
            Iri::parse(DEFAULT_GRAPH.to_string()).unwrap().to_string(),
            DEFAULT_GRAPH.to_string()
        );
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
