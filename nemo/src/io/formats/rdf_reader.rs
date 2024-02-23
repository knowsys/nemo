//! Reader for various RDF formats, which supports triples files (N-Triples, Turtle, RDF/XML) and
//! quads files (N-Quads, TriG).
use bytesize::ByteSize;
use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    dictionary::string_map::NullMap,
    management::bytesized::ByteSized,
};
use std::{io::BufRead, mem::size_of};

use oxiri::Iri;
use rio_api::{
    model::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term, Triple},
    parser::{QuadsParser, TriplesParser},
};
use rio_turtle::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::{io::formats::PROGRESS_NOTIFY_INCREMENT, model::RdfVariant};

use super::rdf::{RdfFormatError, RdfValueFormat};

const DEFAULT_GRAPH: &str = "__DEFAULT_GRAPH__";

/// A [`TableProvider`] for RDF 1.1 files containing triples.
pub(super) struct RdfReader {
    read: Box<dyn BufRead>,
    variant: RdfVariant,
    base: Option<Iri<String>>,
    value_formats: Vec<RdfValueFormat>,
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
    pub(super) fn new(
        read: Box<dyn BufRead>,
        variant: RdfVariant,
        base: Option<Iri<String>>,
        value_formats: Vec<RdfValueFormat>,
    ) -> Self {
        Self {
            read,
            variant,
            base,
            value_formats,
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
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::SKIP)
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

        let mut triple_count = 0;

        let mut on_triple = |triple: Triple| {
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
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::SKIP)
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

        let mut quad_count = 0;

        let mut on_triple = |quad: Quad| {
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

#[cfg(test)]
mod test {
    use super::RdfReader;
    use std::cell::RefCell;

    use nemo_physical::{datasources::tuple_writer::TupleWriter, management::database::Dict};
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
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
            ],
        );
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

        let reader = RdfReader::new(
            Box::new(data),
            RdfVariant::Turtle,
            None,
            vec![
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
            ],
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
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
                RdfValueFormat::ANYTHING,
            ],
        );
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
