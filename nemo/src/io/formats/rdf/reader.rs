//! Reader for various RDF formats, which supports triples files (N-Triples, Turtle, RDF/XML) and
//! quads files (N-Quads, TriG).

use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    dictionary::string_map::NullMap,
    error::ReadingError,
    management::bytesized::ByteSized,
};
use std::{cell::Cell, io::Read, mem::size_of};

use oxiri::Iri;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term};
use oxrdfio::{RdfFormat, RdfParser};

use crate::io::formats::PROGRESS_NOTIFY_INCREMENT;

use super::{
    error::RdfFormatError,
    value_format::{RdfValueFormat, RdfValueFormats},
    RdfVariant, DEFAULT_GRAPH_IRI,
};

/// A [TableProvider] for RDF 1.1 files containing triples.
pub(super) struct RdfReader {
    /// Buffer from which content is read
    read: Box<dyn Read>,
    /// RDF format
    variant: RdfVariant,
    /// Base url, if given
    base: Option<Iri<String>>,
    /// Possible [RdfValueFormat] considered when parsing
    value_formats: RdfValueFormats,
    /// If given, this reader will only consider the first `limit` entries
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
    /// Create a new [RdfReader].
    pub(super) fn new(
        read: Box<dyn Read>,
        variant: RdfVariant,
        base: Option<Iri<String>>,
        value_formats: RdfValueFormats,
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

    /// Convert [NamedNode] to [AnyDataValue].
    fn datavalue_from_named_node(value: NamedNode) -> AnyDataValue {
        AnyDataValue::new_iri(value.into_string())
    }

    /// Create a [AnyDataValue] from a [BlankNode],
    /// adding it to the given [NullMap] if new.
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
    fn datavalue_from_literal(value: Literal) -> Result<AnyDataValue, DataValueCreationError> {
        let (value, datatype, language) = value.destruct();

        if let Some(datatype) = datatype {
            AnyDataValue::new_from_typed_literal(value, datatype.into_string())
        } else if let Some(language) = language {
            Ok(AnyDataValue::new_language_tagged_string(value, language))
        } else {
            Ok(AnyDataValue::new_plain_string(value))
        }
    }

    /// Create a [AnyDataValue] for a given [Subject].
    ///
    /// This function might create new nulls and enter
    /// them into the given [NullMap].
    fn datavalue_from_subject(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Subject,
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            Subject::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Subject::BlankNode(bn) => {
                Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn))
            }
        }
    }

    /// Create a [AnyDataValue] for a given [Term].
    ///
    /// This function might create new nulls and enter
    /// them into the given [NullMap].
    fn datavalue_from_term(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: Term,
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            Term::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            Term::BlankNode(bn) => Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn)),
            Term::Literal(lit) => Ok(Self::datavalue_from_literal(lit)?),
        }
    }

    /// Create a [AnyDataValue] for a given [GraphName].
    ///
    /// This function might create new nulls and enter
    /// them into the given [NullMap].
    fn datavalue_from_graph_name(
        bnode_map: &mut NullMap,
        tuple_writer: &mut TupleWriter,
        value: GraphName,
    ) -> Result<AnyDataValue, RdfFormatError> {
        match value {
            GraphName::DefaultGraph => Ok(AnyDataValue::new_iri(DEFAULT_GRAPH_IRI.to_string())),
            GraphName::NamedNode(nn) => Ok(Self::datavalue_from_named_node(nn)),
            GraphName::BlankNode(bn) => {
                Ok(Self::datavalue_from_blank_node(bnode_map, tuple_writer, bn))
            }
        }
    }

    /// Read the RDF triples from a parser.
    fn read_triples_with_parser(
        mut self,
        tuple_writer: &mut TupleWriter,
        parser: RdfParser,
    ) -> Result<(), ReadingError> {
        log::info!("Starting RDF import (format {})", self.variant);

        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::Skip)
            .collect();

        assert_eq!(skip.len(), 3);
        assert_eq!(
            tuple_writer.column_number(),
            skip.iter().filter(|b| !*b).count()
        );

        let stop_limit = self.limit.unwrap_or(u64::MAX);
        let triple_count = Cell::new(0);

        // [RdfParser] always returns quads, but we ignore the graph
        // name here (since it will always be
        // [GraphName::DefaultGraph]).
        let mut on_quad = |quad: Quad| {
            // This is needed since a parser might process several RDF statements
            // before giving us a chance to stop in the outer loop.
            if triple_count.get() == stop_limit {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }

            if !skip[0] {
                let subject =
                    Self::datavalue_from_subject(&mut self.bnode_map, tuple_writer, quad.subject)?;
                tuple_writer.add_tuple_value(subject);
            }
            if !skip[1] {
                let predicate = Self::datavalue_from_named_node(quad.predicate);
                tuple_writer.add_tuple_value(predicate);
            }
            if !skip[2] {
                let object =
                    Self::datavalue_from_term(&mut self.bnode_map, tuple_writer, quad.object)?;
                tuple_writer.add_tuple_value(object);
            }

            triple_count.set(triple_count.get() + 1);
            if triple_count.get().is_multiple_of(PROGRESS_NOTIFY_INCREMENT) {
                log::info!("... processed {} triples", triple_count.get())
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        for quad in parser.for_reader(self.read) {
            match quad {
                Ok(triple) => {
                    if let Err(e) = on_quad(triple) {
                        log::info!("Ignoring malformed RDF: {e}");
                    }
                }
                Err(e) => {
                    log::info!("Ignoring malformed RDF: {e}");
                }
            }

            if triple_count.get() == stop_limit {
                break;
            }
        }

        log::info!("Finished import: processed {} triples", triple_count.get());

        Ok(())
    }

    /// Read the RDF quads from a parser.
    fn read_quads_with_parser(
        mut self,
        tuple_writer: &mut TupleWriter,
        parser: RdfParser,
    ) -> Result<(), ReadingError> {
        log::info!("Starting RDF import (format {})", self.variant);

        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == RdfValueFormat::Skip)
            .collect();

        assert_eq!(skip.len(), 4);
        assert_eq!(
            tuple_writer.column_number(),
            skip.iter().filter(|b| !*b).count()
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
            if quad_count.get().is_multiple_of(PROGRESS_NOTIFY_INCREMENT) {
                log::info!("... processed {} triples", quad_count.get())
            }

            Ok::<_, Box<dyn std::error::Error>>(())
        };

        for quad in parser.for_reader(self.read) {
            match quad {
                Ok(quad) => {
                    if let Err(e) = on_quad(quad) {
                        log::info!("Ignoring malformed RDF: {e}");
                    }
                }
                Err(e) => {
                    log::info!("Ignoring malformed RDF: {e}");
                }
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
    ) -> Result<(), ReadingError> {
        let base_iri = self.base.clone();
        let with_base_iri = |parser: RdfParser| {
            if let Some(base) = base_iri {
                parser
                    .with_base_iri(base.to_string())
                    .expect("should be a valid IRI")
            } else {
                parser
            }
        };

        match &self.variant {
            RdfVariant::NTriples => self.read_triples_with_parser(
                tuple_writer,
                RdfParser::from_format(RdfFormat::NTriples),
            ),

            RdfVariant::NQuads => {
                self.read_quads_with_parser(tuple_writer, RdfParser::from_format(RdfFormat::NQuads))
            }
            RdfVariant::Turtle => self.read_triples_with_parser(
                tuple_writer,
                with_base_iri(RdfParser::from_format(RdfFormat::Turtle)),
            ),
            RdfVariant::RDFXML => self.read_triples_with_parser(
                tuple_writer,
                with_base_iri(RdfParser::from_format(RdfFormat::RdfXml)),
            ),
            RdfVariant::TriG => self.read_quads_with_parser(
                tuple_writer,
                with_base_iri(RdfParser::from_format(RdfFormat::TriG)),
            ),
        }
    }

    fn arity(&self) -> usize {
        self.value_formats.arity()
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
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

#[cfg(test)]
mod test {
    use super::{RdfReader, DEFAULT_GRAPH_IRI};
    use std::cell::RefCell;

    use nemo_physical::{
        datasources::tuple_writer::TupleWriter, datavalues::AnyDataValue,
        dictionary::string_map::NullMap, management::database::Dict,
    };
    use oxiri::Iri;
    use oxrdf::GraphName;
    use oxrdfio::{RdfFormat, RdfParser};
    #[cfg(not(miri))]
    use test_log::test;

    use crate::io::formats::rdf::{value_format::RdfValueFormats, RdfVariant};

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
            RdfValueFormats::default(3),
            None,
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader.read_triples_with_parser(
            &mut tuple_writer,
            RdfParser::from_format(RdfFormat::NTriples),
        );
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
            RdfValueFormats::default(3),
            None,
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader
            .read_triples_with_parser(&mut tuple_writer, RdfParser::from_format(RdfFormat::Turtle));
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
            RdfValueFormats::default(3),
            None,
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let result = reader.read_triples_with_parser(
            &mut tuple_writer,
            RdfParser::from_format(RdfFormat::NTriples),
        );
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 1);
    }

    #[test]
    fn default_graph_name() {
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 3);
        let mut null_map = NullMap::default();
        let graph_dv = AnyDataValue::new_iri(DEFAULT_GRAPH_IRI.to_string());

        // check that we use our own default graph IRI
        assert_eq!(
            RdfReader::datavalue_from_graph_name(
                &mut null_map,
                &mut tuple_writer,
                GraphName::DefaultGraph
            )
            .expect(""),
            graph_dv
        );
        // check that our default graph is a valid IRI in the first place
        assert_eq!(
            Iri::parse(DEFAULT_GRAPH_IRI.to_string())
                .unwrap()
                .to_string(),
            DEFAULT_GRAPH_IRI.to_string()
        );
    }
}
