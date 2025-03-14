//! The writer for RDF files.

use std::io::{BufWriter, Write};

use nemo_physical::datavalues::{AnyDataValue, DataValue, ValueDomain};
use oxrdf::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, QuadRef, SubjectRef, TermRef, TripleRef,
};
use oxrdfio::{RdfFormat, RdfSerializer};

use crate::{
    error::Error,
    io::formats::{TableWriter, PROGRESS_NOTIFY_INCREMENT},
};

use super::{
    value_format::{RdfValueFormat, RdfValueFormats},
    RdfVariant, DEFAULT_GRAPH_IRI,
};

/// Private struct to record the type of an RDF term that
/// is to be created on demand.
#[derive(Debug, Default)]
enum RdfTermType {
    #[default]
    Iri,
    BNode,
    TypedLiteral,
    LangString,
    SimpleStringLiteral,
}

#[derive(Debug, Default)]
enum QuadGraphName {
    #[default]
    DefaultGraph,
    NamedNode(String),
    BlankNode(String),
}

#[derive(Debug)]
struct InvalidGraphNameError;

impl TryFrom<&AnyDataValue> for QuadGraphName {
    type Error = InvalidGraphNameError;

    fn try_from(value: &AnyDataValue) -> Result<Self, Self::Error> {
        match value.value_domain() {
            ValueDomain::Iri => {
                let iri = value.to_iri_unchecked();

                if iri == DEFAULT_GRAPH_IRI {
                    Ok(Self::DefaultGraph)
                } else {
                    Ok(Self::NamedNode(iri))
                }
            }
            ValueDomain::Null => Ok(Self::BlankNode(value.lexical_value())),
            _ => Err(InvalidGraphNameError),
        }
    }
}

/// Struct to store information of one quad (or triple) for export.
/// This is necessary since all RIO RDF term implementations use `&str`
/// pointers internally, that must be owned elsewhere.
#[derive(Debug, Default)]
struct QuadBuffer {
    graph_name: QuadGraphName,
    subject_is_blank: bool,
    subject: String,
    predicate: String,
    object_part1: String,
    object_part2: String,
    object_type: RdfTermType,
}
impl<'a> QuadBuffer {
    fn subject(&'a self) -> SubjectRef<'a> {
        if self.subject_is_blank {
            SubjectRef::BlankNode(BlankNodeRef::new_unchecked(&self.subject))
        } else {
            SubjectRef::NamedNode(NamedNodeRef::new_unchecked(&self.subject))
        }
    }

    fn predicate(&'a self) -> NamedNodeRef<'a> {
        NamedNodeRef::new_unchecked(&self.predicate)
    }

    fn object(&'a self) -> TermRef<'a> {
        match self.object_type {
            RdfTermType::Iri => TermRef::NamedNode(NamedNodeRef::new_unchecked(&self.object_part1)),
            RdfTermType::BNode => {
                TermRef::BlankNode(BlankNodeRef::new_unchecked(&self.object_part1))
            }
            RdfTermType::TypedLiteral => TermRef::Literal(LiteralRef::new_typed_literal(
                &self.object_part1,
                NamedNodeRef::new_unchecked(&self.object_part2),
            )),
            RdfTermType::LangString => {
                TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                    &self.object_part1,
                    &self.object_part2,
                ))
            }
            RdfTermType::SimpleStringLiteral => {
                TermRef::Literal(LiteralRef::new_simple_literal(&self.object_part1))
            }
        }
    }

    fn graph_name(&'a self) -> GraphNameRef<'a> {
        match &self.graph_name {
            QuadGraphName::DefaultGraph => GraphNameRef::DefaultGraph,
            QuadGraphName::NamedNode(iri) => {
                GraphNameRef::NamedNode(NamedNodeRef::new_unchecked(iri))
            }
            QuadGraphName::BlankNode(id) => {
                GraphNameRef::BlankNode(BlankNodeRef::new_unchecked(id))
            }
        }
    }

    fn set_subject_from_datavalue(&mut self, datavalue: &AnyDataValue) -> bool {
        match datavalue.value_domain() {
            ValueDomain::Iri => {
                self.subject = datavalue.to_iri_unchecked();
                self.subject_is_blank = false;
                true
            }
            ValueDomain::Null => {
                self.subject = datavalue.lexical_value();
                self.subject_is_blank = true;
                true
            }
            _ => false,
        }
    }

    fn set_predicate_from_datavalue(&mut self, datavalue: &AnyDataValue) -> bool {
        match datavalue.value_domain() {
            ValueDomain::Iri => {
                self.predicate = datavalue.to_iri_unchecked();
                true
            }
            _ => false,
        }
    }

    fn set_object_from_datavalue(&mut self, datavalue: &AnyDataValue) -> bool {
        match datavalue.value_domain() {
            ValueDomain::PlainString => {
                self.object_type = RdfTermType::SimpleStringLiteral;
                self.object_part1 = datavalue.to_plain_string_unchecked();
            }
            ValueDomain::LanguageTaggedString => {
                self.object_type = RdfTermType::LangString;
                (self.object_part1, self.object_part2) =
                    datavalue.to_language_tagged_string_unchecked();
            }
            ValueDomain::Iri => {
                self.object_type = RdfTermType::Iri;
                self.object_part1 = datavalue.to_iri_unchecked();
            }
            ValueDomain::Float
            | ValueDomain::Double
            | ValueDomain::UnsignedLong
            | ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int
            | ValueDomain::Boolean
            | ValueDomain::Other => {
                self.object_type = RdfTermType::TypedLiteral;
                self.object_part1 = datavalue.lexical_value();
                self.object_part2 = datavalue.datatype_iri();
            }
            ValueDomain::Tuple => {
                return false;
            }
            ValueDomain::Map => {
                return false;
            }
            ValueDomain::Null => {
                self.object_type = RdfTermType::BNode;
                self.object_part1 = datavalue.lexical_value();
                return true;
            }
        }
        true
    }

    fn set_graph_name_from_datavalue(
        &mut self,
        datavalue: &AnyDataValue,
    ) -> Result<(), InvalidGraphNameError> {
        self.graph_name = QuadGraphName::try_from(datavalue)?;

        Ok(())
    }

    fn triple_ref(&'a self) -> TripleRef<'a> {
        TripleRef::new(self.subject(), self.predicate(), self.object())
    }

    fn quad_ref(&'a self) -> QuadRef<'a> {
        QuadRef::new(
            self.subject(),
            self.predicate(),
            self.object(),
            self.graph_name(),
        )
    }
}

/// A writer object for writing RDF files.
pub(super) struct RdfWriter {
    writer: Box<dyn Write>,
    variant: RdfVariant,
    value_formats: RdfValueFormats,
    limit: Option<u64>,
}

impl RdfWriter {
    pub(super) fn new(
        writer: Box<dyn Write>,
        variant: RdfVariant,
        value_formats: RdfValueFormats,
        limit: Option<u64>,
    ) -> Self {
        RdfWriter {
            writer,
            variant,
            value_formats,
            limit,
        }
    }

    fn export_triples<'a>(
        self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
        serializer: RdfSerializer,
    ) -> Result<(), Error> {
        log::info!("Starting RDF export (format {})", self.variant);

        let mut triple_pos = [0; 3];
        let mut cur = 0;
        for (idx, format) in self.value_formats.iter().enumerate() {
            if *format != RdfValueFormat::Skip {
                assert!(cur <= 2); // max number of non-skip entries is 3
                triple_pos[cur] = idx;
                cur += 1;
            }
        }
        assert_eq!(cur, 3); // three triple components found
        let [s_pos, p_pos, o_pos] = triple_pos;

        let mut serializer = serializer.for_writer(BufWriter::new(self.writer));
        let mut buffer: QuadBuffer = Default::default();

        let stop_limit = self.limit.unwrap_or(u64::MAX);
        let mut triple_count: u64 = 0;
        let mut drop_count: u64 = 0;

        for record in table {
            assert_eq!(record.len(), self.value_formats.len());

            if !buffer.set_subject_from_datavalue(&record[s_pos]) {
                continue;
            }

            if !buffer.set_predicate_from_datavalue(&record[p_pos]) {
                continue;
            }

            if !buffer.set_object_from_datavalue(&record[o_pos]) {
                continue;
            }

            if let Err(e) = serializer.serialize_triple(buffer.triple_ref()) {
                log::debug!("failed to write triple: {e}");
                drop_count += 1;
            } else {
                triple_count += 1;

                if (triple_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                    log::info!("... processed {triple_count} triples");
                }

                if triple_count == stop_limit {
                    break;
                }
            }
        }

        serializer.finish()?;

        log::info!("Finished export: processed {triple_count} triples (dropped {drop_count})");

        Ok(())
    }

    fn export_quads<'a>(
        self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
        serializer: RdfSerializer,
    ) -> Result<(), Error> {
        log::info!("Starting RDF export (format {})", self.variant);

        let mut quad_pos = [0; 4];
        let mut cur = 0;
        for (idx, format) in self.value_formats.iter().enumerate() {
            if *format != RdfValueFormat::Skip {
                assert!(cur <= 3); // max number of non-skip entries is 3
                quad_pos[cur] = idx;
                cur += 1;
            }
        }
        assert_eq!(cur, 4); // three triple components found
        let [g_pos, s_pos, p_pos, o_pos] = quad_pos;

        let mut serializer = serializer.for_writer(BufWriter::new(self.writer));
        let mut buffer: QuadBuffer = Default::default();

        let stop_limit = self.limit.unwrap_or(u64::MAX);
        let mut quad_count: u64 = 0;
        let mut drop_count: u64 = 0;

        for record in table {
            assert_eq!(record.len(), self.value_formats.len());

            if !buffer.set_subject_from_datavalue(&record[s_pos]) {
                continue;
            }

            if !buffer.set_predicate_from_datavalue(&record[p_pos]) {
                continue;
            }

            if !buffer.set_object_from_datavalue(&record[o_pos]) {
                continue;
            }

            if buffer
                .set_graph_name_from_datavalue(&record[g_pos])
                .is_err()
            {
                continue;
            }

            if let Err(e) = serializer.serialize_quad(buffer.quad_ref()) {
                log::debug!("failed to write quad: {e}");
                drop_count += 1;
            } else {
                quad_count += 1;

                if (quad_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                    log::info!("... processed {quad_count} triples");
                }

                if quad_count == stop_limit {
                    break;
                }
            }
        }

        serializer.finish()?;

        log::info!("Finished export: processed {quad_count} quads (dropped {drop_count})");

        Ok(())
    }
}

impl TableWriter for RdfWriter {
    fn export_table_data<'a>(
        self: Box<Self>,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        match self.variant {
            RdfVariant::NTriples => {
                self.export_triples(table, RdfSerializer::from_format(RdfFormat::NTriples))
            }
            RdfVariant::NQuads => {
                self.export_quads(table, RdfSerializer::from_format(RdfFormat::NQuads))
            }
            RdfVariant::Turtle => {
                self.export_triples(table, RdfSerializer::from_format(RdfFormat::Turtle))
            }
            RdfVariant::RDFXML => {
                self.export_triples(table, RdfSerializer::from_format(RdfFormat::RdfXml))
            }
            RdfVariant::TriG => {
                self.export_quads(table, RdfSerializer::from_format(RdfFormat::TriG))
            }
        }
    }
}

impl std::fmt::Debug for RdfWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdfWriter")
            .field("write", &"<unspecified std::io::Read>")
            .finish()
    }
}
