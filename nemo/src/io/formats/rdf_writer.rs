//! The writer for RDF files.

use nemo_physical::datavalues::{AnyDataValue, DataValue, ValueDomain};
use rio_api::{
    formatter::{QuadsFormatter, TriplesFormatter},
    model::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term, Triple},
};
use rio_turtle::{NQuadsFormatter, NTriplesFormatter, TriGFormatter, TurtleFormatter};
use rio_xml::RdfXmlFormatter;
use std::io::Write;

use super::{rdf::RdfValueFormat, types::TableWriter};
use crate::{error::Error, io::formats::PROGRESS_NOTIFY_INCREMENT, model::RdfVariant};

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

/// Struct to store information of one quad (or triple) for export.
/// This is necessary since all RIO RDF term implementations use `&str`
/// pointers internally, that must be owned elsewhere.
#[derive(Debug, Default)]
struct QuadBuffer {
    graph_name_is_blank: bool,
    graph_name: String,
    subject_is_blank: bool,
    subject: String,
    predicate: String,
    object_part1: String,
    object_part2: String,
    object_type: RdfTermType,
}
impl<'a> QuadBuffer {
    fn subject(&'a self) -> Subject<'a> {
        if self.subject_is_blank {
            Subject::BlankNode(BlankNode {
                id: self.subject.as_str(),
            })
        } else {
            Subject::NamedNode(NamedNode {
                iri: self.subject.as_str(),
            })
        }
    }

    fn predicate(&'a self) -> NamedNode<'a> {
        NamedNode {
            iri: self.predicate.as_str(),
        }
    }

    fn object(&'a self) -> Term<'a> {
        match self.object_type {
            RdfTermType::Iri => Term::NamedNode(NamedNode {
                iri: self.object_part1.as_str(),
            }),
            RdfTermType::BNode => Term::BlankNode(BlankNode {
                id: self.object_part1.as_str(),
            }),
            RdfTermType::TypedLiteral => Term::Literal(Literal::Typed {
                value: self.object_part1.as_str(),
                datatype: NamedNode {
                    iri: self.object_part2.as_str(),
                },
            }),
            RdfTermType::LangString => Term::Literal(Literal::LanguageTaggedString {
                value: self.object_part1.as_str(),
                language: self.object_part2.as_str(),
            }),
            RdfTermType::SimpleStringLiteral => Term::Literal(Literal::Simple {
                value: self.object_part1.as_str(),
            }),
        }
    }

    fn graph_name(&'a self) -> GraphName<'a> {
        if self.graph_name_is_blank {
            GraphName::BlankNode(BlankNode {
                id: self.graph_name.as_str(),
            })
        } else {
            GraphName::NamedNode(NamedNode {
                iri: self.graph_name.as_str(),
            })
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

    fn set_graph_name_from_datavalue(&mut self, datavalue: &AnyDataValue) -> bool {
        match datavalue.value_domain() {
            ValueDomain::Iri => {
                self.graph_name = datavalue.to_iri_unchecked();
                self.graph_name_is_blank = false;
                true
            }
            ValueDomain::Null => {
                self.graph_name = datavalue.lexical_value();
                self.graph_name_is_blank = true;
                true
            }
            _ => false,
        }
    }
}

/// A writer object for writing RDF files.
pub(super) struct RdfWriter {
    writer: Box<dyn Write>,
    variant: RdfVariant,
    value_formats: Vec<RdfValueFormat>,
}

impl RdfWriter {
    pub(super) fn new(
        writer: Box<dyn Write>,
        variant: RdfVariant,
        value_formats: Vec<RdfValueFormat>,
    ) -> Self {
        RdfWriter {
            writer,
            variant,
            value_formats,
        }
    }

    fn export_triples<'a, Formatter>(
        self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
        make_formatter: impl Fn(Box<dyn Write>) -> std::io::Result<Formatter>,
        finish_formatter: impl Fn(Formatter),
    ) -> Result<(), Error>
    where
        Formatter: TriplesFormatter,
    {
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

        let mut formatter = make_formatter(self.writer)?;
        let mut buffer: QuadBuffer = Default::default();

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
            if let Err(e) = formatter.format(&Triple {
                subject: buffer.subject(),
                predicate: buffer.predicate(),
                object: buffer.object(),
            }) {
                log::debug!("failed to write triple: {e}");
                drop_count += 1;
            } else {
                triple_count += 1;
                if (triple_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                    log::info!("... processed {triple_count} triples");
                }
            }
        }
        finish_formatter(formatter);

        log::info!("Finished export: processed {triple_count} triples (dropped {drop_count})");

        Ok(())
    }

    fn export_quads<'a, Formatter>(
        self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
        make_formatter: impl Fn(Box<dyn Write>) -> std::io::Result<Formatter>,
        finish_formatter: impl Fn(Formatter),
    ) -> Result<(), Error>
    where
        Formatter: QuadsFormatter,
    {
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

        let mut formatter = make_formatter(self.writer)?;
        let mut buffer: QuadBuffer = Default::default();

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
            if !buffer.set_graph_name_from_datavalue(&record[g_pos]) {
                continue;
            }
            if let Err(e) = formatter.format(&Quad {
                subject: buffer.subject(),
                predicate: buffer.predicate(),
                object: buffer.object(),
                graph_name: Some(buffer.graph_name()),
            }) {
                log::debug!("failed to write quad: {e}");
                drop_count += 1;
            } else {
                quad_count += 1;
                if (quad_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                    log::info!("... processed {quad_count} triples");
                }
            }
        }
        finish_formatter(formatter);

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
            RdfVariant::NTriples => self.export_triples(
                table,
                |write| Ok(NTriplesFormatter::new(write)),
                |f| {
                    let _ = f.finish();
                },
            ),
            RdfVariant::NQuads => self.export_quads(
                table,
                |write| Ok(NQuadsFormatter::new(write)),
                |f| {
                    let _ = f.finish();
                },
            ),
            RdfVariant::Turtle => self.export_triples(
                table,
                |write| Ok(TurtleFormatter::new(write)),
                |f| {
                    let _ = f.finish();
                },
            ),
            RdfVariant::RDFXML => self.export_triples(table, RdfXmlFormatter::new, |f| {
                let _ = f.finish();
            }),
            RdfVariant::TriG => self.export_quads(
                table,
                |write| Ok(TriGFormatter::new(write)),
                |f| {
                    let _ = f.finish();
                },
            ),
            RdfVariant::Unspecified => unreachable!(
                "the writer should not be instantiated with unknown format by the handler"
            ),
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
