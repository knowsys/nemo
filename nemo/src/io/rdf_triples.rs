//! Reading of RDF 1.1 triples files (N-Triples, Turtle, RDF/XML)
use std::io::{BufRead, BufReader};

use nemo_physical::{
    builder_proxy::PhysicalBuilderProxyEnum,
    error::ReadingError,
    table_reader::{Resource, TableReader},
};
use rio_api::{model::Triple, parser::TriplesParser};
use rio_turtle::{NTriplesParser, TurtleParser};
use rio_xml::RdfXmlParser;

use crate::types::LogicalTypeEnum;

use super::input_manager::ResourceProviders;

/// A [`TableReader`] for RDF 1.1 files containing triples.
#[derive(Debug, Clone)]
pub struct RDFTriplesReader {
    resource_providers: ResourceProviders,
    resource: Resource,
}

impl RDFTriplesReader {
    /// Create a new [`RDFTriplesReader`]
    pub fn new(resource_providers: ResourceProviders, resource: Resource) -> Self {
        Self {
            resource_providers,
            resource,
        }
    }

    fn read_with_buf_reader<'a, 'b, Reader, Parser, MakeParser>(
        &self,
        physical_builder_proxies: &'b mut [PhysicalBuilderProxyEnum<'a>],
        reader: &'b mut Reader,
        make_parser: MakeParser,
    ) -> Result<(), ReadingError>
    where
        'a: 'b,
        Reader: BufRead,
        Parser: TriplesParser,
        MakeParser: FnOnce(&'b mut Reader) -> Parser,
        ReadingError: From<<Parser as TriplesParser>::Error>,
    {
        let mut builders = physical_builder_proxies
            .iter_mut()
            .map(|physical| LogicalTypeEnum::Any.wrap_physical_column_builder(physical))
            .collect::<Vec<_>>();

        assert!(builders.len() == 3);

        let mut on_triple = |triple: Triple| {
            builders[0].add(triple.subject.to_string())?;
            builders[1].add(triple.predicate.to_string())?;
            builders[2].add(triple.object.to_string())
        };

        let mut parser = make_parser(reader);

        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                log::info!("Ignoring malformed triple: {e}");
            }
        }

        Ok(())
    }
}

impl TableReader for RDFTriplesReader {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        &self,
        builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let mut reader = BufReader::new(reader);

        if self.resource.ends_with(".ttl.gz") || self.resource.ends_with(".ttl") {
            self.read_with_buf_reader(builder_proxies, &mut reader, |reader| {
                TurtleParser::new(reader, None)
            })
        } else if self.resource.ends_with(".rdf.gz") || self.resource.ends_with(".rdf") {
            self.read_with_buf_reader(builder_proxies, &mut reader, |reader| {
                RdfXmlParser::new(reader, None)
            })
        } else {
            self.read_with_buf_reader(builder_proxies, &mut reader, NTriplesParser::new)
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use nemo_physical::{
        builder_proxy::{PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy},
        dictionary::{Dictionary, PrefixedStringDictionary},
    };
    use rio_turtle::TurtleParser;
    use test_log::test;

    use super::*;

    #[test]
    fn example_1() {
        macro_rules! parse_example_with_rdf_parser {
            ($make_parser:expr) => {
                let mut data = r#"<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here
                      # or on a line by themselves
                      _:subject1 <http://an.example/predicate1> "object1" .
                      _:subject2 <http://an.example/predicate2> "object2" .
                      "#.as_bytes();

                let dict = RefCell::new(PrefixedStringDictionary::default());
                let mut builders = vec![
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                    PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
                ];
                let reader = RDFTriplesReader::new(ResourceProviders::empty(), String::from(""));

                let result = reader.read_with_buf_reader(&mut builders, &mut data, $make_parser);
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
                                    .and_then(|usize| dict.borrow_mut().entry(usize))
                                    .unwrap()
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                log::debug!("triple: {triples:?}");
                assert_eq!(
                    triples[0],
                    vec![
                        "http://one.example/subject1",
                        "http://one.example/predicate1",
                        "http://one.example/object1"
                    ]
                );
                assert_eq!(
                    triples[1],
                    vec!["_:subject1", "http://an.example/predicate1", r#""object1""#]
                );
                assert_eq!(
                    triples[2],
                    vec!["_:subject2", "http://an.example/predicate2", r#""object2""#]
                );
            };
        }

        parse_example_with_rdf_parser!(NTriplesParser::new);
        parse_example_with_rdf_parser!(|reader| TurtleParser::new(reader, None));
    }
}
