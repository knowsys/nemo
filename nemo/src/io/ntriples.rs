//! Reading of RDF 1.1 N-Triples files
use std::{
    io::{BufRead, BufReader, Read},
    path::PathBuf,
};

use nemo_physical::{
    builder_proxy::PhysicalBuilderProxyEnum, error::ReadingError, table_reader::TableReader,
};

use crate::{read_from_possibly_compressed_file, types::LogicalTypeEnum};

/// A [`TableReader`] for RDF 1.1 N-Triples files.
#[derive(Debug, Clone)]
pub struct NTriplesReader {
    file: PathBuf,
}

impl NTriplesReader {
    /// Create a new [`NTriplesReader`] for the given [`file`][`PathBuf`]
    pub fn new(file: PathBuf) -> Self {
        Self { file }
    }

    fn read_with_buf_reader<'a, 'b, R>(
        &self,
        physical_builder_proxies: &'b mut [PhysicalBuilderProxyEnum<'a>],
        reader: &mut BufReader<R>,
    ) -> Result<(), ReadingError>
    where
        'a: 'b,
        R: Read,
    {
        let mut builders = physical_builder_proxies
            .iter_mut()
            .map(|physical| LogicalTypeEnum::Any.wrap_physical_column_builder(physical))
            .collect::<Vec<_>>();

        assert!(builders.len() == 3);

        for (row, line) in reader.lines().enumerate() {
            let line = line.map_err(ReadingError::from)?;
            // split line
            match crate::io::parser::ntriples::triple(line.as_str().into()) {
                Ok(None) => (), // line was a comment, ignore
                Ok(Some((subject, predicate, object))) => {
                    builders[0]
                        .add(subject.to_string())
                        .expect("we have verified that this is a valid IRI or bnode");
                    builders[1]
                        .add(predicate.to_string())
                        .expect("we have verified that this is a valid IRI");
                    builders[2]
                        .add(object.to_string())
                        .expect("we have verified that this is a valid IRI, bnode, or RDF literal");
                }
                Err(e) => {
                    log::info!("Ignoring line {row:?}, parsing failed with: {e}");
                }
            }
        }

        todo!()
    }
}

impl TableReader for NTriplesReader {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        &self,
        builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        read_from_possibly_compressed_file!(self.file, |reader| {
            self.read_with_buf_reader(builder_proxies, &mut BufReader::new(reader))
        })
    }
}
