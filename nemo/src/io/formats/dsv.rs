//! Reading of delimiter-separated value files

use std::collections::HashSet;
use std::io::{BufReader, Read};

use csv::{Reader, ReaderBuilder, WriterBuilder};
use nemo_physical::datavalues::DataValue;
use thiserror::Error;

use oxiri::Iri;
use rio_api::model::{Term, Triple};
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleParser;

use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    resource::Resource,
};

use crate::{
    error::Error,
    io::{
        formats::{
            rdf::RDFReader,
            types::{
                Direction, ExportSpec, FileFormat, FileFormatError, FileFormatMeta,
                ImportExportSpec, ImportSpec, TableWriter,
            },
            PROGRESS_NOTIFY_INCREMENT,
        },
        parser::{parse_bare_name, span_from_str},
        resource_providers::ResourceProviders,
    },
    model::{Constant, Identifier, Key, Map, PrimitiveType, TupleConstraint, TypeConstraint},
};

type DataValueParserFunction = fn(String) -> Result<AnyDataValue, DataValueCreationError>;

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// Parsing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// which can be set for each column separately.
#[derive(Debug)]
pub(crate) struct DsvReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    delimiter: u8,
    escape: u8,
    input_type_constraint: TupleConstraint,
}

impl DsvReader {
    /// Instantiate a [`DsvReader`] for a given delimiter
    pub fn new(
        resource_providers: ResourceProviders,
        resource: Resource,
        delimiter: u8,
        input_type_constraint: TupleConstraint,
    ) -> Self {
        Self {
            resource_providers,
            resource,
            delimiter,
            escape: b'\\',
            input_type_constraint,
        }
    }

    /// Static function to create a CSV reader
    ///
    /// The function takes an arbitrary [`Reader`][Read] and wraps it into a [`Reader`][csv::Reader] for csv
    fn dsv_reader<R>(reader: R, delimiter: u8, escape: Option<u8>) -> Reader<R>
    where
        R: Read,
    {
        ReaderBuilder::new()
            .delimiter(delimiter)
            .escape(escape)
            .has_headers(false)
            .double_quote(true)
            .from_reader(reader)
    }

    /// Make a list of pareser functions to be used for ingesting the data in each column.
    fn make_parsers(&self) -> Vec<DataValueParserFunction> {
        let mut result = Vec::with_capacity(self.input_type_constraint.arity());
        for ty in self.input_type_constraint.iter() {
            match ty {
                TypeConstraint::Exact(PrimitiveType::Any)
                | TypeConstraint::AtLeast(PrimitiveType::Any) =>
                {
                    #[allow(trivial_casts)]
                    result.push(Self::parse_any_value_from_string as DataValueParserFunction)
                }
                TypeConstraint::Exact(PrimitiveType::String)
                | TypeConstraint::AtLeast(PrimitiveType::String) =>
                {
                    #[allow(trivial_casts)]
                    result.push(Self::parse_string_from_string as DataValueParserFunction)
                }
                TypeConstraint::Exact(PrimitiveType::Integer)
                | TypeConstraint::AtLeast(PrimitiveType::Integer) =>
                {
                    #[allow(trivial_casts)]
                    result.push(AnyDataValue::new_from_integer_literal as DataValueParserFunction)
                }
                TypeConstraint::Exact(PrimitiveType::Float64)
                | TypeConstraint::AtLeast(PrimitiveType::Float64) =>
                {
                    #[allow(trivial_casts)]
                    result.push(AnyDataValue::new_from_double_literal as DataValueParserFunction)
                }
                TypeConstraint::None => unreachable!(
                    "Type constraints for input types are always initialized (with fallbacks)."
                ),
                TypeConstraint::Tuple(_) => {
                    todo!("We do not support tuples in CSV currently. Should we?")
                }
            }
        }
        result
    }

    /// Actually reads the data from the file, using the given parsers to convert strings to [`AnyDataValue`]s.
    /// If a field cannot be read or parsed, the line will be ignored
    fn read<R>(
        &self,
        tuple_writer: &mut TupleWriter,
        dsv_reader: &mut Reader<R>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        R: Read,
    {
        let parsers = self.make_parsers();

        let mut line_count: u64 = 0;
        let mut drop_count: u64 = 0;

        for row in dsv_reader.records().flatten() {
            for idx_field in row.iter().enumerate() {
                if let Ok(dv) = parsers[idx_field.0](idx_field.1.to_string()) {
                    tuple_writer.add_tuple_value(dv);
                } else {
                    drop_count += 1;
                    tuple_writer.drop_current_tuple();
                    break;
                }
            }

            line_count += 1;
            if (line_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                log::info!("loading: processed {line_count} lines");
            }
        }
        log::info!("Finished loading: processed {line_count} lines (dropped {drop_count})");

        Ok(())
    }

    /// Simple wrapper function that makes CSV strings into [`AnyDataValue`]. We wrap this
    /// to match the error-producing signature of other parsing functions.
    fn parse_string_from_string(input: String) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue::new_string(input))
    }

    /// Best-effort parsing function for strings from CSV. True to the nature of CSV, this function
    /// will try hard to find a usable value in the string.
    fn parse_any_value_from_string(input: String) -> Result<AnyDataValue, DataValueCreationError> {
        const BASE: &str = "a:";

        let trimmed = input.trim();

        // Represent empty cells as empty strings
        if trimmed.is_empty() {
            return Ok(AnyDataValue::new_string("".to_string()));
        }

        // Try to interpret value as RDF term using the RIO parser
        let data = format!("<> <> {trimmed}.");
        let mut parser = TurtleParser::new(
            BufReader::new(data.as_bytes()),
            Iri::parse(BASE.to_string()).ok(),
        );

        let mut result: Option<Result<AnyDataValue, DataValueCreationError>> = None;
        let mut triple_count = 0;
        let mut on_triple = |triple: Triple| {
            triple_count += 1;
            match triple.object {
                Term::NamedNode(nn) => {
                    if let Some(s) = nn.iri.to_string().strip_prefix(BASE) {
                        result = Some(Ok(AnyDataValue::new_iri(s.to_string())));
                    } else {
                        result = Some(Ok(AnyDataValue::new_iri(nn.iri.to_string())));
                    }
                }
                Term::BlankNode(_) => {
                    // do not support blank nodes in CSV (continue processing)
                }
                Term::Literal(lit) => {
                    result = Some(RDFReader::datavalue_from_literal(lit));
                }
                Term::Triple(_) => {
                    // do not support RDF* syntax in CSV (continue processing)
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let _ = parser.parse_all(&mut on_triple); // ignore errors; we will see this next anyhow
        if triple_count == 1 {
            if let Some(res) = result {
                return res;
            }
        }

        // Not a valid RDF term.
        // Check if it's a valid bare name
        // TODO: Assess whether this adds anything useful on top of the local IRIs supported in RDF.
        if let Ok((remainder, _)) = parse_bare_name(span_from_str(trimmed)) {
            if remainder.is_empty() {
                return Ok(AnyDataValue::new_iri(trimmed.to_string()));
            }
        }

        // Might still be a full IRI
        if let Ok(iri) = Iri::parse(trimmed) {
            return Ok(AnyDataValue::new_iri(iri.to_string()));
        }

        // Otherwise treat the input as a string literal
        Ok(AnyDataValue::new_string(trimmed.to_string()))
    }
}

impl TableProvider for DsvReader {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let mut dsv_reader = Self::dsv_reader(reader, self.delimiter, Some(self.escape));

        self.read(tuple_writer, &mut dsv_reader)
    }
}

struct DsvWriter {
    delimiter: u8,
}

impl TableWriter for DsvWriter {
    fn write_record(
        &mut self,
        record: &[AnyDataValue],
        writer: &mut dyn std::io::Write,
    ) -> Result<(), Error> {
        let mut string_record = Vec::with_capacity(record.len());
        for dv in record {
            // FIXME: this may not be the correct format in all cases; needs some output-specific type information
            string_record.push(dv.canonical_string());
        }
        // FIXME: Are we building a new writer for every single record?!
        Ok(WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer)
            .write_record(string_record)?)
    }
}

/// A file format for delimiter-separated values.
#[derive(Debug, Clone, Copy, Default)]
pub struct DsvFormat {
    /// A concrete delimiter for this format.
    delimiter: Option<u8>,
}

impl DsvFormat {
    const DEFAULT_COLUMN_TYPE: PrimitiveType = PrimitiveType::String;

    /// Construct a generic DSV file format, with the delimiter not
    /// yet fixed.
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct a DSV file format with a fixed delimiter.
    pub fn with_delimiter(delimiter: u8) -> Self {
        Self {
            delimiter: Some(delimiter),
        }
    }

    /// Construct a CSV file format.
    pub fn csv() -> Self {
        Self::with_delimiter(b',')
    }

    /// Construct a TSV file format.
    pub fn tsv() -> Self {
        Self::with_delimiter(b'\t')
    }

    fn try_into_import_export(
        mut self,
        direction: Direction,
        resource: Resource,
        predicate: Identifier,
        declared_types: TupleConstraint,
    ) -> Result<ImportExportSpec, FileFormatError> {
        let attributes = Map::singleton(
            Key::identifier_from_str(RESOURCE),
            Constant::StringLiteral(resource),
        );
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
    ) -> Result<ImportSpec, FileFormatError> {
        Ok(ImportSpec::from(self.try_into_import_export(
            Direction::Reading,
            resource,
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
    ) -> Result<ExportSpec, FileFormatError> {
        Ok(ExportSpec::from(self.try_into_import_export(
            Direction::Writing,
            resource,
            predicate,
            declared_types,
        )?))
    }
}

use super::types::attributes::RESOURCE;
const DELIMITER: &str = "delimiter";

/// Errors related to DSV file format specifications.
#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DsvFormatError {
    /// Delimiter should have a string literal as a value, but was
    /// some other term.
    #[error("Delimiter should be a string literal")]
    InvalidDelimiterType(Constant),
    /// Delimiter should consist of exactly one byte, but had a
    /// different length.
    #[error("Delimiter should be exactly one byte")]
    InvalidDelimiterLength(Constant),
    /// Path should have a string literal as a value, but was some
    /// other term.
    #[error("Resource should be a string literal or an IRI")]
    InvalidResourceType(Constant),
}

impl From<DsvFormatError> for FileFormatError {
    fn from(error: DsvFormatError) -> Self {
        match &error {
            DsvFormatError::InvalidDelimiterType(constant)
            | DsvFormatError::InvalidDelimiterLength(constant) => Self::InvalidAttributeValue {
                value: constant.clone(),
                attribute: Key::identifier_from_str(DELIMITER),
                description: error.to_string(),
            },
            DsvFormatError::InvalidResourceType(constant) => Self::InvalidAttributeValue {
                value: constant.clone(),
                attribute: Key::identifier_from_str(RESOURCE),
                description: error.to_string(),
            },
        }
    }
}

impl FileFormatMeta for DsvFormat {
    fn file_format(&self) -> FileFormat {
        match self.delimiter {
            Some(b',') => FileFormat::CSV,
            Some(b'\t') => FileFormat::TSV,
            _ => FileFormat::DSV,
        }
    }

    fn reader(
        &self,
        attributes: &Map,
        declared_types: &TupleConstraint,
        resource_providers: ResourceProviders,
    ) -> Result<Box<dyn TableProvider>, Error> {
        let resource = attributes
            .pairs
            .get(&Key::identifier_from_str(RESOURCE))
            .expect("is a required attribute")
            .as_resource()
            .expect("must be a string or an IRI");

        let delimiter = self.delimiter.unwrap_or_else(|| {
            attributes
                .pairs
                .get(&Key::identifier_from_str(DELIMITER))
                .expect("is a required attribute if the format has no default delimiter")
                .as_string()
                .expect("must be a string")
                .as_bytes()[0]
        });

        Ok(Box::new(DsvReader::new(
            resource_providers,
            resource.to_string(),
            delimiter,
            declared_types.clone(),
        )))
    }

    fn writer(&self, _attributes: &Map) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DsvWriter {
            delimiter: self.delimiter.expect("is a required attribute"),
        }))
    }

    fn resources(&self, attributes: &Map) -> Vec<Resource> {
        vec![attributes
            .pairs
            .get(&Key::identifier_from_str(RESOURCE))
            .expect("is a required attribute")
            .as_resource()
            .expect("must be a string or an IRI")
            .to_string()]
    }

    fn optional_attributes(&self, _direction: Direction) -> HashSet<Key> {
        [].into()
    }

    fn required_attributes(&self, _direction: Direction) -> HashSet<Key> {
        let mut attributes = HashSet::new();

        attributes.insert(RESOURCE);

        if self.delimiter.is_none() {
            attributes.insert(DELIMITER);
        }

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
        if self.delimiter.is_none() {
            let delimiter = attributes
                .pairs
                .get(&Key::identifier_from_str(DELIMITER))
                .expect("is a required attribute");

            if let Some(delim) = delimiter.as_string() {
                if delim.len() != 1 {
                    return Err(DsvFormatError::InvalidDelimiterLength(delimiter.clone()).into());
                }

                self.delimiter = Some(delim.as_bytes()[0]);
            } else {
                return Err(DsvFormatError::InvalidDelimiterType(delimiter.clone()).into());
            }
        }

        let resource = attributes
            .pairs
            .get(&Key::identifier_from_str(RESOURCE))
            .expect("is a required attribute");

        if resource.as_resource().is_none() {
            return Err(DsvFormatError::InvalidResourceType(resource.clone()).into());
        }

        Ok(())
    }

    fn validate_and_refine_type_declaration(
        &mut self,
        declared_types: TupleConstraint,
    ) -> Result<TupleConstraint, FileFormatError> {
        declared_types
            .into_flat_primitive_with_default(Self::DEFAULT_COLUMN_TYPE)
            .ok_or_else(|| FileFormatError::UnsupportedComplexTypes {
                format: self.file_format(),
            })
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    // use quickcheck_macros::quickcheck;
    use test_log::test;

    use super::*;
    use csv::ReaderBuilder;
    use nemo_physical::management::database::Dict;

    #[test]
    fn dsv_reading_basic() {
        let data = r#"city;country;pop;fraction
        Boston;United States;4628910;3.14
        Line;too short;123
        Also-too-short
        This;line is too;123;3.14;long
        This;Line is ok;12345;3.15
        This;line;fails for the integer column;3.14
        This;line;123;fails for the double column
        "#;

        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let reader = DsvReader::new(
            ResourceProviders::empty(),
            "test".to_string(),
            b',',
            TupleConstraint::at_least([
                PrimitiveType::Any,
                PrimitiveType::String,
                PrimitiveType::Integer,
                PrimitiveType::Float64,
            ]),
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 4);
        let result = reader.read(&mut tuple_writer, &mut rdr);
        assert!(result.is_ok());
        assert_eq!(tuple_writer.size(), 2);
    }

    //     #[test]
    //     fn csv_one_line() {
    //         let data = "\
    // city;country;pop
    // Boston;United States;4628910
    // ";

    //         let mut rdr = ReaderBuilder::new()
    //             .delimiter(b';')
    //             .from_reader(data.as_bytes());

    //         let mut dict = std::cell::RefCell::new(Dict::default());
    //         let dsvreader = DSVReader::dsv(
    //             ResourceProviders::empty(),
    //             &DsvFile::csv_file(
    //                 "test",
    //                 [PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any]
    //                     .into_iter()
    //                     .map(TypeConstraint::AtLeast)
    //                     .collect(),
    //             ),
    //             vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any],
    //         );
    //         let mut builder = vec![
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //         ];

    //         let result = dsvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

    //         let x: Vec<VecT> = builder
    //             .into_iter()
    //             .map(|bp| match bp {
    //                 PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
    //             })
    //             .collect();

    //         assert!(result.is_ok());
    //         assert_eq!(x.len(), 3);
    //         assert!(x.iter().all(|vect| vect.len() == 1));

    //         let dvit = DataValueIteratorT::String(Box::new(x.into_iter().map(|vt| {
    //             dict.get_mut()
    //                 .get(usize::try_from(u64::try_from(vt.get(0).unwrap()).unwrap()).unwrap())
    //                 .map(PhysicalString::from)
    //                 .unwrap()
    //         })));

    //         let output_iterator = PrimitiveType::Any.serialize_output(dvit);

    //         for (value, expected) in output_iterator.zip(vec![
    //             "Boston",
    //             "United States",
    //             r#""4628910"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
    //         ]) {
    //             assert_eq!(value, expected);
    //         }
    //     }

    //     #[test]
    //     fn csv_with_various_different_constant_and_literal_representations() {
    //         let data = r#"a;b;c;d
    // Boston;United States;Some String;4628910
    // <Dresden>;Germany;Another String;1234567
    // My Home Town;Some<where >Nice;<https://string.parsing.should/not/change#that>;2
    // Trailing Spaces do not belong to the name   ; What about spaces in the beginning though;  what happens to spaces in string parsing?  ;123
    // """Do String literals work?""";"""Even with datatype annotation?"""^^<http://www.w3.org/2001/XMLSchema#string>;"""even string literals should just be piped through"""^^<http://www.w3.org/2001/XMLSchema#string>;456
    // The next 2 columns are empty;;;789
    // "#;

    //         let expected_result = [
    //             ("Boston", "United States", "Some String", 4628910),
    //             ("Dresden", "Germany", "Another String", 1234567),
    //             (
    //                 "My Home Town",
    //                 r#""Some<where >Nice""#,
    //                 "<https://string.parsing.should/not/change#that>",
    //                 2,
    //             ),
    //             (
    //                 "Trailing Spaces do not belong to the name",
    //                 "What about spaces in the beginning though",
    //                 "  what happens to spaces in string parsing?  ",
    //                 123,
    //             ),
    //             (
    //                 r#""Do String literals work?""#,
    //                 r#""Even with datatype annotation?""#,
    //                 r#""even string literals should just be piped through"^^<http://www.w3.org/2001/XMLSchema#string>"#,
    //                 456,
    //             ),
    //             ("The next 2 columns are empty", r#""""#, "", 789),
    //         ];

    //         let mut rdr = ReaderBuilder::new()
    //             .delimiter(b';')
    //             .from_reader(data.as_bytes());

    //         let mut dict = std::cell::RefCell::new(Dict::default());
    //         let csvreader = DSVReader::dsv(
    //             ResourceProviders::empty(),
    //             &DsvFile::csv_file(
    //                 "test",
    //                 [
    //                     PrimitiveType::Any,
    //                     PrimitiveType::Any,
    //                     PrimitiveType::String,
    //                     PrimitiveType::Integer,
    //                 ]
    //                 .into_iter()
    //                 .map(TypeConstraint::AtLeast)
    //                 .collect(),
    //             ),
    //             vec![
    //                 PrimitiveType::Any,
    //                 PrimitiveType::Any,
    //                 PrimitiveType::String,
    //                 PrimitiveType::Integer,
    //             ],
    //         );
    //         let mut builder = vec![
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::I64(Default::default()),
    //         ];
    //         let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);
    //         assert!(result.is_ok());

    //         let cols: Vec<VecT> = builder.into_iter().map(|bp| bp.finalize()).collect();

    //         let VecT::Id64(ref col0_idx) = cols[0] else {
    //             unreachable!()
    //         };
    //         let VecT::Id64(ref col1_idx) = cols[1] else {
    //             unreachable!()
    //         };
    //         let VecT::Id64(ref col2_idx) = cols[2] else {
    //             unreachable!()
    //         };
    //         let VecT::Int64(ref col3) = cols[3] else {
    //             unreachable!()
    //         };

    //         let col0 = DataValueIteratorT::String(Box::new(
    //             col0_idx
    //                 .iter()
    //                 .copied()
    //                 .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
    //                 .map(PhysicalString::from)
    //                 .collect::<Vec<_>>()
    //                 .into_iter(),
    //         ));
    //         let col1 = DataValueIteratorT::String(Box::new(
    //             col1_idx
    //                 .iter()
    //                 .copied()
    //                 .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
    //                 .map(PhysicalString::from)
    //                 .collect::<Vec<_>>()
    //                 .into_iter(),
    //         ));
    //         let col2 = DataValueIteratorT::String(Box::new(
    //             col2_idx
    //                 .iter()
    //                 .copied()
    //                 .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
    //                 .map(PhysicalString::from)
    //                 .collect::<Vec<_>>()
    //                 .into_iter(),
    //         ));
    //         let col3 = DataValueIteratorT::I64(Box::new(col3.iter().copied()));

    //         PrimitiveType::Any
    //             .serialize_output(col0)
    //             .zip(PrimitiveType::Any.serialize_output(col1))
    //             .zip(PrimitiveType::String.serialize_output(col2))
    //             .zip(PrimitiveType::Integer.serialize_output(col3))
    //             .map(|(((c0, c1), c2), c3)| (c0, c1, c2, c3))
    //             .zip(expected_result.into_iter().map(|(e0, e1, e2, e3)| {
    //                 (
    //                     e0.to_string(),
    //                     e1.to_string(),
    //                     e2.to_string(),
    //                     e3.to_string(),
    //                 )
    //             }))
    //             .for_each(|(t1, t2)| assert_eq!(t1, t2));
    //     }

    //     #[test]
    //     #[cfg_attr(miri, ignore)]
    //     fn csv_with_ignored_and_faulty() {
    //         let data = "\
    // 10;20;30;40;20;valid
    // asdf;12.2;413;22.3;23;invalid
    // node01;22;33.33;12.333332;10;valid
    // node02;1312;12.33;313;1431;valid
    // node03;123;123;13;55;123;invalid
    // ";
    //         let mut rdr = ReaderBuilder::new()
    //             .delimiter(b';')
    //             .has_headers(false)
    //             .from_reader(data.as_bytes());

    //         let dict = std::cell::RefCell::new(Dict::default());
    //         let csvreader: DSVReader = DSVReader::dsv(
    //             ResourceProviders::empty(),
    //             &DsvFile::csv_file(
    //                 "test",
    //                 [
    //                     PrimitiveType::Any,
    //                     PrimitiveType::Integer,
    //                     PrimitiveType::Float64,
    //                     PrimitiveType::Float64,
    //                     PrimitiveType::Integer,
    //                     PrimitiveType::Any,
    //                 ]
    //                 .into_iter()
    //                 .map(TypeConstraint::AtLeast)
    //                 .collect(),
    //             ),
    //             vec![
    //                 PrimitiveType::Any,
    //                 PrimitiveType::Integer,
    //                 PrimitiveType::Float64,
    //                 PrimitiveType::Float64,
    //                 PrimitiveType::Integer,
    //                 PrimitiveType::Any,
    //             ],
    //         );
    //         let mut builder = vec![
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //             PhysicalBuilderProxyEnum::I64(Default::default()),
    //             PhysicalBuilderProxyEnum::Double(Default::default()),
    //             PhysicalBuilderProxyEnum::Double(Default::default()),
    //             PhysicalBuilderProxyEnum::I64(Default::default()),
    //             PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
    //         ];
    //         let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

    //         let imported: Vec<VecT> = builder
    //             .into_iter()
    //             .map(|bp| match bp {
    //                 PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
    //             })
    //             .collect();

    //         eprintln!("{imported:?}");

    //         assert!(result.is_ok());
    //         assert_eq!(imported.len(), 6);
    //         assert_eq!(imported[1].len(), 3);
    //     }

    //     #[quickcheck]
    //     #[cfg_attr(miri, ignore)]
    //     fn csv_quickchecked(mut i64_vec: Vec<i64>, double_vec: Vec<f64>, float_vec: Vec<f32>) -> bool {
    //         let mut double_vec = double_vec
    //             .iter()
    //             .filter(|val| !val.is_nan())
    //             .copied()
    //             .collect::<Vec<_>>();
    //         let mut float_vec = float_vec
    //             .iter()
    //             .filter(|val| !val.is_nan())
    //             .copied()
    //             .collect::<Vec<_>>();
    //         let len = double_vec.len().min(float_vec.len().min(i64_vec.len()));
    //         double_vec.truncate(len);
    //         float_vec.truncate(len);
    //         i64_vec.truncate(len);
    //         let mut csv = String::new();
    //         for i in 0..len {
    //             csv = format!(
    //                 "{}\n{},{},{},{}",
    //                 csv, i, double_vec[i], i64_vec[i], float_vec[i]
    //             );
    //         }

    //         let mut rdr = ReaderBuilder::new()
    //             .delimiter(b',')
    //             .has_headers(false)
    //             .from_reader(csv.as_bytes());
    //         let csvreader = DSVReader::dsv(
    //             ResourceProviders::empty(),
    //             &DsvFile::csv_file(
    //                 "test",
    //                 [
    //                     PrimitiveType::Integer,
    //                     PrimitiveType::Float64,
    //                     PrimitiveType::Integer,
    //                     PrimitiveType::Float64,
    //                 ]
    //                 .into_iter()
    //                 .map(TypeConstraint::AtLeast)
    //                 .collect(),
    //             ),
    //             vec![
    //                 PrimitiveType::Integer,
    //                 PrimitiveType::Float64,
    //                 PrimitiveType::Integer,
    //                 PrimitiveType::Float64,
    //             ],
    //         );
    //         let mut builder = vec![
    //             PhysicalBuilderProxyEnum::I64(Default::default()),
    //             PhysicalBuilderProxyEnum::Double(Default::default()),
    //             PhysicalBuilderProxyEnum::I64(Default::default()),
    //             PhysicalBuilderProxyEnum::Double(Default::default()),
    //         ];

    //         let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

    //         let imported: Vec<VecT> = builder
    //             .into_iter()
    //             .map(|bp| match bp {
    //                 PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
    //                 PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
    //             })
    //             .collect();

    //         assert!(result.is_ok());
    //         assert_eq!(imported.len(), 4);
    //         assert_eq!(imported[0].len(), len);
    //         true
    //     }
}
