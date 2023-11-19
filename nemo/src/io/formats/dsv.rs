//! Reading of delimiter-separated value files

use std::collections::HashSet;
use std::io::Read;

use csv::{Reader, ReaderBuilder, WriterBuilder};
use thiserror::Error;

use nemo_physical::{
    builder_proxy::{ColumnBuilderProxy, PhysicalBuilderProxyEnum},
    table_reader::{Resource, TableReader},
};

use crate::{
    builder_proxy::LogicalColumnBuilderProxyT,
    error::{Error, ReadingError},
    io::{
        formats::{
            types::{
                Direction, ExportSpec, FileFormat, FileFormatError, FileFormatMeta,
                ImportExportSpec, ImportSpec, TableWriter,
            },
            PROGRESS_NOTIFY_INCREMENT,
        },
        resource_providers::ResourceProviders,
    },
    model::{
        types::primitive_logical_value::{LogicalFloat64, LogicalInteger, LogicalString},
        Constant, Identifier, Key, Map, PrimitiveType, TupleConstraint, TypeConstraint,
    },
};

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// The reader object relates a given [resource][Resource] in DSV format to a tuple of [logical types][PrimitiveType].
/// It accesses the resource through the given [resource_providers][ResourceProviders].
/// Via the implementation of [`TableReader`] it fills the corresponding [`PhysicalBuilderProxys`][nemo_physical::builder_proxy::PhysicalBuilderProxyEnum]
/// with the data from the file.
/// It combines the logical and physical BuilderProxies to handle the read data according to both datatype models.
#[derive(Debug)]
pub(crate) struct DSVReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    delimiter: u8,
    escape: u8,
    logical_types: Vec<PrimitiveType>,
    input_type_constraint: TupleConstraint,
}

impl DSVReader {
    /// Instantiate a [DSVReader] for a given delimiter
    pub fn new(
        resource_providers: ResourceProviders,
        resource: Resource,
        delimiter: u8,
        input_type_constraint: TupleConstraint,
        logical_types: Vec<PrimitiveType>,
    ) -> Self {
        Self {
            resource_providers,
            resource,
            delimiter,
            escape: b'\\',
            logical_types,
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

    /// Actually reads the data from the file and distributes the different fields into the corresponding [ColumnBuilderProxy]
    /// If a field cannot be read or parsed, the line will be ignored
    fn read_with_reader<'a, R2>(
        &self,
        mut builder: Vec<Box<dyn ColumnBuilderProxy<String> + 'a>>,
        dsv_reader: &mut Reader<R2>,
    ) -> Result<(), ReadingError>
    where
        R2: Read,
    {
        let mut lines = 0;

        for row in dsv_reader.records().flatten() {
            if let Err(Error::Rollback(rollback)) =
                row.iter().enumerate().try_for_each(|(idx, item)| {
                    if idx < builder.len() {
                        if let Err(column_err) = builder[idx].add(item.to_string()) {
                            log::info!("Ignoring line {row:?}, parsing failed with: {column_err}");
                            Err(Error::Rollback(idx))
                        } else {
                            Ok(())
                        }
                    } else {
                        Err(Error::Rollback(idx - 1))
                    }
                })
            {
                builder.iter_mut().enumerate().for_each(|(idx, builder)| {
                    // Forget the stored values of the row to be rolled back.
                    if idx <= rollback {
                        builder.forget();
                    }
                });
            }

            lines += 1;
            if (lines % PROGRESS_NOTIFY_INCREMENT) == 0 {
                log::info!("loading: processed {lines} lines");
            }
        }
        log::info!("Finished loading: processed {lines} lines");

        Ok(())
    }

    fn read_into_builder_proxies_with_reader<'a: 'b, 'b, R: Read>(
        &self,
        physical_builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
        dsv_reader: &mut Reader<R>,
    ) -> Result<(), ReadingError> {
        macro_rules! into_parser {
            ($it:ident, $lcbp:ident) => {{
                let boxed: Box<dyn ColumnBuilderProxy<String>> = match $it {
                    TypeConstraint::Exact(PrimitiveType::Any) | TypeConstraint::AtLeast(PrimitiveType::Any) => Box::new($lcbp.into_parser::<Constant>()),
                    TypeConstraint::Exact(PrimitiveType::String) | TypeConstraint::AtLeast(PrimitiveType::String) => Box::new($lcbp.into_parser::<LogicalString>()),
                    TypeConstraint::Exact(PrimitiveType::Integer) | TypeConstraint::AtLeast(PrimitiveType::Integer) => Box::new($lcbp.into_parser::<LogicalInteger>()),
                    TypeConstraint::Exact(PrimitiveType::Float64) | TypeConstraint::AtLeast(PrimitiveType::Float64) => Box::new($lcbp.into_parser::<LogicalFloat64>()),
                    TypeConstraint::None => unreachable!("Type constraints for input types are always initialized (with fallbacks)."),
                    TypeConstraint::Tuple(_) => todo!("We do not support tuples in CSV currently. Should we?"),
                };
                boxed
            }};
        }

        let logical_builder_proxies: Vec<Box<dyn ColumnBuilderProxy<String> + 'b>> = self
            .logical_types
            .iter()
            .cloned()
            .zip(self.input_type_constraint.iter().cloned())
            .zip(physical_builder_proxies)
            .map(|((ty, it), bp)| match ty.wrap_physical_column_builder(bp) {
                LogicalColumnBuilderProxyT::Any(lcbp) => into_parser!(it, lcbp),
                LogicalColumnBuilderProxyT::String(lcbp) => into_parser!(it, lcbp),
                LogicalColumnBuilderProxyT::Integer(lcbp) => into_parser!(it, lcbp),
                LogicalColumnBuilderProxyT::Float64(lcbp) => into_parser!(it, lcbp),
            })
            .collect();

        self.read_with_reader(logical_builder_proxies, dsv_reader)
    }
}

impl TableReader for DSVReader {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        self: Box<Self>,
        physical_builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let mut dsv_reader = Self::dsv_reader(reader, self.delimiter, Some(self.escape));

        self.read_into_builder_proxies_with_reader(physical_builder_proxies, &mut dsv_reader)
    }
}

struct DSVWriter {
    delimiter: u8,
}

impl TableWriter for DSVWriter {
    fn write_record(
        &mut self,
        record: &[String],
        writer: &mut dyn std::io::Write,
    ) -> Result<(), Error> {
        Ok(WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer)
            .write_record(record)?)
    }
}

/// A file format for delimiter-separated values.
#[derive(Debug, Clone, Copy, Default)]
pub struct DSVFormat {
    /// A concrete delimiter for this format.
    delimiter: Option<u8>,
}

impl DSVFormat {
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
pub enum DSVFormatError {
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

impl From<DSVFormatError> for FileFormatError {
    fn from(error: DSVFormatError) -> Self {
        match &error {
            DSVFormatError::InvalidDelimiterType(constant)
            | DSVFormatError::InvalidDelimiterLength(constant) => Self::InvalidAttributeValue {
                value: constant.clone(),
                attribute: Key::identifier_from_str(DELIMITER),
                description: error.to_string(),
            },
            DSVFormatError::InvalidResourceType(constant) => Self::InvalidAttributeValue {
                value: constant.clone(),
                attribute: Key::identifier_from_str(RESOURCE),
                description: error.to_string(),
            },
        }
    }
}

impl FileFormatMeta for DSVFormat {
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
        inferred_types: &TupleConstraint,
    ) -> Result<Box<dyn TableReader>, Error> {
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

        let inferred_types = inferred_types
            .clone()
            .into_flat_primitive()
            .expect("must be flat and primitive");

        Ok(Box::new(DSVReader::new(
            resource_providers,
            resource.to_string(),
            delimiter,
            declared_types.clone(),
            inferred_types,
        )))
    }

    fn writer(&self, _attributes: &Map) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DSVWriter {
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
                    return Err(DSVFormatError::InvalidDelimiterLength(delimiter.clone()).into());
                }

                self.delimiter = Some(delim.as_bytes()[0]);
            } else {
                return Err(DSVFormatError::InvalidDelimiterType(delimiter.clone()).into());
            }
        }

        let resource = attributes
            .pairs
            .get(&Key::identifier_from_str(RESOURCE))
            .expect("is a required attribute");

        if resource.as_resource().is_none() {
            return Err(DSVFormatError::InvalidResourceType(resource.clone()).into());
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
    use quickcheck_macros::quickcheck;
    use test_log::test;

    use super::*;
    use csv::ReaderBuilder;
    use nemo_physical::{
        builder_proxy::{PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy},
        datatypes::{
            data_value::{DataValueIteratorT, PhysicalString},
            storage_value::VecT,
        },
        dictionary::Dictionary,
        management::database::Dict,
    };

    #[test]
    fn csv_one_line() {
        let data = "\
city;country;pop
Boston;United States;4628910
";

        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let mut dict = std::cell::RefCell::new(Dict::default());
        let csvreader = DSVReader::new(
            ResourceProviders::empty(),
            "test".to_string(),
            b',',
            TupleConstraint::at_least([PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any]),
            vec![PrimitiveType::Any, PrimitiveType::Any, PrimitiveType::Any],
        );
        let mut builder = vec![
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
        ];

        let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

        let x: Vec<VecT> = builder
            .into_iter()
            .map(|bp| match bp {
                PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
            })
            .collect();

        assert!(result.is_ok());
        assert_eq!(x.len(), 3);
        assert!(x.iter().all(|vect| vect.len() == 1));

        let dvit = DataValueIteratorT::String(Box::new(x.into_iter().map(|vt| {
            dict.get_mut()
                .get(usize::try_from(u64::try_from(vt.get(0).unwrap()).unwrap()).unwrap())
                .map(PhysicalString::from)
                .unwrap()
        })));

        let output_iterator = PrimitiveType::Any.serialize_output(dvit);

        for (value, expected) in output_iterator.zip(vec![
            "Boston",
            "United States",
            r#""4628910"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
        ]) {
            assert_eq!(value, expected);
        }
    }

    #[test]
    fn csv_with_various_different_constant_and_literal_representations() {
        let data = r#"a;b;c;d
Boston;United States;Some String;4628910
<Dresden>;Germany;Another String;1234567
My Home Town;Some<where >Nice;<https://string.parsing.should/not/change#that>;2
Trailing Spaces do not belong to the name   ; What about spaces in the beginning though;  what happens to spaces in string parsing?  ;123
"""Do String literals work?""";"""Even with datatype annotation?"""^^<http://www.w3.org/2001/XMLSchema#string>;"""even string literals should just be piped through"""^^<http://www.w3.org/2001/XMLSchema#string>;456
The next 2 columns are empty;;;789
"#;

        let expected_result = [
            ("Boston", "United States", "Some String", 4628910),
            ("Dresden", "Germany", "Another String", 1234567),
            (
                "My Home Town",
                r#""Some<where >Nice""#,
                "<https://string.parsing.should/not/change#that>",
                2,
            ),
            (
                "Trailing Spaces do not belong to the name",
                "What about spaces in the beginning though",
                "  what happens to spaces in string parsing?  ",
                123,
            ),
            (
                r#""Do String literals work?""#,
                r#""Even with datatype annotation?""#,
                r#""even string literals should just be piped through"^^<http://www.w3.org/2001/XMLSchema#string>"#,
                456,
            ),
            ("The next 2 columns are empty", r#""""#, "", 789),
        ];

        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let mut dict = std::cell::RefCell::new(Dict::default());
        let csvreader = DSVReader::new(
            ResourceProviders::empty(),
            "test".to_string(),
            b',',
            TupleConstraint::at_least([
                PrimitiveType::Any,
                PrimitiveType::Any,
                PrimitiveType::String,
                PrimitiveType::Integer,
            ]),
            vec![
                PrimitiveType::Any,
                PrimitiveType::Any,
                PrimitiveType::String,
                PrimitiveType::Integer,
            ],
        );
        let mut builder = vec![
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::I64(Default::default()),
        ];
        let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);
        assert!(result.is_ok());

        let cols: Vec<VecT> = builder.into_iter().map(|bp| bp.finalize()).collect();

        let VecT::U64(ref col0_idx) = cols[0] else {
            unreachable!()
        };
        let VecT::U64(ref col1_idx) = cols[1] else {
            unreachable!()
        };
        let VecT::U64(ref col2_idx) = cols[2] else {
            unreachable!()
        };
        let VecT::I64(ref col3) = cols[3] else {
            unreachable!()
        };

        let col0 = DataValueIteratorT::String(Box::new(
            col0_idx
                .iter()
                .copied()
                .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
                .map(PhysicalString::from)
                .collect::<Vec<_>>()
                .into_iter(),
        ));
        let col1 = DataValueIteratorT::String(Box::new(
            col1_idx
                .iter()
                .copied()
                .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
                .map(PhysicalString::from)
                .collect::<Vec<_>>()
                .into_iter(),
        ));
        let col2 = DataValueIteratorT::String(Box::new(
            col2_idx
                .iter()
                .copied()
                .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
                .map(PhysicalString::from)
                .collect::<Vec<_>>()
                .into_iter(),
        ));
        let col3 = DataValueIteratorT::I64(Box::new(col3.iter().copied()));

        PrimitiveType::Any
            .serialize_output(col0)
            .zip(PrimitiveType::Any.serialize_output(col1))
            .zip(PrimitiveType::String.serialize_output(col2))
            .zip(PrimitiveType::Integer.serialize_output(col3))
            .map(|(((c0, c1), c2), c3)| (c0, c1, c2, c3))
            .zip(expected_result.into_iter().map(|(e0, e1, e2, e3)| {
                (
                    e0.to_string(),
                    e1.to_string(),
                    e2.to_string(),
                    e3.to_string(),
                )
            }))
            .for_each(|(t1, t2)| assert_eq!(t1, t2));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn csv_with_ignored_and_faulty() {
        let data = "\
10;20;30;40;20;valid
asdf;12.2;413;22.3;23;invalid
node01;22;33.33;12.333332;10;valid
node02;1312;12.33;313;1431;valid
node03;123;123;13;55;123;invalid
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(data.as_bytes());

        let dict = std::cell::RefCell::new(Dict::default());
        let csvreader: DSVReader = DSVReader::new(
            ResourceProviders::empty(),
            "test".to_string(),
            b',',
            TupleConstraint::at_least([
                PrimitiveType::Any,
                PrimitiveType::Integer,
                PrimitiveType::Float64,
                PrimitiveType::Float64,
                PrimitiveType::Integer,
                PrimitiveType::Any,
            ]),
            vec![
                PrimitiveType::Any,
                PrimitiveType::Integer,
                PrimitiveType::Float64,
                PrimitiveType::Float64,
                PrimitiveType::Integer,
                PrimitiveType::Any,
            ],
        );
        let mut builder = vec![
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
            PhysicalBuilderProxyEnum::I64(Default::default()),
            PhysicalBuilderProxyEnum::Double(Default::default()),
            PhysicalBuilderProxyEnum::Double(Default::default()),
            PhysicalBuilderProxyEnum::I64(Default::default()),
            PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
        ];
        let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

        let imported: Vec<VecT> = builder
            .into_iter()
            .map(|bp| match bp {
                PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
            })
            .collect();

        eprintln!("{imported:?}");

        assert!(result.is_ok());
        assert_eq!(imported.len(), 6);
        assert_eq!(imported[1].len(), 3);
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn csv_quickchecked(mut i64_vec: Vec<i64>, double_vec: Vec<f64>, float_vec: Vec<f32>) -> bool {
        let mut double_vec = double_vec
            .iter()
            .filter(|val| !val.is_nan())
            .copied()
            .collect::<Vec<_>>();
        let mut float_vec = float_vec
            .iter()
            .filter(|val| !val.is_nan())
            .copied()
            .collect::<Vec<_>>();
        let len = double_vec.len().min(float_vec.len().min(i64_vec.len()));
        double_vec.truncate(len);
        float_vec.truncate(len);
        i64_vec.truncate(len);
        let mut csv = String::new();
        for i in 0..len {
            csv = format!(
                "{}\n{},{},{},{}",
                csv, i, double_vec[i], i64_vec[i], float_vec[i]
            );
        }

        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(false)
            .from_reader(csv.as_bytes());
        let csvreader = DSVReader::new(
            ResourceProviders::empty(),
            "test".to_string(),
            b',',
            TupleConstraint::at_least([
                PrimitiveType::Integer,
                PrimitiveType::Float64,
                PrimitiveType::Integer,
                PrimitiveType::Float64,
            ]),
            vec![
                PrimitiveType::Integer,
                PrimitiveType::Float64,
                PrimitiveType::Integer,
                PrimitiveType::Float64,
            ],
        );
        let mut builder = vec![
            PhysicalBuilderProxyEnum::I64(Default::default()),
            PhysicalBuilderProxyEnum::Double(Default::default()),
            PhysicalBuilderProxyEnum::I64(Default::default()),
            PhysicalBuilderProxyEnum::Double(Default::default()),
        ];

        let result = csvreader.read_into_builder_proxies_with_reader(&mut builder, &mut rdr);

        let imported: Vec<VecT> = builder
            .into_iter()
            .map(|bp| match bp {
                PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
                PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
            })
            .collect();

        assert!(result.is_ok());
        assert_eq!(imported.len(), 4);
        assert_eq!(imported[0].len(), len);
        true
    }
}
