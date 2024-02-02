//! Reading of delimiter-separated value files

use std::io::{BufRead, Write};
use std::mem::size_of;

use bytesize::ByteSize;
use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use nemo_physical::datavalues::DataValue;
use nemo_physical::management::bytesized::ByteSized;

use oxiri::Iri;

use nemo_physical::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datavalues::{AnyDataValue, DataValueCreationError},
    resource::Resource,
};

use crate::model::{
    PARAMETER_NAME_ARITY, PARAMETER_NAME_DSV_DELIMITER, PARAMETER_NAME_FORMAT,
    PARAMETER_NAME_RESOURCE, VALUE_FORMAT_ANY, VALUE_FORMAT_DOUBLE, VALUE_FORMAT_INT,
    VALUE_FORMAT_STRING,
};
use crate::{
    error::Error,
    io::{
        formats::{
            types::{Direction, TableWriter},
            PROGRESS_NOTIFY_INCREMENT,
        },
        parser::{parse_bare_name, span_from_str},
    },
    model::{Constant, FileFormat, Map},
};

use super::import_export::{ImportExportError, ImportExportHandler, ImportExportHandlers};

type DataValueParserFunction = fn(String) -> Result<AnyDataValue, DataValueCreationError>;

/// Enum for the various formats that are supported for encoding values
/// in DSV. Since DSV has no own type system, the encoding of data must be
/// controlled through this external mechanism.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DsvValueFormat {
    /// Format that tries various heuristics to interpret and represent values
    /// in the most natural way. The format can interpret any content (the final
    /// fallback is to use it as a string).
    ANYTHING,
    /// Format that interprets the DSV values as literal string values.
    /// All data will be interpreted in this way.
    STRING,
    /// Format that interprets numeric DSV values as integers, and rejects
    /// all values that are not in this form.
    INTEGER,
    /// Format that interprets numeric DSV values as double-precision floating
    /// point numbers, and rejects all values that are not in this form.
    DOUBLE,
}
impl DsvValueFormat {
    /// Try to convert a string name for a value format to one of the supported
    /// DSV value formats, or return an error for unsupported formats.
    pub(crate) fn from_string(name: &str) -> Result<Self, ImportExportError> {
        match name {
            VALUE_FORMAT_ANY => Ok(DsvValueFormat::ANYTHING),
            VALUE_FORMAT_STRING => Ok(DsvValueFormat::STRING),
            VALUE_FORMAT_INT => Ok(DsvValueFormat::INTEGER),
            VALUE_FORMAT_DOUBLE => Ok(DsvValueFormat::DOUBLE),
            _ => Err(ImportExportError::InvalidValueFormat {
                value_format: name.to_string(),
                format: FileFormat::DSV,
            }),
        }
    }

    /// Return a function for parsing value strings for this format.
    fn data_value_parser_function(&self) -> DataValueParserFunction {
        match self {
            DsvValueFormat::ANYTHING => Self::parse_any_value_from_string,
            DsvValueFormat::STRING => Self::parse_string_from_string,
            DsvValueFormat::INTEGER => AnyDataValue::new_from_integer_literal,
            DsvValueFormat::DOUBLE => AnyDataValue::new_from_double_literal,
        }
    }

    /// Simple wrapper function that makes CSV strings into [`AnyDataValue`]. We wrap this
    /// to match the error-producing signature of other parsing functions.
    fn parse_string_from_string(input: String) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue::new_string(input))
    }

    /// Best-effort parsing function for strings from CSV. True to the nature of CSV, this function
    /// will try hard to find a usable value in the string.
    /// TODO: This function could possibly share some methods with the parser code later on.
    /// TODO: Currently no support for guessing floating point values, ony decimal.
    fn parse_any_value_from_string(input: String) -> Result<AnyDataValue, DataValueCreationError> {
        let trimmed = input.trim();

        // Represent empty cells as empty strings
        if trimmed.is_empty() {
            return Ok(AnyDataValue::new_string("".to_string()));
        }
        assert!(trimmed.len() > 0);

        match trimmed.as_bytes()[0] {
            b'<' => {
                if trimmed.as_bytes()[trimmed.len() - 1] == b'>' {
                    return Ok(AnyDataValue::new_iri(
                        trimmed[1..trimmed.len() - 1].to_string(),
                    ));
                }
            }
            b'0'..=b'9' | b'+' | b'-' => {
                if let Ok(dv) = AnyDataValue::new_from_decimal_literal(trimmed.to_string()) {
                    return Ok(dv);
                }
            }
            b'"' => {
                if let Some(pos) = trimmed.rfind("\"") {
                    if pos == trimmed.len() - 1 {
                        return Ok(AnyDataValue::new_string(
                            trimmed[1..trimmed.len() - 1].to_string(),
                        ));
                    } else if trimmed.as_bytes()[pos + 1] == b'@' {
                        return Ok(AnyDataValue::new_language_tagged_string(
                            trimmed[1..pos].to_string(),
                            trimmed[pos + 2..trimmed.len()].to_string(),
                        ));
                    } else if trimmed.as_bytes()[trimmed.len() - 1] == b'>'
                        && trimmed.len() > pos + 4
                        && &trimmed[pos..pos + 4] == "\"^^<"
                    {
                        if let Ok(dv) = AnyDataValue::new_from_typed_literal(
                            trimmed[1..pos].to_string(),
                            trimmed[pos + 4..trimmed.len() - 1].to_string(),
                        ) {
                            return Ok(dv);
                        }
                    }
                }
            }
            _ => {}
        }

        // Check if it's a valid bare name
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

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// Parsing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// which can be set for each column separately.
pub(crate) struct DsvReader {
    read: Box<dyn BufRead>,
    delimiter: u8,
    escape: u8,
    value_formats: Vec<DsvValueFormat>,
}

impl DsvReader {
    /// Instantiate a [`DsvReader`] for a given delimiter
    pub fn new(read: Box<dyn BufRead>, delimiter: u8, value_formats: Vec<DsvValueFormat>) -> Self {
        Self {
            read,
            delimiter,
            escape: b'\\',
            value_formats: value_formats,
        }
    }

    /// Create a low-level reader for parsing the DSV format.
    fn reader(self) -> Reader<Box<dyn BufRead>> {
        ReaderBuilder::new()
            .delimiter(self.delimiter)
            .escape(Some(self.escape))
            .has_headers(false)
            .double_quote(true)
            .from_reader(self.read)
    }

    /// Make a list of parser functions to be used for ingesting the data in each column.
    fn make_parsers(&self) -> Vec<DataValueParserFunction> {
        let mut result = Vec::with_capacity(self.value_formats.len());
        for ty in self.value_formats.iter() {
            result.push(ty.data_value_parser_function());
        }
        result
    }

    /// Actually reads the data from the file, using the given parsers to convert strings to [`AnyDataValue`]s.
    /// If a field cannot be read or parsed, the line will be ignored
    fn read(self, tuple_writer: &mut TupleWriter) -> Result<(), Box<dyn std::error::Error>> {
        let parsers = self.make_parsers();
        let mut dsv_reader = self.reader();

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
}

impl TableProvider for DsvReader {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.read(tuple_writer)
    }
}

impl std::fmt::Debug for DsvReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DsvReader")
            .field("read", &"<unspecified std::io::Read>")
            .field("delimiter", &self.delimiter)
            .field("escape", &self.escape)
            .finish()
    }
}

impl ByteSized for DsvReader {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64)
    }
}

struct DsvWriter {
    writer: Writer<Box<dyn Write>>,
}

impl DsvWriter {
    fn new(delimiter: u8, writer: Box<dyn Write>) -> Self {
        DsvWriter {
            writer: WriterBuilder::new()
                .delimiter(delimiter)
                .from_writer(writer),
        }
    }
}

impl TableWriter for DsvWriter {
    fn export_table_data<'a>(
        &mut self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        for record in table {
            let mut string_record = Vec::with_capacity(record.len());
            for dv in record {
                // FIXME: this may not be the correct format in all cases; needs some output-specific type information
                string_record.push(dv.canonical_string());
            }

            self.writer.write_record(string_record)?;
        }

        Ok(())
    }
}

/// Internal enum to distnguish variants of the DSV format.
enum DsvVariant {
    /// Delimiter-separated values
    DSV,
    /// Comma-separated values
    CSV,
    /// Tab-separated values
    TSV,
}

/// An [ImportExportHandler] for delimiter-separated values.
#[derive(Debug, Clone)]
pub(crate) struct DsvHandler {
    /// The specific delimiter for this format.
    delimiter: u8,
    /// The resource to write to/read from.
    /// This can be `None` for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so the predicate is setting there.
    resource: Option<Resource>,
    /// The list of value formats to be used for exporting this data.
    /// If only the arity is given, this will use the most general export format
    /// for each value (and the list will still be set). The list can be `None`
    /// if neither formats nor arity were given for writing: in this case, a default
    /// arity-based formats can be used if the arity is clear from another source.
    value_formats: Option<Vec<DsvValueFormat>>,
}

impl DsvHandler {
    /// Construct a DSV file handler with an arbitrary delimiter.
    pub(crate) fn try_new_dsv(
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::DSV, attributes, direction)
    }

    /// Construct a CSV file handler.
    pub(crate) fn try_new_csv(
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::CSV, attributes, direction)
    }

    /// Construct a TSV file handler.
    pub(crate) fn try_new_tsv(
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::TSV, attributes, direction)
    }

    /// Construct a DSV handler of the given variant.
    fn try_new(
        variant: DsvVariant,
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // Basic checks for unsupported attributes:
        ImportExportHandlers::check_attributes(
            attributes,
            &vec![
                PARAMETER_NAME_FORMAT,
                PARAMETER_NAME_RESOURCE,
                PARAMETER_NAME_ARITY,
                PARAMETER_NAME_DSV_DELIMITER,
            ],
        )?;

        let delimiter = Self::extract_delimiter(variant, attributes)?;
        let resource = ImportExportHandlers::extract_resource(attributes, direction)?;
        let value_formats = Self::extract_value_formats(attributes, direction)?;

        Ok(Box::new(Self {
            delimiter: delimiter,
            resource: resource,
            value_formats: value_formats,
        }))
    }

    fn default_value_format_strings(arity: usize) -> Vec<String> {
        vec![VALUE_FORMAT_ANY; arity]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn extract_value_formats(
        attributes: &Map,
        direction: Direction,
    ) -> Result<Option<Vec<DsvValueFormat>>, ImportExportError> {
        let arity = ImportExportHandlers::extract_integer(attributes, PARAMETER_NAME_ARITY, true)?;
        let mut value_format_strings: Option<Vec<String>> =
            ImportExportHandlers::extract_value_format_strings(attributes)?;

        if let Some(a) = arity {
            if a <= 0 || a > 65536 {
                // ridiculously large value, but still in usize for all conceivable platforms
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_ARITY,
                    Constant::NumericLiteral(crate::model::NumericLiteral::Integer(a)),
                    format!("arity should be greater than 0 and at most {}", 65536).as_str(),
                ));
            }

            let us_a = usize::try_from(a).expect("range was checked above");
            if let Some(ref v) = value_format_strings {
                // check if arity is consistent with given value formats
                if us_a != v.len() {
                    return Err(ImportExportError::invalid_att_value_error(
                        PARAMETER_NAME_ARITY,
                        Constant::NumericLiteral(crate::model::NumericLiteral::Integer(a)),
                        format!(
                            "arity should be {}, the number of value types given for \"{}\"",
                            v.len(),
                            PARAMETER_NAME_FORMAT
                        )
                        .as_str(),
                    ));
                }
            } else {
                // default value formats from arity:
                value_format_strings = Some(Self::default_value_format_strings(
                    usize::try_from(a).expect("range was checked above"),
                ));
            }
        }

        if let Some(format_strings) = value_format_strings {
            Ok(Some(DsvHandler::formats_from_strings(format_strings)?))
        // TODO: remove or re-instantiate ...
        // } else if direction == Direction::Import {
        //     Err(ImportExportError::MissingAttribute(
        //         PARAMETER_NAME_FORMAT.to_string(),
        //     ))
        } else {
            Ok(None)
        }
    }

    fn formats_from_strings(
        value_format_strings: Vec<String>,
    ) -> Result<Vec<DsvValueFormat>, ImportExportError> {
        let mut value_formats = Vec::with_capacity(value_format_strings.len());
        for s in value_format_strings {
            value_formats.push(DsvValueFormat::from_string(s.as_str())?);
        }
        Ok(value_formats)
    }

    fn extract_delimiter(variant: DsvVariant, attributes: &Map) -> Result<u8, ImportExportError> {
        let delim_opt: Option<u8>;
        if let Some(string) =
            ImportExportHandlers::extract_string(attributes, PARAMETER_NAME_DSV_DELIMITER, true)?
        {
            if string.len() == 1 {
                delim_opt = Some(string.as_bytes()[0]);
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_DSV_DELIMITER,
                    Constant::StringLiteral(string.to_owned()),
                    "delimiter should be exactly one byte",
                ));
            }
        } else {
            delim_opt = None;
        }

        let delimiter: u8;
        match (variant, delim_opt) {
            (DsvVariant::DSV, Some(delim)) => {
                delimiter = delim;
            }
            (DsvVariant::DSV, None) => {
                return Err(ImportExportError::MissingAttribute(
                    PARAMETER_NAME_DSV_DELIMITER.to_string(),
                ));
            }
            (DsvVariant::CSV, None) => {
                delimiter = b',';
            }
            (DsvVariant::TSV, None) => {
                delimiter = b',';
            }
            (DsvVariant::CSV, Some(_)) | (DsvVariant::TSV, Some(_)) => {
                return Err(ImportExportError::UnknownAttribute(
                    PARAMETER_NAME_DSV_DELIMITER.to_string(),
                ));
            }
        }
        return Ok(delimiter);
    }
}

impl ImportExportHandler for DsvHandler {
    fn file_format(&self) -> FileFormat {
        match self.delimiter {
            b',' => FileFormat::CSV,
            b'\t' => FileFormat::TSV,
            _ => FileFormat::DSV,
        }
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        arity: usize,
    ) -> Result<Box<dyn TableProvider>, Error> {
        if let Some(ref vf) = self.value_formats {
            Ok(Box::new(DsvReader::new(read, self.delimiter, vf.clone())))
        } else {
            Ok(Box::new(DsvReader::new(
                read,
                self.delimiter,
                DsvHandler::formats_from_strings(DsvHandler::default_value_format_strings(arity))
                    .unwrap(),
            )))
        }
    }

    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DsvWriter::new(self.delimiter, writer)))
    }

    fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn arity(&self) -> Option<usize> {
        self.value_formats.as_ref().map(|vfs| vfs.len())
    }

    fn file_extension(&self) -> Option<String> {
        match self.file_format() {
            FileFormat::CSV => Some("csv".to_string()),
            FileFormat::DSV => Some("dsv".to_string()),
            FileFormat::TSV => Some("tsv".to_string()),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    // use quickcheck_macros::quickcheck;
    use test_log::test;

    use super::*;
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

        let reader = DsvReader::new(
            Box::new(data.as_bytes()),
            b';',
            vec![
                DsvValueFormat::ANYTHING,
                DsvValueFormat::STRING,
                DsvValueFormat::INTEGER,
                DsvValueFormat::DOUBLE,
            ],
        );
        let dict = RefCell::new(Dict::default());
        let mut tuple_writer = TupleWriter::new(&dict, 4);
        let result = reader.read(&mut tuple_writer);
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
