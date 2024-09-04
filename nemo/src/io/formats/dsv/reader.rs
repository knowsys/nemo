//! The reader for DSV files.

use std::io::BufRead;
use std::mem::size_of;

use bytesize::ByteSize;
use csv::{Reader, ReaderBuilder};
use nemo_physical::management::bytesized::ByteSized;

use nemo_physical::datasources::{table_providers::TableProvider, tuple_writer::TupleWriter};

use crate::io::formats::PROGRESS_NOTIFY_INCREMENT;

use super::value_format::{DataValueParserFunction, DsvValueFormat, DsvValueFormats};

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// Parsing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// as defined by [DsvValueFormat].
pub(super) struct DsvReader {
    /// Buffer from which content is read
    read: Box<dyn BufRead>,

    /// Delimiter used to separate values in the file
    delimiter: u8,
    /// Escape character used
    escape: Option<u8>,
    /// List of [DsvValueFormat] indicating for each column
    /// the type of value parser that should be use
    value_formats: DsvValueFormats,
    /// Maximum number of entries that should be read.
    limit: Option<u64>,
}

impl DsvReader {
    /// Instantiate a [DsvReader] for a given delimiter
    pub(super) fn new(
        read: Box<dyn BufRead>,
        delimiter: u8,
        value_formats: DsvValueFormats,
        escape: Option<u8>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            read,
            delimiter,
            value_formats,
            escape,
            limit,
        }
    }

    /// Create a low-level reader for parsing the DSV format.
    fn reader(self) -> Reader<Box<dyn BufRead>> {
        ReaderBuilder::new()
            .delimiter(self.delimiter)
            .escape(self.escape)
            .has_headers(false)
            .double_quote(true)
            .from_reader(self.read)
    }

    /// Actually reads the data from the file, using the given parsers to convert strings to [AnyDataValue]s.
    /// If a field cannot be read or parsed, the line will be ignored
    fn read(self, tuple_writer: &mut TupleWriter) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("Starting data import");

        let parsers: Vec<DataValueParserFunction> = self
            .value_formats
            .iter()
            .map(|vf| vf.data_value_parser_function())
            .collect();
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == DsvValueFormat::Skip)
            .collect();
        let expected_file_arity = parsers.len();
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

        let stop_limit = self.limit.unwrap_or(0);

        let mut dsv_reader = self.reader();

        let mut line_count: u64 = 0;
        let mut drop_count: u64 = 0;
        for row in dsv_reader.records().flatten() {
            for (idx, value) in row.iter().enumerate() {
                if idx >= expected_file_arity || skip[idx] {
                    continue;
                }
                if let Ok(dv) = parsers[idx](value.to_string()) {
                    tuple_writer.add_tuple_value(dv);
                } else {
                    drop_count += 1;
                    tuple_writer.drop_current_tuple();
                    break;
                }
            }

            line_count += 1;
            if (line_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                log::info!("... processed {line_count} lines");
            }
            if line_count == stop_limit {
                break;
            }
        }
        log::info!("Finished import: processed {line_count} lines (dropped {drop_count})");

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
            .field("value formats", &self.value_formats)
            .finish()
    }
}

impl ByteSized for DsvReader {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    // use quickcheck_macros::quickcheck;
    use test_log::test;

    use crate::io::formats::dsv::{
        reader::DsvReader,
        value_format::{DsvValueFormat, DsvValueFormats},
    };
    use nemo_physical::{datasources::tuple_writer::TupleWriter, management::database::Dict};

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
            DsvValueFormats::new(vec![
                DsvValueFormat::Anything,
                DsvValueFormat::String,
                DsvValueFormat::Integer,
                DsvValueFormat::Double,
            ]),
            None,
            None,
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
