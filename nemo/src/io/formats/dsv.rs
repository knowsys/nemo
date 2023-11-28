//! Reading of delimiter-separated value files
//!
//! This module provides [`DSVReader`], a [`TableProvider`] that can parse DSV (delimiter-separated value) files.
//! CSV does not specify how individual values are to be interpreted, and it has no type system, so we have to make
//! some choices here. Our most general approach is to first try and process string values as RDF terms (written as
//! in RDF formats like Turtle), and then to fall back to IRIs and (finally) strings. More specific approaches exist,
//! e.g., for integers, doubles, and strings (which will use values verbatim as string contents, even when an RDF-like
//! syntax was used).
//!
//! # Examples
//! On the logical layer, the [`DSVReader`] is created.
//! The following examples use this csv-file, which is representing a tuple with one string (logical any) and one number (logical integer) terms.
//! ```csv
//! Boston,654776
//! Dresden,554649
//! ```
//! This file can be found in the current repository at `resources/doc/examples/city_population.csv`
//! ## Logical layer
//! ```
//! # use nemo_physical::table_reader::TableReader;
//! # use nemo::{model::{DsvFile, PrimitiveType, TypeConstraint}, io::{resource_providers::ResourceProviders, formats::DSVReader}};
//! # let file_path = String::from("../resources/doc/examples/city_population.csv");
//! let csv_reader = DSVReader::dsv(
//!     ResourceProviders::default(),
//!     &DsvFile::csv_file(
//!         &file_path,
//!         [PrimitiveType::Any, PrimitiveType::Integer]
//!             .into_iter()
//!             .map(TypeConstraint::AtLeast)
//!             .collect(),
//!     ),
//!     vec![
//!         PrimitiveType::Any,
//!         PrimitiveType::Integer,
//!     ],
//! );
//! // Pack the csv_reader into a [`TableReader`] trait object for the physical layer
//! let table_reader: Box<dyn TableReader> = Box::new(csv_reader);
//! ```
//!
//! The example first creates a [`DSVReader`] which uses a comma as its separator.
//! It passes the reader to the physical layer as a [`TableReader`] object.
//!
//! ## Physical layer
//! The physical layer receives `table_reader`, the [`TableReader`] trait object from the example above.
//! ```
//! # use nemo_physical::table_reader::TableReader;
//! #
//! # use nemo::{model::{DsvFile, PrimitiveType, TypeConstraint}, io::{resource_providers::ResourceProviders, formats::DSVReader}};
//! # use std::cell::RefCell;
//! # use nemo_physical::builder_proxy::{
//! #    PhysicalBuilderProxyEnum, PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy
//! # };
//! # #[cfg(miri)]
//! # fn main() {}
//! # #[cfg(not(miri))]
//! # fn main() {
//! # let file_path = String::from("resources/doc/examples/city_population.csv");
//! #
//! # let csv_reader = DSVReader::dsv(
//! #     ResourceProviders::default(),
//! #     &DsvFile::csv_file(
//! #         &file_path,
//! #         [PrimitiveType::Any, PrimitiveType::Integer]
//! #             .into_iter()
//! #             .map(TypeConstraint::AtLeast)
//! #             .collect(),
//! #     ),
//! #     vec![
//! #         PrimitiveType::Any,
//! #         PrimitiveType::Integer,
//! #     ],
//! # );
//! # let table_reader:Box<dyn TableReader> = Box::new(csv_reader);
//! # let mut dict = RefCell::new(nemo_physical::management::database::Dict::default());
//! let mut builder = vec![
//!     PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
//!     PhysicalBuilderProxyEnum::I64(Default::default()),
//! ];
//! // Read the data into the builder
//! let result = table_reader.read_into_builder_proxies(&mut builder);
//! let columns = builder.into_iter().map(|bp| bp.finalize()).collect::<Vec<_>>();
//! # }
//! ```

use std::io::{BufReader, Read};

use csv::{Reader, ReaderBuilder};

use nemo_physical::datasources::{TableProvider, TableWriter};
use nemo_physical::datavalues::{AnyDataValue, DataValueCreationError};
use nemo_physical::table_reader::Resource;
use oxiri::Iri;
use rio_api::model::{Term, Triple};
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleParser;

use crate::io::formats::RDFTriplesReader;
use crate::model::{DataSource, DsvFile, TupleConstraint, TypeConstraint};
use crate::{
    io::parser::{parse_bare_name, span_from_str},
    io::{formats::PROGRESS_NOTIFY_INCREMENT, resource_providers::ResourceProviders},
    model::PrimitiveType,
};

type DataValueParserFunction = fn(String) -> Result<AnyDataValue, DataValueCreationError>;

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
pub struct DSVReader {
    resource_providers: ResourceProviders,
    resource: Resource,
    delimiter: u8,
    escape: u8,
    input_type_constraint: TupleConstraint,
}

impl DSVReader {
    /// Instantiate a [DSVReader] for a given delimiter
    pub fn dsv(resource_providers: ResourceProviders, dsv_file: &DsvFile) -> Self {
        Self {
            resource_providers,
            resource: dsv_file.resource.clone(),
            delimiter: dsv_file.delimiter,
            escape: b'\\',
            input_type_constraint: dsv_file.input_types(),
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
        table_writer: &mut TableWriter,
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
                    table_writer.next_value(dv);
                } else {
                    drop_count += 1;
                    table_writer.drop_current_row();
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
                    result = Some(RDFTriplesReader::datavalue_from_literal(lit));
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
        if let Ok((remainder, _)) = parse_bare_name(span_from_str(trimmed)) {
            if remainder.is_empty() {
                return Ok(AnyDataValue::new_iri(trimmed.to_string().into()));
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

impl TableProvider for DSVReader {
    fn provide_table_data(
        self: Box<Self>,
        table_writer: &mut TableWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = self
            .resource_providers
            .open_resource(&self.resource, true)?;

        let mut dsv_reader = Self::dsv_reader(reader, self.delimiter, Some(self.escape));

        self.read(table_writer, &mut dsv_reader)
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

        let reader = DSVReader::dsv(
            ResourceProviders::empty(),
            &DsvFile::csv_file(
                "test",
                [
                    PrimitiveType::Any,
                    PrimitiveType::String,
                    PrimitiveType::Integer,
                    PrimitiveType::Float64,
                ]
                .into_iter()
                .map(TypeConstraint::AtLeast)
                .collect(),
            ),
        );
        let dict = RefCell::new(Dict::default());
        let mut table_writer = TableWriter::new(&dict, 4);
        let result = reader.read(&mut table_writer, &mut rdr);
        assert!(result.is_ok());
        assert_eq!(table_writer.size(), 2);
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
