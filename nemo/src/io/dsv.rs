//! Reading of delimiter-separated value files
//!
//! This module provides [`DSVReader`], a [`TableReader`] that can parse DSV (delimiter-separated value) files.
//! It is typically instantiated by the logical layer with the logical types corresponding to each column in the file.
//! The physical layer then passes in [physical builder proxies][nemo_physical::builder_proxy::PhysicalColumnBuilderProxy]
//! that convert the read data into the appropriate storage types.
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
//! # use std::path::PathBuf;
//! # use nemo_physical::table_reader::TableReader;
//! # use nemo::{types::LogicalTypeEnum, io::dsv::DSVReader};
//! # let file = PathBuf::from("../resources/doc/examples/city_population.csv");
//! let csv_reader = DSVReader::csv(
//!     file,
//!     vec![
//!         LogicalTypeEnum::Any,
//!         LogicalTypeEnum::Integer,
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
//! # use std::path::PathBuf;
//! #
//! # use nemo::{types::LogicalTypeEnum, io::dsv::DSVReader};
//! # use std::cell::RefCell;
//! # use nemo_physical::builder_proxy::{
//! #    PhysicalBuilderProxyEnum, PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy
//! # };
//! # #[cfg(miri)]
//! # fn main() {}
//! # #[cfg(not(miri))]
//! # fn main() {
//! # let file = PathBuf::from("resources/doc/examples/city_population.csv");
//! #
//! # let csv_reader = DSVReader::csv(
//! #     file,
//! #     vec![
//! #         LogicalTypeEnum::Any,
//! #         LogicalTypeEnum::Integer,
//! #     ],
//! # );
//! # let table_reader:Box<dyn TableReader> = Box::new(csv_reader);
//! # let mut dict = RefCell::new(nemo_physical::dictionary::PrefixedStringDictionary::default());
//! let mut builder = vec![
//!     PhysicalBuilderProxyEnum::String(PhysicalStringColumnBuilderProxy::new(&dict)),
//!     PhysicalBuilderProxyEnum::I64(Default::default()),
//! ];
//! // Read the data into the builder
//! let result = table_reader.read_into_builder_proxies(&mut builder);
//! let columns = builder.into_iter().map(|bp| bp.finalize()).collect::<Vec<_>>();
//! # }
//! ```

use std::io::Read;
use std::path::PathBuf;

use csv::{Reader, ReaderBuilder};

use nemo_physical::builder_proxy::PhysicalBuilderProxyEnum;
use nemo_physical::table_reader::TableReader;

use crate::builder_proxy::LogicalColumnBuilderProxy;
use crate::error::{Error, ReadingError};
use crate::types::LogicalTypeEnum;

use crate::read_from_possibly_compressed_file;

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// The reader object relates a given [file][PathBuf] in DSV format to a tuple of [logical types][LogicalTypeEnum].
/// Via the implementation of [`TableReader`] it fills the corresponding [`PhysicalBuilderProxys`][nemo_physical::builder_proxy::PhysicalBuilderProxyEnum]
/// with the data from the file.
/// It combines the logical and physical BuilderProxies to handle the read data according to both datatype models.

#[derive(Debug, Clone)]
pub struct DSVReader {
    file: PathBuf,
    delimiter: u8,
    escape: Option<u8>,
    logical_types: Vec<LogicalTypeEnum>,
}

impl DSVReader {
    /// Instantiate a [DSVReader] for CSV (comma separated values) files
    pub fn csv(file: PathBuf, logical_types: Vec<LogicalTypeEnum>) -> Self {
        Self::dsv(file, b',', logical_types)
    }

    /// Instantiate a [DSVReader] for TSV (tab separated values) files
    pub fn tsv(file: PathBuf, logical_types: Vec<LogicalTypeEnum>) -> Self {
        Self::dsv(file, b'\t', logical_types)
    }

    /// Instantiate a [DSVReader] for a given delimiter
    pub fn dsv(file: PathBuf, delimiter: u8, logical_types: Vec<LogicalTypeEnum>) -> Self {
        Self {
            file,
            delimiter,
            escape: Some(b'\\'),
            logical_types,
        }
    }

    /// Static function to create a serde reader
    ///
    /// The function takes an arbitrary [`Reader`][Read] and wraps it into a [`Reader`][csv::Reader] for csv
    fn reader<R>(rdr: R, delimiter: u8, escape: Option<u8>) -> Reader<R>
    where
        R: Read,
    {
        ReaderBuilder::new()
            .delimiter(delimiter)
            .escape(escape)
            .has_headers(false)
            .double_quote(true)
            .from_reader(rdr)
    }

    /// Actually reads the data from the file and distributes the different fields into the corresponding [ProxyColumnBuilder]
    /// If a field cannot be read or parsed, the line will be ignored
    fn read_with_reader<'a, 'b, R>(
        &self,
        mut builder: Vec<Box<dyn LogicalColumnBuilderProxy<'a, 'b> + 'b>>,
        reader: &mut Reader<R>,
    ) -> Result<(), ReadingError>
    where
        R: Read,
    {
        for row in reader.records().flatten() {
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
        }

        Ok(())
    }

    fn read_into_builder_proxies_with_reader<'a: 'b, 'b, R>(
        &self,
        physical_builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
        reader: &mut Reader<R>,
    ) -> Result<(), ReadingError>
    where
        R: Read,
    {
        let logical_builder_proxies: Vec<Box<dyn LogicalColumnBuilderProxy + 'b>> = self
            .logical_types
            .iter()
            .cloned()
            .zip(physical_builder_proxies)
            .map(|(ty, bp)| ty.wrap_physical_column_builder::<'a, 'b>(bp))
            .collect();

        self.read_with_reader(logical_builder_proxies, reader)
    }
}

impl TableReader for DSVReader {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        &self,
        physical_builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        read_from_possibly_compressed_file!(self.file, |reader| {
            self.read_into_builder_proxies_with_reader(
                physical_builder_proxies,
                &mut Self::reader(reader, self.delimiter, self.escape),
            )
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
        datatypes::storage_value::VecT,
        dictionary::{Dictionary, PrefixedStringDictionary},
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

        let mut dict = std::cell::RefCell::new(PrefixedStringDictionary::default());
        let csvreader = DSVReader::csv(
            "test".into(),
            vec![
                LogicalTypeEnum::Any,
                LogicalTypeEnum::Any,
                LogicalTypeEnum::Any,
            ],
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
        assert_eq!(
            x[0].get(0)
                .and_then(|dvt| dvt.try_into().ok())
                .and_then(|u64: u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.get_mut().entry(usize))
                .unwrap(),
            "Boston"
        );
        assert_eq!(
            x[1].get(0)
                .and_then(|dvt| dvt.try_into().ok())
                .and_then(|u64: u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.get_mut().entry(usize))
                .unwrap(),
            "United States"
        );
        assert_eq!(
            x[2].get(0)
                .and_then(|dvt| dvt.try_into().ok())
                .and_then(|u64: u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.get_mut().entry(usize))
                .unwrap(),
            r#""4628910"^^<http://www.w3.org/2001/XMLSchema#integer>"#
        );
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
            ("Boston", "United States", r#""Some String""#, 4628910),
            ("Dresden", "Germany", r#""Another String""#, 1234567),
            (
                "My Home Town",
                r#""Some<where >Nice""#,
                r#""<https://string.parsing.should/not/change#that>""#,
                2,
            ),
            (
                "Trailing Spaces do not belong to the name",
                "What about spaces in the beginning though",
                r#""  what happens to spaces in string parsing?  ""#,
                123,
            ),
            (
                r#""Do String literals work?""#,
                r#""Even with datatype annotation?""#,
                r#"""even string literals should just be piped through"^^<http://www.w3.org/2001/XMLSchema#string>""#,
                456,
            ),
            ("The next 2 columns are empty", r#""""#, r#""""#, 789),
        ];

        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let mut dict = std::cell::RefCell::new(PrefixedStringDictionary::default());
        let csvreader = DSVReader::csv(
            "test".into(),
            vec![
                LogicalTypeEnum::Any,
                LogicalTypeEnum::Any,
                LogicalTypeEnum::String,
                LogicalTypeEnum::Integer,
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

        let VecT::U64(ref col0_idx) = cols[0] else { unreachable!() };
        let VecT::U64(ref col1_idx) = cols[1] else { unreachable!() };
        let VecT::U64(ref col2_idx) = cols[2] else { unreachable!() };
        let VecT::I64(ref col3) = cols[3] else { unreachable!() };

        let col0: Vec<String> = col0_idx
            .iter()
            .copied()
            .map(|idx| dict.get_mut().entry(idx.try_into().unwrap()).unwrap())
            .collect();
        let col1: Vec<String> = col1_idx
            .iter()
            .copied()
            .map(|idx| dict.get_mut().entry(idx.try_into().unwrap()).unwrap())
            .collect();
        let col2: Vec<String> = col2_idx
            .iter()
            .copied()
            .map(|idx| dict.get_mut().entry(idx.try_into().unwrap()).unwrap())
            .collect();

        col0.into_iter()
            .zip(col1)
            .zip(col2)
            .zip(col3)
            .map(|(((c0, c1), c2), c3)| (c0, c1, c2, *c3))
            .zip(
                expected_result
                    .into_iter()
                    .map(|(e0, e1, e2, e3)| (e0.to_string(), e1.to_string(), e2.to_string(), e3)),
            )
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

        let dict = std::cell::RefCell::new(PrefixedStringDictionary::default());
        let csvreader = DSVReader::csv(
            "test".into(),
            vec![
                LogicalTypeEnum::Any,
                LogicalTypeEnum::Integer,
                LogicalTypeEnum::Float64,
                LogicalTypeEnum::Float64,
                LogicalTypeEnum::Integer,
                LogicalTypeEnum::Any,
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
        let csvreader = DSVReader::csv(
            "test".into(),
            vec![
                LogicalTypeEnum::Integer,
                LogicalTypeEnum::Float64,
                LogicalTypeEnum::Integer,
                LogicalTypeEnum::Float64,
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
