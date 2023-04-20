//! Represents different data-import methods

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use crate::error::Error;
use crate::io::builder_proxy::{
    ColumnBuilderProxy, DoubleColumnBuilderProxy, FloatColumnBuilderProxy,
    StringColumnBuilderProxy, U32ColumnBuilderProxy, U64ColumnBuilderProxy,
};
use crate::physical::datatypes::storage_value::VecT;
use crate::physical::datatypes::DataTypeName;
use crate::physical::management::database::Dict;
use crate::physical::tabular::traits::table_schema::TableSchema;
use csv::{Reader, ReaderBuilder};

/// A reader Object, which allows to read a DSV (delimiter separated) file
#[derive(Debug, Clone)]
pub struct DSVReader {
    file: PathBuf,
    delimiter: u8,
    escape: Option<u8>,
}

impl DSVReader {
    /// Instantiate a [DSVReader] for CSV files
    pub fn csv(file: PathBuf) -> Self {
        Self::dsv(file, b',')
    }

    /// Instantiate a [DSVReader] for TSV files
    pub fn tsv(file: PathBuf) -> Self {
        Self::dsv(file, b'\t')
    }

    /// Instantiate a [DSVReader] for a given delimiter
    pub fn dsv(file: PathBuf, delimiter: u8) -> Self {
        Self {
            file,
            delimiter,
            escape: Some(b'\\'),
        }
    }

    /// Static function to create a serde reader
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
    /// Read the file, using the [TableSchema] and a [dictionary][Dict]
    /// Returns a Vector of [VecT] or a corresponding [Error]
    pub fn read(self, schema: &TableSchema, dictionary: &mut Dict) -> Result<Vec<VecT>, Error> {
        let gz_decoder =
            flate2::read::GzDecoder::new(File::open(self.file.as_path()).map_err(|error| {
                Error::IOReading {
                    error,
                    filename: self
                        .file
                        .to_str()
                        .expect("Path is expected to be valid utf-8")
                        .into(),
                }
            })?);
        let builder = self.generate_proxies(schema);
        if gz_decoder.header().is_some() {
            self.read_with_reader(
                builder,
                &mut Self::reader(gz_decoder, self.delimiter, self.escape),
                dictionary,
            )
        } else {
            self.read_with_reader(
                builder,
                &mut Self::reader(
                    File::open(self.file.as_path())?,
                    self.delimiter,
                    self.escape,
                ),
                dictionary,
            )
        }
    }

    /// Actually reads the data from the file and distributes the different fields into the corresponding [ProxyColumnBuilder]
    /// If a field cannot be read or parsed, the line will be ignored
    fn read_with_reader<R>(
        &self,
        mut builder: Vec<Box<dyn ColumnBuilderProxy>>,
        reader: &mut Reader<R>,
        dictionary: &mut Dict,
    ) -> Result<Vec<VecT>, Error>
    where
        R: Read,
    {
        for row in reader.records().flatten() {
            if let Err(Error::Rollback(rollback)) =
                row.iter().enumerate().try_for_each(|(idx, item)| {
                    if idx < builder.len() {
                        if let Err(column_err) = builder[idx].add(item, Some(dictionary)) {
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
        let mut result = Vec::with_capacity(builder.len());
        builder.reverse();
        while let Some(b) = builder.pop() {
            result.push(b.finalize());
        }
        Ok(result)
    }

    /// Given a TableSchema, generates the corresponding Proxy implementation
    fn generate_proxies(&self, schema: &TableSchema) -> Vec<Box<dyn ColumnBuilderProxy>> {
        schema
            .iter()
            .map(|data_type_name| -> Box<dyn ColumnBuilderProxy> {
                match data_type_name {
                    DataTypeName::String => Box::<StringColumnBuilderProxy>::default(),
                    DataTypeName::U32 => Box::<U32ColumnBuilderProxy>::default(),
                    DataTypeName::U64 => Box::<U64ColumnBuilderProxy>::default(),
                    DataTypeName::Float => Box::<FloatColumnBuilderProxy>::default(),
                    DataTypeName::Double => Box::<DoubleColumnBuilderProxy>::default(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};

    use super::*;
    use csv::ReaderBuilder;
    use quickcheck_macros::quickcheck;
    use test_log::test;

    #[test]
    fn csv_one_line() {
        let data = "\
city;country;pop
Boston;United States;4628910
";

        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let mut dict = PrefixedStringDictionary::default();
        let csvreader = DSVReader::csv("test".into());
        let result = csvreader.read_with_reader(
            vec![
                Box::<StringColumnBuilderProxy>::default(),
                Box::<StringColumnBuilderProxy>::default(),
                Box::<StringColumnBuilderProxy>::default(),
            ],
            &mut rdr,
            &mut dict,
        );
        let x = result.unwrap();

        assert_eq!(x.len(), 3);
        assert!(x.iter().all(|vect| vect.len() == 1));
        assert_eq!(
            x[0].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap(),
            "Boston"
        );
        assert_eq!(
            x[1].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap(),
            "United States"
        );
        assert_eq!(
            x[2].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap(),
            "4628910"
        );
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

        let mut dict = PrefixedStringDictionary::default();
        let csvreader = DSVReader::csv("test".into());
        let builder = csvreader.generate_proxies(&TableSchema::from_vec(vec![
            DataTypeName::String,
            DataTypeName::U64,
            DataTypeName::Double,
            DataTypeName::Float,
            DataTypeName::U64,
            DataTypeName::String,
        ]));
        let imported = csvreader.read_with_reader(builder, &mut rdr, &mut dict);

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 6);
        assert_eq!(imported.as_ref().unwrap()[1].len(), 3);
    }

    #[quickcheck]
    #[cfg_attr(miri, ignore)]
    fn csv_quickchecked(u64_vec: Vec<u64>, double_vec: Vec<f64>, float_vec: Vec<f32>) -> bool {
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
        let mut u64_vec = u64_vec;
        let len = double_vec.len().min(float_vec.len().min(u64_vec.len()));
        double_vec.truncate(len);
        float_vec.truncate(len);
        u64_vec.truncate(len);
        let mut csv = String::new();
        for i in 0..len {
            csv = format!(
                "{}\n{},{},{},{}",
                csv, i, double_vec[i], u64_vec[i], float_vec[i]
            );
        }

        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(false)
            .from_reader(csv.as_bytes());
        let mut dict = PrefixedStringDictionary::default();
        let csvreader = DSVReader::csv("test".into());
        let builder = csvreader.generate_proxies(&TableSchema::from_vec(vec![
            DataTypeName::U64,
            DataTypeName::Double,
            DataTypeName::U64,
            DataTypeName::Float,
        ]));

        let imported = csvreader.read_with_reader(builder, &mut rdr, &mut dict);

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 4);
        assert_eq!(imported.as_ref().unwrap()[0].len(), len);
        true
    }
}
