//! Represents different data-import methods

use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::error::Error;
use crate::physical::columnar::proxy_builder::{
    ColumnBuilderProxy, DoubleColumnBuilderProxy, FloatColumnBuilderProxy,
    StringColumnBuilderProxy, U32ColumnBuilderProxy, U64ColumnBuilderProxy,
};
use crate::physical::datatypes::storage_value::VecT;
use crate::physical::datatypes::StorageTypeName;
use crate::physical::management::database::Dict;
use crate::physical::tabular::table_types::trie::DebugTrie;
use crate::physical::tabular::traits::table_schema::TableSchema;
use csv::{Reader, ReaderBuilder};
use flate2::write::GzEncoder;
use flate2::Compression;
use sanitise_file_name::{sanitise_with_options, Options};

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
        let gz_decoder = flate2::read::GzDecoder::new(File::open(self.file.as_path())?);
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
                    // Write the last line, if one exists
                    // This has been done till the rollback-index, therefore the old data needs to be written
                    if idx > rollback {
                        builder.write();
                    }
                    builder.rollback();
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
            .get_entries()
            .iter()
            .map(|schema_entry| -> Box<dyn ColumnBuilderProxy> {
                if schema_entry.dict {
                    // assume it is just a string for now
                    // TODO: implement for all proxies and types
                    Box::<StringColumnBuilderProxy>::default()
                } else {
                    match schema_entry.type_name {
                        StorageTypeName::U32 => Box::<U32ColumnBuilderProxy>::default(),
                        StorageTypeName::U64 => Box::<U64ColumnBuilderProxy>::default(),
                        StorageTypeName::Float => Box::<FloatColumnBuilderProxy>::default(),
                        StorageTypeName::Double => Box::<DoubleColumnBuilderProxy>::default(),
                    }
                }
            })
            .collect()
    }
}

/// Contains all the needed information, to write results into csv-files
#[derive(Debug)]
pub struct CSVWriter<'a> {
    /// The path to where the results shall be written to.
    path: &'a PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// Gzip csv-file.
    gzip: bool,
}

impl<'a> CSVWriter<'a> {
    /// Instantiate a [`CSVWriter`].
    ///
    /// Returns [`Ok`] if the given `path` is writeable. Otherwise an [`Error`] is thrown.
    /// TODO: handle constant dict correctly
    pub fn try_new(path: &'a PathBuf, overwrite: bool, gzip: bool) -> Result<Self, Error> {
        create_dir_all(path)?;
        Ok(CSVWriter {
            path,
            overwrite,
            gzip,
        })
    }
}

impl CSVWriter<'_> {
    /// Creates a csv-file and returns a [`File`] handle to write data.
    pub fn create_file(&self, pred: &str) -> Result<File, Error> {
        let sanitise_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };
        let file_name = sanitise_with_options(pred, &sanitise_options);
        let file_path = PathBuf::from(format!(
            "{}/{file_name}.csv{}",
            self.path
                .as_os_str()
                .to_str()
                .expect("Path should be a unicode string"),
            if self.gzip { ".gz" } else { "" }
        ));
        log::info!("Creating {pred} as {file_path:?}");
        let mut options = OpenOptions::new();
        options.write(true);
        if self.overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        };
        options.open(file_path).map_err(|err| err.into())
    }
    /// Writes a predicate as a csv-file into the corresponding result-directory
    /// # Parameters
    /// * `pred` is a [`&str`], representing a predicate name
    /// * `trie` is the [`Trie`] which holds all the content of the given predicate
    /// * `dict` is a [`Dictionary`], containing the mapping of the internal number representation to a [`String`]
    /// # Returns
    /// * [`Ok`][std::result::Result::Ok] if the predicate could be written to the csv-file
    /// * [`Error`] in case of any issues during writing the file
    pub fn write_predicate(&self, pred: &str, trie: DebugTrie) -> Result<(), Error> {
        log::debug!("Writing {pred}");
        let mut file = self.create_file(pred)?;
        let content = trie;
        if self.gzip {
            write!(GzEncoder::new(file, Compression::best()), "{}", &content)?;
        } else {
            write!(file, "{}", &content)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        dictionary::{Dictionary, PrefixedStringDictionary},
        tabular::traits::table_schema::TableSchemaEntry,
    };

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

        log::error!(
            "{:?}{:?}{:?}",
            x[0].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap(),
            x[1].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap(),
            x[2].get(0)
                .and_then(|dvt| dvt.as_u64())
                .and_then(|u64| usize::try_from(u64).ok())
                .and_then(|usize| dict.entry(usize))
                .unwrap()
        );
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
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: true,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::Double,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::Float,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: true,
                nullable: false,
            },
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
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::Double,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::U64,
                dict: false,
                nullable: false,
            },
            TableSchemaEntry {
                type_name: StorageTypeName::Float,
                dict: false,
                nullable: false,
            },
        ]));

        let imported = csvreader.read_with_reader(builder, &mut rdr, &mut dict);

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 4);
        assert_eq!(imported.as_ref().unwrap()[0].len(), len);
        true
    }
}
