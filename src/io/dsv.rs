//! Represents different data-import methods

use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

use crate::error::Error;
use crate::io::builder_proxy::{
    ColumnBuilderProxy, DoubleColumnBuilderProxy, FloatColumnBuilderProxy,
    StringColumnBuilderProxy, U32ColumnBuilderProxy, U64ColumnBuilderProxy,
};
use crate::logical::model::Identifier;
use crate::physical::datatypes::storage_value::VecT;
use crate::physical::datatypes::DataTypeName;
use crate::physical::dictionary::Dictionary;
use crate::physical::management::database::Dict;
use crate::physical::tabular::traits::table_schema::TableSchema;
use csv::{Reader, ReaderBuilder};
use flate2::write::GzEncoder;
use flate2::Compression;

use super::{FileCompression, FileFormat};

/// Compression level for gzip output, cf. gzip(1):
///
/// > Regulate the speed of compression using the specified digit #,
/// > where -1 or --fast indicates the fastest compression method (less
/// > compression) and -9 or --best indicates the slowest compression
/// > method (best compression).  The default compression level is -6
/// > (that is, biased towards high compression at expense of speed).
const GZIP_COMPRESSION_LEVEL: Compression = Compression::new(6);

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

/// Contains all the needed information, to write results into csv-files
#[derive(Debug)]
pub struct CSVWriter<'a> {
    /// The path to where the results shall be written to.
    path: &'a PathBuf,
    /// Overwrite files, note that the target folder will be emptied if `overwrite` is set to [true].
    overwrite: bool,
    /// Compression and file format.
    pub compression_format: FileCompression,
}

impl<'a> CSVWriter<'a> {
    /// Instantiate a [`CSVWriter`].
    ///
    /// Returns [`Ok`] if the given `path` is writeable. Otherwise an [`Error`] is thrown.
    pub fn try_new(path: &'a PathBuf, overwrite: bool, gzip: bool) -> Result<Self, Error> {
        create_dir_all(path)?;
        let file_format = FileFormat::DSV(b',');
        let compression_format = if gzip {
            FileCompression::Gzip(file_format)
        } else {
            FileCompression::None(file_format)
        };
        Ok(CSVWriter {
            path,
            overwrite,
            compression_format,
        })
    }
}

impl CSVWriter<'_> {
    /// Creates a `.csv` (or possibly a `.csv.gz`) file for predicate
    /// [`pred`] and returns a [`BufWriter<File>`] to it.
    pub fn create_file(&self, pred: &Identifier) -> Result<(BufWriter<File>, String), Error> {
        let mut options = OpenOptions::new();
        options.write(true);
        if self.overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        };
        let pred_path = pred.sanitised_file_name(self.path.to_path_buf(), self.compression_format);
        let file_name_with_extensions = pred_path
            .to_str()
            .expect("Path is expected to be utf-printable")
            .to_string();

        options
            .open(&pred_path)
            .map(|file| (BufWriter::new(file), file_name_with_extensions.clone()))
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::AlreadyExists => Error::IOExists {
                    error: err,
                    filename: file_name_with_extensions,
                },
                _ => err.into(),
            })
    }

    /// Writes a predicate as a csv-file into the corresponding result-directory
    /// # Parameters
    /// * `pred` is a [`&Identifier`], representing a predicate
    /// * `schema` is the [`TableSchema`] associated with the prediacte
    /// * `cols` is vector of [`VecT`] respresenting columns
    /// * `dict` is a [`Dictionary`], containing the mapping of the internal number representation to a [`String`]
    /// # Returns
    /// * [`Ok`][std::result::Result::Ok] if the predicate could be written to the csv-file
    /// * [`Error`] in case of any issues during writing the file
    pub fn write_predicate(
        &self,
        pred: &Identifier,
        schema: &TableSchema,
        cols: Vec<VecT>,
        dict: &Dict,
    ) -> Result<(), Error> {
        log::debug!("Writing {}", pred.name());
        let (file, filename) = self.create_file(pred)?;
        log::debug!("Outputting into {filename}");

        let num_rows = if let Some(col) = cols.first() {
            col.len()
        } else {
            return Ok(()); // there is nothing to write
        };

        let mut writer: Box<dyn Write> = match self.compression_format {
            FileCompression::Gzip(_) => Box::new(GzEncoder::new(file, GZIP_COMPRESSION_LEVEL)),
            FileCompression::None(_) => Box::new(file),
        };

        // TODO: have a similar solution as for the builder proxies for writing to prevent unpacking enums over and over again
        for row_idx in 0..num_rows {
            let row: Vec<String> = (0..cols.len()).map(|col_idx| {
                match schema[col_idx] {
                    DataTypeName::String => match &cols[col_idx] {
                        VecT::U64(val) => dict.entry(val[row_idx].try_into().expect("We were able to store this so we should be able to read this as well.")).unwrap_or_else(|| format!("<__Null#{}>", val[row_idx])),
                        _ => unreachable!("DataType and Storage Type are incompatible. This should never happen!"),
                    }
                    DataTypeName::U64 => match &cols[col_idx] {
                        VecT::U64(val) => val[row_idx].to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                        _ => unreachable!("DataType and Storage Type are incompatible. This should never happen!"),
                    }
                    DataTypeName::U32 => match &cols[col_idx] {
                        VecT::U32(val) => val[row_idx].to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                        _ => unreachable!("DataType and Storage Type are incompatible. This should never happen!"),
                    }
                    DataTypeName::Float => match &cols[col_idx] {
                        VecT::Float(val) => val[row_idx].to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                        _ => unreachable!("DataType and Storage Type are incompatible. This should never happen!"),
                    }
                    DataTypeName::Double => match &cols[col_idx] {
                        VecT::Double(val) => val[row_idx].to_string(), // TODO: do we allow nulls here? if yes, how do we distiguish them?
                        _ => unreachable!("DataType and Storage Type are incompatible. This should never happen!"),
                    }
                }
            }).collect();

            writeln!(writer, "{}", &row.join(",")).map_err(|error| Error::IOWriting {
                error,
                filename: filename.clone(),
            })?;
        }

        writer.flush().map_err(|error| Error::IOWriting {
            error,
            filename: filename.clone(),
        })?;

        Ok(())
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
