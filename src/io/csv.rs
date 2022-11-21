//! Represents different data-import methods

use crate::error::Error;
use crate::physical::datatypes::{data_value::VecT, DataTypeName};
use crate::physical::dictionary::Dictionary;
use csv::Reader;

/// Imports a csv file
/// Needs a list of Options of [DataTypeName] and a [csv::Reader] reference, as well as a [Dictionary][crate::physical::dictionary::Dictionary]
/// # Parameters
/// * `datatypes` this is a list of [`DataTypeName`] options, which needs to match the number of fields in the csv-file.
///   If the Option is [`None`] the field will be ignored. [`Some(DataTypeName)`] describes the datatype of the field in the csv-file.
/// # Behaviour
/// If a given datatype from `datatypes` is not matching the value in the field (i.e. it cannot be parsed into such a value), the whole line will be ignored and an error massage is emitted to the log.
pub fn read<T>(
    datatypes: &[Option<DataTypeName>],
    csv_reader: &mut Reader<T>,
    dict: &mut dyn Dictionary,
) -> Result<Vec<VecT>, Error>
where
    T: std::io::Read,
{
    let mut result: Vec<Option<VecT>> = Vec::new();

    datatypes.iter().for_each(|dtype| {
        result.push(dtype.and_then(|dt| {
            Some(match dt {
                DataTypeName::U64 => VecT::U64(Vec::new()),
                DataTypeName::Float => VecT::Float(Vec::new()),
                DataTypeName::Double => VecT::Double(Vec::new()),
                DataTypeName::String => VecT::String(Vec::new()),
            })
        }));
    });
    csv_reader.records().for_each(|rec| {
        if let Ok(row) = rec {
            log::trace!("imported row: {:?}", row);
            if let Err(Error::RollBack(rollback)) =
                row.iter().enumerate().try_for_each(|(idx, item)| {
                    if let Some(datatype) = datatypes[idx] {
                        match datatype.parse(item, dict) {
                            Ok(val) => {
                                result[idx].as_mut().map(|vect| {
                                    vect.push(&val);
                                    Some(())
                                });
                                Ok(())
                            }
                            Err(e) => {
                                log::error!("Ignoring line {:?}, parsing failed: {}", row, e);
                                Err(Error::RollBack(idx))
                            }
                        }
                    } else {
                        Ok(())
                    }
                })
            {
                for item in result.iter_mut().take(rollback) {
                    if let Some(vec) = item.as_mut() {
                        vec.pop()
                    }
                }
            }
        }
    });
    Ok(result.into_iter().flatten().collect())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::physical::dictionary::PrefixedStringDictionary;
    use csv::ReaderBuilder;
    use quickcheck_macros::quickcheck;
    use test_log::test;

    #[test]
    fn csv_empty() {
        let data = "\
city;country;pop
Boston;United States;4628910
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let x = read(
            &[None, None, None],
            &mut rdr,
            &mut PrefixedStringDictionary::new(),
        );
        assert!(x.is_ok());
        assert_eq!(x.unwrap().len(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn csv_with_ignored_and_faulty() {
        let data = "\
10;20;30;40;20;valid
asdf;12.2;413;22.3;23;invalid
node01;22;33.33;12.333332;10;valid again
node02;1312;12.33;313;1431;valid
node03;123;123;13;55;123;invalid
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(data.as_bytes());

        let imported = read(
            &[
                None,
                Some(DataTypeName::U64),
                Some(DataTypeName::Double),
                Some(DataTypeName::Float),
                Some(DataTypeName::U64),
                Some(DataTypeName::String),
            ],
            &mut rdr,
            &mut PrefixedStringDictionary::new(),
        );

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 5);
        assert_eq!(imported.as_ref().unwrap()[0].len(), 3);
        log::debug!("imported: {:?}", imported);
        assert_eq!(
            imported.as_ref().unwrap()[4]
                .get(0)
                .map(|v| v.as_string().unwrap()),
            Some(0usize)
        );
        assert_eq!(
            imported.as_ref().unwrap()[4]
                .get(1)
                .map(|v| v.as_string().unwrap()),
            Some(1usize)
        );
        assert_eq!(
            imported.as_ref().unwrap()[4]
                .get(2)
                .map(|v| v.as_string().unwrap()),
            Some(0usize)
        );
        assert_eq!(
            imported.as_ref().unwrap()[4]
                .get(3)
                .map(|v| v.as_string().unwrap()),
            None
        );
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
        let imported = read(
            &[
                Some(DataTypeName::U64),
                Some(DataTypeName::Double),
                Some(DataTypeName::U64),
                Some(DataTypeName::Float),
            ],
            &mut rdr,
            &mut PrefixedStringDictionary::new(),
        );

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 4);
        assert_eq!(imported.as_ref().unwrap()[0].len(), len);
        true
    }
}
