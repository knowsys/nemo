//! Represents different data-import methods

use std::ops::Deref;

use crate::error::Error;
use crate::physical::datatypes::{DataTypeName, DataValueT, Double, Float};
use csv::Reader;

/// Imports a csv file
pub fn csv<T>(
    datatypes: &[Option<DataTypeName>],
    csv_reader: &mut Reader<T>,
) -> Result<Vec<VecT>, Error>
where
    T: std::io::Read,
{
    let mut result: Vec<Option<VecT>> = Vec::new();

    datatypes.iter().for_each(|dtype| {
        result.push(dtype.and_then(|dt| {
            Some(match dt {
                DataTypeName::U64 => VecT::VecU64(Vec::new()),
                DataTypeName::Float => VecT::VecFloat(Vec::new()),
                DataTypeName::Double => VecT::VecDouble(Vec::new()),
            })
        }));
    });
    csv_reader.records().for_each(|x| {});
    Ok(result.into_iter().flatten().collect())
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec<u64>
    VecU64(Vec<u64>),
    /// Case Vec<Float>
    VecFloat(Vec<Float>),
    /// Case Vec<Double>
    VecDouble(Vec<Double>),
}

#[cfg(test)]
mod test {
    use super::*;

    use csv::ReaderBuilder;
    #[test]
    fn csv() {
        let data = "\
city;country;pop
Boston;United States;4628910
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let x = super::csv(&[], &mut rdr);
    }
}
