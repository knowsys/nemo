use super::csv::read;
use crate::error::Error;
use crate::logical::permutator::Permutator;
use crate::physical::datatypes::{data_value::VecT, DataTypeName};
use crate::physical::tables::Trie;
use csv::Reader;

// Read csv file and return an [`IntervalTrie`]
// TODO: Should work with more input formats than just csv
// TODO: Should either read data types from the file or infer them from the data
// TODO: Read table headers and store them in a dictionary
pub fn read_trie<T>(
    datatypes: &[Option<DataTypeName>],
    csv_reader: &mut Reader<T>,
) -> Result<Trie, Error>
where
    T: std::io::Read,
{
    let reader_data = read(datatypes, csv_reader)?;
    let permutator = Permutator::sort_from_multiple_vec(&reader_data);
}
