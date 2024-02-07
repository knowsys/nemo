use std::{
    cell::RefCell,
    time::{Duration, Instant},
};

use nemo_physical::{
    datasources::tuple_writer::TupleWriter, datatypes::StorageValueT, datavalues::AnyDataValue,
    dictionary::meta_dv_dict::MetaDvDictionary, tabular::trie::Trie,
};
use rand::Rng;

use crate::old::{self, trie::Table};

fn fill_random_rows_tuplewriter(length: usize, arity: usize, tuple_writer: &mut TupleWriter) {
    let mut rng = rand::thread_rng();
    for _ in 0..(arity * length) {
        tuple_writer.add_tuple_value(AnyDataValue::new_integer_from_i64(
            rng.gen_range(1..(length as i64)),
        ));
    }
}

pub fn random_integer_trie(length: usize, arity: usize) -> Trie {
    let dictionary = RefCell::new(MetaDvDictionary::new());
    let mut tuple_writer = TupleWriter::new(&dictionary, arity);

    fill_random_rows_tuplewriter(length, arity, &mut tuple_writer);

    Trie::from_tuple_writer(tuple_writer)
}

fn fill_random_rows_svt(length: usize, arity: usize) -> Vec<Vec<StorageValueT>> {
    let mut rng = rand::thread_rng();

    let mut rows = Vec::new();
    for _row in 0..length {
        let mut row = Vec::new();
        for _entry in 0..arity {
            row.push(StorageValueT::Int64(rng.gen_range(1..(length as i64))));
        }
        rows.push(row);
    }

    rows
}

pub fn random_integer_trie_old(length: usize, arity: usize) -> old::trie::Trie {
    let rows = fill_random_rows_svt(length, arity);

    old::trie::Trie::from_rows(&rows)
}

pub fn time_load_new_trie(length: usize, arity: usize) {
    let dictionary = RefCell::new(MetaDvDictionary::new());
    let mut tuple_writer = TupleWriter::new(&dictionary, arity);

    let start_time = Instant::now();
    fill_random_rows_tuplewriter(length, arity, &mut tuple_writer);
    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();
    println!("Load new (TW): {duration}ms");

    let start_time = Instant::now();
    let trie = Trie::from_tuple_writer(tuple_writer);
    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();
    println!("Load new (trie): {duration}ms, rows: {}", trie.num_rows());
}

pub fn time_load_old_trie(length: usize, arity: usize) {
    let rows = fill_random_rows_svt(length, arity);

    let start_time = Instant::now();
    let trie = old::trie::Trie::from_rows(&rows);
    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();
    println!("Load old (trie): {duration}ms, rows: {}", trie.row_num());
}
