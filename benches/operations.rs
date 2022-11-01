use std::fs::File;

use criterion::{criterion_group, criterion_main, Criterion};
use csv::ReaderBuilder;
use polars::prelude::{CsvReader, DataType, JoinType, Schema, SerReader};
use stage2::io::csv::read;
use stage2::physical::tables::{materialize, IntervalTrieScan, TrieJoin, TrieScanEnum};
use stage2::{
    logical::{model::DataSource, table_manager::ColumnOrder},
    physical::{
        datatypes::DataTypeName,
        dictionary::PrefixedStringDictionary,
        tables::{Table, Trie, TrieSchema, TrieSchemaEntry},
    },
};

fn load_trie(
    source: &DataSource,
    order: &ColumnOrder,
    dict: &mut PrefixedStringDictionary,
) -> Trie {
    match source {
        DataSource::CsvFile(file) => {
            log::info!("loading CSV file {file:?}");
            // TODO: not everything is u64 :D
            let datatypes: Vec<Option<DataTypeName>> = (0..order.len()).map(|_| None).collect();

            // let mut csv_reader = csv::Reader::from_path(file.as_path()).unwrap();
            let mut reader = ReaderBuilder::new()
                .delimiter(b',')
                .has_headers(false)
                .from_reader(File::open(file.as_path()).unwrap());

            let col_table = read(&datatypes, &mut reader, dict).unwrap();

            let schema = TrieSchema::new(
                (0..col_table.len())
                    .map(|i| TrieSchemaEntry {
                        label: i,
                        datatype: DataTypeName::U64,
                    })
                    .collect(),
            );

            let trie = Trie::from_cols(schema, col_table);

            assert!(trie.row_num() > 0);
            println!("{}", trie.row_num());

            return trie;
        }
        _ => {
            unreachable!()
        }
    }
}

pub fn benchmark_join(c: &mut Criterion) {
    let mut dict = PrefixedStringDictionary::default();

    let table_a = DataSource::csv_file("out-galen/xe.csv").unwrap();
    let table_a_order: ColumnOrder = vec![0, 1, 2];
    let table_b = DataSource::csv_file("out-galen/aux.csv").unwrap();
    let table_b_order: ColumnOrder = vec![0, 1, 2];

    let trie_a = load_trie(&table_a, &table_a_order, &mut dict);
    let trie_b = load_trie(&table_b, &table_b_order, &mut dict);

    let schema_target = TrieSchema::new(vec![
        TrieSchemaEntry {
            label: 100,
            datatype: DataTypeName::U64,
        },
        TrieSchemaEntry {
            label: 101,
            datatype: DataTypeName::U64,
        },
        TrieSchemaEntry {
            label: 102,
            datatype: DataTypeName::U64,
        },
        TrieSchemaEntry {
            label: 103,
            datatype: DataTypeName::U64,
        },
    ]);

    let mut group_ours = c.benchmark_group("trie_join");
    group_ours.sample_size(10);
    group_ours.bench_function("trie_join", |b| {
        b.iter_with_setup(
            || {
                TrieJoin::new(
                    vec![
                        TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                        TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
                    ],
                    &[vec![0, 1, 2], vec![0, 1, 3]],
                    schema_target.clone(),
                )
            },
            |join_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieJoin(join_iter));
            },
        );
    });
    group_ours.finish();

    let file_a = File::open("out-galen/xe.csv").expect("could not open file");
    let file_b = File::open("outt-galen/aux.csv").expect("could not open file");

    let table_a_schema = Schema::new()
        .insert_index(0, "AX".to_string(), DataType::Utf8)
        .unwrap()
        .insert_index(0, "AY".to_string(), DataType::Utf8)
        .unwrap()
        .insert_index(0, "AZ".to_string(), DataType::Utf8)
        .unwrap();

    let table_a = CsvReader::new(file_a)
        .with_schema(&table_a_schema)
        .has_header(false)
        .finish()
        .unwrap()
        .sort(&["AX", "AY"], vec![false, false])
        .unwrap();

    let table_b_schema = Schema::new()
        .insert_index(0, "BX".to_string(), DataType::Utf8)
        .unwrap()
        .insert_index(0, "BY".to_string(), DataType::Utf8)
        .unwrap()
        .insert_index(0, "BZ".to_string(), DataType::Utf8)
        .unwrap();

    let table_b = CsvReader::new(file_b)
        .with_schema(&table_b_schema)
        .has_header(false)
        .finish()
        .unwrap()
        .sort(&["BX", "BY"], vec![false, false])
        .unwrap();

    let mut group_polar = c.benchmark_group("polar_join");
    group_polar.sample_size(10);
    group_polar.bench_function("polar_join", |b| {
        b.iter_with_setup(
            || {},
            |_| {
                let _ = table_a
                    .join(
                        &table_b,
                        vec!["AX", "AY"],
                        vec!["BX", "BY"],
                        JoinType::Inner,
                        None,
                    )
                    .unwrap();
            },
        );
    });
    group_polar.finish();
}

criterion_group!(benches, benchmark_join);
criterion_main!(benches);
