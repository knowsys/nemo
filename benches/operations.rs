use std::fs::File;

use criterion::{criterion_group, criterion_main, Criterion};
use csv::ReaderBuilder;
use polars::prelude::{CsvReader, DataFrame, DataType, JoinType, Schema, SerReader};
use stage2::io::csv::read;
use stage2::physical::tables::{
    materialize, IntervalTrieScan, TrieJoin, TrieProject, TrieScanEnum, TrieUnion,
};
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
            // Using fallback solution to treat eveything as string for now (storing as u64 internally)
            let datatypes: Vec<Option<DataTypeName>> = (0..order.len()).map(|_| None).collect();

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

            trie
        }
        _ => {
            unreachable!()
        }
    }
}

pub fn benchmark_join(c: &mut Criterion) {
    let mut dict = PrefixedStringDictionary::default();

    let table_a = DataSource::csv_file("test-files/bench/xe.csv").unwrap();
    let table_a_order: ColumnOrder = vec![0, 1, 2];
    let table_b = DataSource::csv_file("test-files/bench/aux.csv").unwrap();
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

    let file_a = File::open("test-files/bench/xe.csv").expect("could not open file");
    let file_b = File::open("test-files/bench/aux.csv").expect("could not open file");

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
        .sort(["AX", "AY"], vec![false, false])
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
        .sort(["BX", "BY"], vec![false, false])
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

fn benchmark_project(c: &mut Criterion) {
    let mut dict = PrefixedStringDictionary::default();

    let table_a = DataSource::csv_file("test-files/bench/xe.csv").unwrap();
    let table_a_order: ColumnOrder = vec![0, 1, 2];
    let table_b = DataSource::csv_file("test-files/bench/aux.csv").unwrap();
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

    let join_iter = TrieJoin::new(
        vec![
            TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
            TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
        ],
        &[vec![0, 1, 2], vec![0, 1, 3]],
        schema_target,
    );

    let join_trie = materialize(&mut TrieScanEnum::TrieJoin(join_iter));

    let mut group_ours = c.benchmark_group("trie_project");
    group_ours.sample_size(10);
    group_ours.bench_function("trie_project_hole", |b| {
        b.iter_with_setup(
            || TrieProject::new(&join_trie, vec![0, 3]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_project_beginning", |b| {
        b.iter_with_setup(
            || TrieProject::new(&join_trie, vec![0, 1]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_project_end", |b| {
        b.iter_with_setup(
            || TrieProject::new(&join_trie, vec![2, 3]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });

    group_ours.bench_function("trie_reorder_1", |b| {
        b.iter_with_setup(
            || TrieProject::new(&trie_b, vec![0, 2, 1]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_reorder_2", |b| {
        b.iter_with_setup(
            || TrieProject::new(&trie_b, vec![1, 0, 2]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_reorder_3", |b| {
        b.iter_with_setup(
            || TrieProject::new(&trie_b, vec![2, 1, 0]),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieProject(project_iter));
            },
        );
    });
    group_ours.finish();
}

fn benchmark_union(c: &mut Criterion) {
    const FILE_NAME: &str = "test-files/bench/aux-split/aux";
    const NUM_PARTS: usize = 10;

    let mut dict = PrefixedStringDictionary::default();

    let mut tries = Vec::<Trie>::new();
    let mut frames = Vec::<DataFrame>::new();
    for trie_index in 0..NUM_PARTS {
        let mut filename = FILE_NAME.to_string();
        filename += "-";
        filename += &trie_index.to_string();
        filename += ".csv";

        let table_source = DataSource::csv_file(&filename).unwrap();
        let table_order: ColumnOrder = vec![0, 1, 2];

        tries.push(load_trie(&table_source, &table_order, &mut dict));

        let file = File::open(filename).expect("could not open file");
        let table_schema = Schema::new()
            .insert_index(0, "X".to_string(), DataType::Utf8)
            .unwrap()
            .insert_index(0, "Y".to_string(), DataType::Utf8)
            .unwrap()
            .insert_index(0, "Z".to_string(), DataType::Utf8)
            .unwrap();

        let frame = CsvReader::new(file)
            .with_schema(&table_schema)
            .has_header(false)
            .finish()
            .unwrap()
            .sort(["X", "Y", "Z"], vec![false, false, false])
            .unwrap();

        frames.push(frame);
    }

    let mut group_ours = c.benchmark_group("trie_union");
    group_ours.sample_size(10);
    group_ours.bench_function("trie_union", |b| {
        b.iter_with_setup(
            || {
                TrieUnion::new(
                    tries
                        .iter()
                        .map(|trie| TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(trie)))
                        .collect(),
                )
            },
            |union_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieUnion(union_iter));
            },
        );
    });
    group_ours.finish();

    let last_frame = frames.pop().unwrap();

    let mut group_polar = c.benchmark_group("polar_union");
    group_polar.sample_size(10);
    group_polar.bench_function("polar_union", |b| {
        b.iter_with_setup(
            || {},
            |_| {
                let mut union_frame = last_frame.clone();
                for frame in &frames {
                    union_frame.vstack_mut(frame).unwrap();
                }

                union_frame
                    .unique(None, polars::prelude::UniqueKeepStrategy::First)
                    .unwrap();
            },
        );
    });
    group_polar.finish();
}

criterion_group!(benches, benchmark_join, benchmark_project, benchmark_union);
criterion_main!(benches);
