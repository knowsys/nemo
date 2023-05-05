use std::{cell::RefCell, fs::File};

use criterion::{criterion_group, criterion_main, Criterion};
use polars::prelude::{CsvReader, DataFrame, DataType, JoinType, Schema, SerReader};

use nemo::{
    io::{
        builder_proxy::{
            PhysicalBuilderProxyEnum, PhysicalColumnBuilderProxy, PhysicalStringColumnBuilderProxy,
        },
        dsv::DSVReader,
        TableReader,
    },
    logical::{model::DataSource, types::LogicalTypeEnum},
    physical::{
        datatypes::{storage_value::VecT, DataTypeName},
        dictionary::PrefixedStringDictionary,
        tabular::{
            operations::{
                materialize, triescan_project::ProjectReordering, JoinBindings, TrieScanJoin,
                TrieScanProject, TrieScanUnion,
            },
            table_types::trie::{Trie, TrieScanGeneric},
            traits::{table::Table, table_schema::TableSchema, triescan::TrieScanEnum},
        },
    },
};

// NOTE: See TableStorage::load_from_disk
fn load_trie(
    source: &DataSource,
    arity: usize,
    dict: &mut RefCell<PrefixedStringDictionary>,
) -> Trie {
    match source {
        DataSource::DsvFile { file, delimiter } => {
            // Using fallback solution to treat eveything as string for now (storing as u64 internally)
            let datatypeschema =
                TableSchema::from_vec((0..arity).map(|_| DataTypeName::String).collect());
            let logical_types = (0..arity).map(|_| LogicalTypeEnum::Any).collect();

            let mut builder_proxies: Vec<PhysicalBuilderProxyEnum> = datatypeschema
                .iter()
                .map(|data_type| match data_type {
                    DataTypeName::String => PhysicalBuilderProxyEnum::String(
                        PhysicalStringColumnBuilderProxy::new(dict),
                    ),
                    DataTypeName::U64 => PhysicalBuilderProxyEnum::U64(Default::default()),
                    DataTypeName::U32 => PhysicalBuilderProxyEnum::U32(Default::default()),
                    DataTypeName::Float => PhysicalBuilderProxyEnum::Float(Default::default()),
                    DataTypeName::Double => PhysicalBuilderProxyEnum::Double(Default::default()),
                })
                .collect();

            let csv_reader = DSVReader::dsv(*file.clone(), *delimiter, logical_types);
            csv_reader
                .read_into_builder_proxies(&mut builder_proxies)
                .expect("Should work");

            let col_table: Vec<VecT> = builder_proxies
                .into_iter()
                .map(|bp| match bp {
                    PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
                    PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
                    PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
                    PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
                    PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
                })
                .collect();

            let trie = Trie::from_cols(col_table);

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
    let mut dict = RefCell::new(PrefixedStringDictionary::default());

    let table_a = DataSource::csv_file("test-files/bench-data/xe.csv").unwrap();
    let table_a_arity = 3;
    let table_b = DataSource::csv_file("test-files/bench-data/aux.csv").unwrap();
    let table_b_arity = 3;

    let trie_a = load_trie(&table_a, table_a_arity, &mut dict);
    let trie_b = load_trie(&table_b, table_b_arity, &mut dict);

    let mut group_ours = c.benchmark_group("trie_join");
    group_ours.sample_size(10);
    group_ours.bench_function("trie_join", |b| {
        b.iter_with_setup(
            || {
                TrieScanJoin::new(
                    vec![
                        TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                        TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
                    ],
                    &JoinBindings::new(vec![vec![0, 1, 2], vec![0, 1, 3]]),
                )
            },
            |join_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter));
            },
        );
    });
    group_ours.finish();

    let file_a = File::open("test-files/bench-data/xe.csv").expect("could not open file");
    let file_b = File::open("test-files/bench-data/aux.csv").expect("could not open file");

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
    let mut dict = RefCell::new(PrefixedStringDictionary::default());

    let table_a = DataSource::csv_file("test-files/bench-data/xe.csv").unwrap();
    let table_a_arity = 3;
    let table_b = DataSource::csv_file("test-files/bench-data/aux.csv").unwrap();
    let table_b_arity = 3;

    let trie_a = load_trie(&table_a, table_a_arity, &mut dict);
    let trie_b = load_trie(&table_b, table_b_arity, &mut dict);

    let join_iter = TrieScanJoin::new(
        vec![
            TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
            TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
        ],
        &JoinBindings::new(vec![vec![0, 1, 2], vec![0, 1, 3]]),
    );

    let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter)).unwrap();

    let mut group_ours = c.benchmark_group("trie_project");
    group_ours.sample_size(10);
    group_ours.bench_function("trie_project_hole", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&join_trie, ProjectReordering::from_vector(vec![0, 3], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_project_beginning", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&join_trie, ProjectReordering::from_vector(vec![0, 1], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_project_end", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&join_trie, ProjectReordering::from_vector(vec![2, 3], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });

    group_ours.bench_function("trie_reorder_1", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&trie_b, ProjectReordering::from_vector(vec![0, 2, 1], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_reorder_2", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&trie_b, ProjectReordering::from_vector(vec![1, 0, 2], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });
    group_ours.bench_function("trie_reorder_3", |b| {
        b.iter_with_setup(
            || TrieScanProject::new(&trie_b, ProjectReordering::from_vector(vec![2, 1, 0], 4)),
            |project_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
            },
        );
    });
    group_ours.finish();
}

fn benchmark_union(c: &mut Criterion) {
    const FILE_NAME: &str = "test-files/bench-data/aux-split/aux";
    const NUM_PARTS: usize = 10;

    let mut dict = RefCell::new(PrefixedStringDictionary::default());

    let mut tries = Vec::<Trie>::new();
    let mut frames = Vec::<DataFrame>::new();
    for trie_index in 0..NUM_PARTS {
        let mut filename = FILE_NAME.to_string();
        filename += "-";
        filename += &trie_index.to_string();
        filename += ".csv";

        let table_source = DataSource::csv_file(&filename).unwrap();
        let table_arity = 3;

        tries.push(load_trie(&table_source, table_arity, &mut dict));

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
                TrieScanUnion::new(
                    tries
                        .iter()
                        .map(|trie| TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(trie)))
                        .collect(),
                )
            },
            |union_iter| {
                let _ = materialize(&mut TrieScanEnum::TrieScanUnion(union_iter));
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
