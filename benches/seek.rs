use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use rand_pcg::Pcg64;
use stage2::physical::columns::{generic_column_scan, ColumnScan, GenericColumnScan, VectorColumn};

pub fn benchmark_seek(c: &mut Criterion) {
    let mut rng = Pcg64::seed_from_u64(21564);
    let mut data: Vec<usize> = Vec::new();
    let seek: Vec<usize>;
    for i in 0..10000001 {
        data.push(rng.gen::<usize>());
    }
    let values = (
        data[rng.gen_range(0..10000000)],
        data[rng.gen_range(0..10000000)],
    );
    let (randa, randb) = if values.0 < values.1 {
        values
    } else {
        (values.1, values.0)
    };
    let test_column = VectorColumn::new(data);

    let mut group = c.benchmark_group("seek");
    group.sample_size(200);
    group.bench_function("seek_dummy", |b| {
        b.iter_with_setup(
            || {
                let mut gc = GenericColumnScan::new(&test_column);
                gc.seek(randa);
                gc
            },
            |mut gcs| {
                gcs.seek(randb);
            },
        )
    });
    group.bench_function("seek_dummy2", |b| {
        b.iter_with_setup(
            || {
                let mut gc = GenericColumnScan::new(&test_column);
                gc.seek(randa);
                gc
            },
            |mut gcs| {
                gcs.seek(randb);
            },
        )
    });
    group.finish();
}

criterion_group!(benches, benchmark_seek);
criterion_main!(benches);
