use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use rand_pcg::Pcg64;
use stage2::physical::columns::{generic_column_scan, ColumnScan, GenericColumnScan, VectorColumn};

pub fn benchmark_seek(c: &mut Criterion) {
    let mut rng = Pcg64::seed_from_u64(2);
    let mut data: Vec<usize> = Vec::new();
    let seek: Vec<usize>;
    for i in 0..1001 {
        data.push(rng.gen::<usize>());
    }
    data.sort_unstable();
    seek = vec![data[300] + 1, data[400] + 12, data[900], data[1000] + 2];
    let test_column = VectorColumn::new(data);
    let mut gcs = GenericColumnScan::new(&test_column);

    let mut group = c.benchmark_group("seek");
    group.sample_size(20);
    group.bench_function("seek_dummy", |b| {
        b.iter(|| {
            gcs.seek(seek[1]);
            //gcs.seek(seek[1]);
            //gcs.seek(seek[2]);
            //gcs.seek(seek[3]);
        })
    });
}

criterion_group!(benches, benchmark_seek);
criterion_main!(benches);
