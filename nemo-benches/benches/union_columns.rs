use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Standard, prelude::*};
use rand_pcg::{Lcg128Xsl64, Pcg64};

use nemo_physical::{
    columnar::{
        adaptive_column_builder::ColumnBuilderAdaptive,
        column_types::{rle::ColumnRle, vector::ColumnVector},
        traits::{
            column::{Column, ColumnEnum},
            columnbuilder::ColumnBuilder,
            columnscan::ColumnScan,
        },
    },
    datatypes::{ColumnDataType, Float},
};

fn create_random_column_enum<T>(rng: &mut Lcg128Xsl64, length: usize) -> ColumnEnum<T>
where
    T: ColumnDataType + Default + Distribution<T>,
    Standard: rand::distributions::Distribution<T>,
{
    let mut column_vec = Vec::<T>::new();
    for _ in 0..length {
        column_vec.push(rng.gen::<T>());
    }

    column_vec.dedup();
    column_vec.sort();

    let mut builder = ColumnBuilderAdaptive::<T>::default();

    for value in column_vec {
        builder.add(value);
    }

    builder.finalize()
}

fn create_random_successor<T>(
    rng: &mut Lcg128Xsl64,
    last_length: usize,
    average_branching: f32,
    percentage_none: f32,
) -> (ColumnEnum<T>, u32)
where
    T: ColumnDataType + Default + Distribution<T>,
    Standard: rand::distributions::Distribution<T>,
{
    todo!()
}

pub fn benchmark_seek(c: &mut Criterion) {
    let mut rng = Pcg64::seed_from_u64(21564);

    let mut data: Vec<f32> = Vec::new();
    let _seek: Vec<usize>;
    for _i in 0..10000001 {
        data.push(rng.gen::<f32>());
    }

    // data.sort_unstable();
    // let randa = data[rng.gen_range(0..10000000)];

    // let test_column = ColumnVector::new(data.clone());
    // let rle_test_column = ColumnRle::new(data.clone());

    // let mut group = c.benchmark_group("seek");
    // group.sample_size(200);
    // group.bench_function("seek_generic_column_scan", |b| {
    //     b.iter_with_setup(
    //         || test_column.iter(),
    //         |mut gcs| {
    //             gcs.seek(randa);
    //         },
    //     )
    // });
    // group.finish();

    // let mut group_rle = c.benchmark_group("seek_rle");
    // group_rle.bench_function("seek_rle_randomized", |b| {
    //     b.iter_with_setup(
    //         || rle_test_column.iter(),
    //         |mut rcs| {
    //             rcs.seek(randa);
    //         },
    //     )
    // });

    // let vec_col_handcrafted = ColumnVector::new(
    //     (1u32..100000)
    //         .chain(200000..400000)
    //         .chain(600000..800000)
    //         .collect(),
    // );
    // let rle_col_handcrafted = ColumnRle::new(
    //     (1u32..100000)
    //         .chain(200000..400000)
    //         .chain(600000..800000)
    //         .collect(),
    // );

    // group_rle.bench_function("seek_vec_handcrafted", |b| {
    //     b.iter_with_setup(
    //         || vec_col_handcrafted.iter(),
    //         |mut rcs| {
    //             rcs.seek(black_box(650000));
    //         },
    //     )
    // });

    // group_rle.bench_function("seek_rle_handcrafted", |b| {
    //     b.iter_with_setup(
    //         || rle_col_handcrafted.iter(),
    //         |mut rcs| {
    //             rcs.seek(black_box(650000));
    //         },
    //     )
    // });
    // group_rle.finish();
}

criterion_group!(benches, benchmark_seek);
criterion_main!(benches);
