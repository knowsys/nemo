//! This module collects benchmarks for seeking in a column.

#[cfg(test)]
mod test {
    use rand::{Rng, SeedableRng};
    use rand_pcg::Pcg64;

    use crate::{
        benches::test::{Bencher, black_box},
        columnar::{
            column::{Column, rle::ColumnRle, vector::ColumnVector},
            columnscan::ColumnScan,
        },
    };

    fn random_data() -> (Vec<usize>, usize) {
        let length: usize = 10_000_000;

        let mut rng = Pcg64::seed_from_u64(21564);
        let mut data: Vec<usize> = Vec::new();

        for _ in 0..length {
            data.push(rng.r#gen::<usize>());
        }
        data.sort_unstable();

        let seek = data[rng.gen_range(0..length)];

        (data, seek)
    }

    #[ignore]
    #[bench]
    fn seek_vector_random(bencher: &mut Bencher) {
        let (data, seek) = random_data();
        let column = ColumnVector::new(data);

        bencher.iter(|| {
            let mut scan = black_box(column.iter());
            let _value = black_box(scan.seek(black_box(seek)));
        })
    }

    /// TODO: This performs quite a lot worse than the others
    #[ignore]
    #[bench]
    fn seek_rle_random(bencher: &mut Bencher) {
        let (data, seek) = random_data();
        let column = ColumnRle::new(data);

        bencher.iter(|| {
            let mut scan = black_box(column.iter());
            let _value = black_box(scan.seek(black_box(seek)));
        })
    }

    #[ignore]
    #[bench]
    fn seek_vector_crafted(bencher: &mut Bencher) {
        let column = ColumnVector::new(
            (1u32..100000)
                .chain(200000..400000)
                .chain(600000..800000)
                .collect(),
        );
        let seek = 650000;

        bencher.iter(|| {
            let mut scan = black_box(column.iter());
            let _value = black_box(scan.seek(black_box(seek)));
        })
    }

    #[ignore]
    #[bench]
    fn seek_rle_crafted(bencher: &mut Bencher) {
        let column = ColumnRle::new(
            (1u32..100000)
                .chain(200000..400000)
                .chain(600000..800000)
                .collect(),
        );
        let seek = 650000;

        bencher.iter(|| {
            let mut scan = black_box(column.iter());
            let _value = black_box(scan.seek(black_box(seek)));
        })
    }
}
