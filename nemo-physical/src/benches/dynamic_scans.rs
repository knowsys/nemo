//! This module collects benchmarks for assessing the impact of using dynamic dispatch in columns.

#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};

    use crate::{
        benches::test::{black_box, Bencher},
        columnar::{
            column::vector::{ColumnScanVector, ColumnVector},
            columnscan::{ColumnScanEnum, ColumnScanRainbow},
            operations::constant::ColumnScanConstant,
        },
        datatypes::{StorageTypeName, StorageValueT},
    };

    fn vector() -> Vec<u64> {
        let length: usize = 10_000_000;

        let mut rng = thread_rng();
        let mut numbers: Vec<u64> = (0..length).map(|_| rng.gen()).collect();

        numbers.sort();
        numbers.dedup();
        numbers
    }

    #[bench]
    fn scan_next_regular(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let scan = black_box(ColumnScanVector::new(&column));
            let mut sum: u64 = 0;

            for value in scan {
                sum = sum.wrapping_add(value);
            }

            println!("{sum}");
        })
    }

    #[bench]
    fn scan_next_enum(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let scan = black_box(ColumnScanEnum::Vector(ColumnScanVector::new(&column)));
            let mut sum: u64 = 0;

            for value in scan {
                sum = sum.wrapping_add(value);
            }

            println!("{sum}");
        })
    }

    #[bench]
    fn scan_next_rainbow(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let mut scan = black_box(ColumnScanRainbow::new(
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Vector(ColumnScanVector::new(&column)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
            ));

            let mut sum: u64 = 0;
            let storage_types = black_box(vec![StorageTypeName::Id64, StorageTypeName::Int64]);
            let current_type = black_box(vec![0]);

            while let Some(value) = scan.next(black_box(storage_types[current_type[0]])) {
                sum = sum.wrapping_add(if let StorageValueT::Id64(value) = value {
                    value
                } else {
                    unreachable!();
                });
            }

            println!("{sum}");
        })
    }
}
