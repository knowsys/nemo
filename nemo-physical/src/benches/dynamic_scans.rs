#[cfg(test)]
mod tests {
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
            let mut scan = black_box(ColumnScanVector::new(&column));
            let mut sum = 0;
            black_box(while let Some(value) = scan.next() {
                sum += value;
            });
            println!("{sum}");
        })
    }

    #[bench]
    fn scan_next_enum(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let mut scan = black_box(ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(
                &column,
            )));
            let mut sum = 0;
            black_box(while let Some(value) = scan.next() {
                sum += value;
            });
            println!("{sum}");
        })
    }

    #[bench]
    fn scan_next_rainbow(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let mut scan = black_box(ColumnScanRainbow::new(
                ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None)),
                ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&column)),
                ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None)),
                ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None)),
                ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None)),
            ));

            let mut sum = 0;
            let storage_types = black_box(vec![StorageTypeName::Id64, StorageTypeName::Int64]);
            let current_type = black_box(vec![0]);

            black_box(
                while let Some(value) = scan.next(black_box(storage_types[current_type[0]])) {
                    sum += if let StorageValueT::Id64(value) = value {
                        value
                    } else {
                        unreachable!();
                    }
                },
            );
            println!("{sum}");
        })
    }
}
