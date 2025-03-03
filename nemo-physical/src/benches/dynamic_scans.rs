//! This module collects benchmarks for assessing the impact of using dynamic dispatch in columns.

#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};

    use crate::{
        benches::test::{black_box, Bencher},
        columnar::{
            column::vector::{ColumnScanVector, ColumnVector},
            columnscan::{ColumnScanEnum, ColumnScanT},
            operations::constant::ColumnScanConstant,
        },
        storagevalues::{
            storagetype::{StorageType, NUM_STORAGE_TYPES},
            storagevalue::StorageValueT,
        },
    };

    fn vector() -> Vec<u64> {
        let length: usize = 10_000_000;

        let mut rng = thread_rng();
        let mut numbers: Vec<u64> = (0..length).map(|_| rng.gen()).collect();

        numbers.sort();
        numbers.dedup();
        numbers
    }

    #[ignore]
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

    #[ignore]
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

    #[ignore]
    #[bench]
    fn scan_next_rainbow(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let mut scan = black_box(ColumnScanT::new(
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Vector(ColumnScanVector::new(&column)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
            ));

            let mut sum: u64 = 0;
            let storage_types = black_box(vec![StorageType::Id64, StorageType::Int64]);
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

    struct PossibleTypes {
        storage_types: [Option<StorageType>; NUM_STORAGE_TYPES],
        current_type: usize,
    }

    impl PossibleTypes {
        fn new(input_types: &[StorageType]) -> Self {
            let mut storage_types = [None; NUM_STORAGE_TYPES];

            for (index, storage_type) in input_types.iter().enumerate() {
                storage_types[index] = Some(*storage_type);
            }

            Self {
                storage_types,
                current_type: 0,
            }
        }

        fn current_type(&self) -> StorageType {
            self.storage_types[self.current_type].unwrap()
        }

        fn inc_type(&mut self) {
            self.current_type += 1;
        }
    }

    #[ignore]
    #[bench]
    fn scan_next_rainbow_comp(bencher: &mut Bencher) {
        let column = ColumnVector::new(vector());

        bencher.iter(|| {
            let mut scan = black_box(ColumnScanT::new(
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Vector(ColumnScanVector::new(&column)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
                ColumnScanEnum::Constant(ColumnScanConstant::new(None)),
            ));

            let mut sum: u64 = 0;
            let mut possible_types = black_box(vec![black_box(PossibleTypes::new(&[
                StorageType::Id32,
                StorageType::Id64,
            ]))]);
            possible_types[0].inc_type();

            while let Some(value) = scan.next(possible_types[black_box(0)].current_type()) {
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
