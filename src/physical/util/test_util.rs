use crate::physical::columns::{GenericIntervalColumn, IntervalColumnT, VectorColumn};

/// Constructs GenericIntervalColumn of U64 type from Slice
pub fn make_gic(values: &[u64], ints: &[usize]) -> GenericIntervalColumn<u64> {
    GenericIntervalColumn::new(
        Box::new(VectorColumn::new(values.to_vec())),
        Box::new(VectorColumn::new(ints.to_vec())),
    )
}

/// Constructs IntervalColumnT (of U64 type) from Slice
pub fn make_gict(values: &[u64], ints: &[usize]) -> IntervalColumnT {
    IntervalColumnT::IntervalColumnU64(Box::new(make_gic(values, ints)))
}
