use super::{
    bitvector::IntervalLookupBitVector, one_column::IntervalLookupOneColumn,
    two_columns::IntervalLookupTwoColumns,
};

#[derive(Debug)]
pub enum IntervalLookupEnum {
    BitVector(IntervalLookupBitVector),
    OneColumn(IntervalLookupOneColumn),
    TwoColumns(IntervalLookupTwoColumns),
}
