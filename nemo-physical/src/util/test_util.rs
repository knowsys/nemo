use crate::columnar::column_storage::column_vector::ColumnVector;
use crate::columnar::column_storage::interval::{ColumnWithIntervals, ColumnWithIntervalsT};
use crate::tabular::table_types::trie::Trie;
use crate::{
    columnar::column_storage::column::{Column, ColumnEnum},
    datatypes::ColumnDataType,
};
use arbitrary::{Arbitrary, Result, Unstructured};
use num::{One, Zero};
use std::cmp::Eq;
use std::ops::{Add, Sub};

/// Constructs ColumnWithIntervals of U64 type from Slice
pub fn make_column_with_intervals<'a, T>(
    values: &'a [T],
    ints: &'a [usize],
) -> ColumnWithIntervals<T>
where
    T: ColumnDataType,
{
    ColumnWithIntervals::new(
        ColumnEnum::ColumnVector(ColumnVector::new(values.to_vec())),
        ColumnEnum::ColumnVector(ColumnVector::new(ints.to_vec())),
    )
}

/// Constructs ColumnWithIntervalsT (of U64 type) from Slice
pub fn make_column_with_intervals_t<'a>(
    values: &'a [u64],
    ints: &'a [usize],
) -> ColumnWithIntervalsT {
    ColumnWithIntervalsT::Id64(make_column_with_intervals(values, ints))
}

/// Constructs ColumnWithIntervalsT (of I64 type) from Slice
pub fn make_column_with_intervals_int_t<'a>(
    values: &'a [i64],
    ints: &'a [usize],
) -> ColumnWithIntervalsT {
    ColumnWithIntervalsT::Int64(make_column_with_intervals(values, ints))
}

/// Helper function which, given a slice of sorted values,
/// returns a slice of sorted distinct values
fn make_distinct<T>(values: &mut [T])
where
    T: Add<T, Output = T> + Sub<T, Output = T> + Eq + One + Zero + Copy,
{
    let mut current_increment = T::zero();

    for index in 0..values.len() {
        if index == 0 {
            continue;
        }

        if values[index - 1] - current_increment == values[index] {
            current_increment = current_increment + T::one();
        }

        values[index] = values[index] + current_increment;
    }
}

/// Hepler function which creates an arbitrary IntervalColumnGeneric
/// given a number of sections and a maximum for the number of enries per section
fn arbitrary_column_with_intervals<'a, T>(
    u: &mut Unstructured<'a>,
    sections: usize,
    avg_per_section: usize,
) -> Result<ColumnWithIntervals<T>>
where
    T: Arbitrary<'a> + ColumnDataType,
{
    let max_index = sections * avg_per_section;

    let mut intervals = Vec::<usize>::new();
    for _ in 0..sections {
        intervals.push(usize::arbitrary(u)? % max_index);
    }

    intervals.sort_unstable();
    make_distinct(&mut intervals);

    let mut values = Vec::<T>::new();
    for section in 0..sections {
        let current_length = if section < sections - 1 {
            intervals[section + 1] - intervals[section]
        } else {
            avg_per_section
        };

        for elem_index in 0..current_length {
            let mut elem_inc = T::arbitrary(u)?;
            if elem_inc == T::zero() {
                elem_inc = T::one();
            }

            let next_elem = if elem_index > 0 {
                values[elem_index - 1] + elem_inc
            } else {
                elem_inc
            };

            values.push(next_elem);
        }
    }

    Ok(make_column_with_intervals(&values, &intervals))
}

impl<'a, T> Arbitrary<'a> for ColumnWithIntervals<T>
where
    T: Arbitrary<'a> + ColumnDataType,
{
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        const NUMBER_OF_SECTIONS: usize = 8;
        const AVG_PER_SECTION: usize = 4;
        arbitrary_column_with_intervals(u, NUMBER_OF_SECTIONS, AVG_PER_SECTION)
    }
}

impl<'a> Arbitrary<'a> for Trie {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        const MAX_DEPTH: usize = 4;
        const AVG_BRANCHING: usize = 2;
        const INITIAL_SECTIONS: usize = 4;

        let depth = usize::arbitrary(u)? % MAX_DEPTH;

        let mut columns = Vec::<ColumnWithIntervalsT>::new();
        for depth_index in 0..depth {
            let section_count = if depth_index == 0 {
                INITIAL_SECTIONS
            } else {
                columns[depth_index - 1].len()
            };

            columns.push(ColumnWithIntervalsT::Id64(arbitrary_column_with_intervals(
                u,
                section_count,
                AVG_BRANCHING,
            )?));
        }

        Ok(Trie::new(columns))
    }
}
