use crate::physical::columns::{GenericIntervalColumn, IntervalColumnT, VectorColumn};
use crate::physical::datatypes::DataTypeName;
use crate::physical::tables::{Trie, TrieSchema, TrieSchemaEntry};
use arbitrary::{Arbitrary, Result, Unstructured};
use num::{One, Zero};
use std::cmp::Eq;
use std::fmt::Debug;
use std::ops::{Add, Sub};

/// Constructs GenericIntervalColumn of U64 type from Slice
pub fn make_gic<T>(values: &[T], ints: &[usize]) -> GenericIntervalColumn<T>
where
    T: Copy + Ord + Debug + 'static,
{
    GenericIntervalColumn::new(
        Box::new(VectorColumn::new(values.to_vec())),
        Box::new(VectorColumn::new(ints.to_vec())),
    )
}

/// Constructs IntervalColumnT (of U64 type) from Slice
pub fn make_gict(values: &[u64], ints: &[usize]) -> IntervalColumnT {
    IntervalColumnT::IntervalColumnU64(Box::new(make_gic(values, ints)))
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

/// Hepler function which creates an arbitrary GenericIntervalColumn
/// given a number of sections and a maximum for the number of enries per section
fn arbitrary_gic<'a, T>(
    u: &mut Unstructured<'a>,
    sections: usize,
    avg_per_section: usize,
) -> Result<GenericIntervalColumn<T>>
where
    T: Arbitrary<'a> + Debug + Copy + Ord + Add<T, Output = T> + One + Zero + 'static,
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

    Ok(make_gic(&values, &intervals))
}

impl<'a, T> Arbitrary<'a> for GenericIntervalColumn<T>
where
    T: Arbitrary<'a> + Debug + Copy + Ord + Add<T, Output = T> + One + Zero + 'static,
{
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        const NUMBER_OF_SECTIONS: usize = 8;
        const AVG_PER_SECTION: usize = 4;
        arbitrary_gic(u, NUMBER_OF_SECTIONS, AVG_PER_SECTION)
    }
}

impl<'a> Arbitrary<'a> for Trie {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        const MAX_DEPTH: usize = 4;
        const AVG_BRANCHING: usize = 2;
        const INITIAL_SECTIONS: usize = 4;

        let depth = usize::arbitrary(u)? % MAX_DEPTH;
        let mut entries = Vec::<TrieSchemaEntry>::new();
        for depth_index in 0..depth {
            entries.push(TrieSchemaEntry {
                label: depth_index,
                datatype: DataTypeName::U64,
            });
        }

        let schema = TrieSchema::new(entries);

        let mut columns = Vec::<IntervalColumnT>::new();
        for depth_index in 0..depth {
            let section_count = if depth_index == 0 {
                INITIAL_SECTIONS
            } else {
                columns[depth_index - 1].len()
            };

            columns.push(IntervalColumnT::IntervalColumnU64(Box::new(arbitrary_gic(
                u,
                section_count,
                AVG_BRANCHING,
            )?)));
        }

        Ok(Trie::new(schema, columns))
    }
}
