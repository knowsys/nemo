use super::{FTableSchema, Table, TableSchema};
use crate::physical::columns::IntervalColumnT;
use std::fmt::Debug;

/// Implementation of a factorized trie, which might be a subtrie of a larger strcuture.
#[derive(Debug)]
pub struct Ftrie {
    schema: FTableSchema,
    columns: Vec<IntervalColumnT>,
}

impl Ftrie {
    /// Constructs a new fTrie without parent or children.
    pub fn new(schema: FTableSchema, columns: Vec<IntervalColumnT>) -> Ftrie {
        Ftrie { schema, columns }
    }

    /// Counts the total number of rows in a given interval of a certain column,
    /// as it would appear if we would serialise all child nodes recursively into
    /// a flat table. The column index ['usize::MAX'] can be used for starting with
    /// root columns.
    ///
    /// # Panics
    /// Panics if `col_idx` or `int_idx` are out of bounds.
    fn count_rows(&self, col_idx: usize, int_idx: usize) -> usize {
        let mut children: Vec<usize> = Vec::new();
        for idx in 0..self.schema.arity() {
            if self.schema.get_parent(idx).unwrap_or(usize::MAX) == col_idx {
                children.push(idx);
            }
        }

        let bounds = if col_idx == usize::MAX {
            (0, 0)
        } else {
            self.columns[col_idx].int_bounds(int_idx)
        };

        if children.is_empty() {
            return bounds.1 - bounds.0 + 1;
        }

        let mut sum = 0;
        for i in bounds.0..=bounds.1 {
            let mut product = 1;
            for cidx in children.iter() {
                product *= self.count_rows(*cidx, i);
            }
            sum += product;
        }
        sum
    }

    // TODO: more funcionality needed
}

impl Table for Ftrie {
    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize {
        self.count_rows(usize::MAX, 0)
    }

    /// Returns the schema of the table.
    fn schema(&self) -> &dyn TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::{FTableSchema, Table};
    use super::Ftrie;
    use crate::physical::columns::{GenericIntervalColumn, IntervalColumnT, VectorColumn};
    use crate::physical::datatypes::DataTypeName;
    use test_env_log::test;

    fn make_gic(values: &[u64], ints: &[usize]) -> GenericIntervalColumn<u64> {
        GenericIntervalColumn::new(
            Box::new(VectorColumn::new(values.to_vec())),
            Box::new(VectorColumn::new(ints.to_vec())),
        )
    }

    fn make_gict(values: &[u64], ints: &[usize]) -> IntervalColumnT {
        IntervalColumnT::IntervalColumnU64(Box::new(make_gic(values, ints)))
    }

    #[test]
    fn test_count_rows_linear() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, DataTypeName::U64, 0);
        fts.add_entry(11, DataTypeName::U64, 1);
        fts.add_entry(111, DataTypeName::U64, 11);

        let gic1 = make_gict(&[1, 2, 3], &[0]);
        let gic11 = make_gict(&[11, 21, 31], &[0, 1, 2]);
        let gic111 = make_gict(&[11, 21, 31], &[0, 1, 2]);

        let columns = vec![gic1, gic11, gic111];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 3);
    }

    #[test]
    fn test_count_rows() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, DataTypeName::U64, 0);
        fts.add_entry(11, DataTypeName::U64, 1);
        fts.add_entry(12, DataTypeName::U64, 1);
        fts.add_entry(121, DataTypeName::U64, 12);

        let gic1 = make_gict(&[1, 2, 3], &[0]);
        let gic11 = make_gict(&[11, 21, 22, 31], &[0, 1, 3]);
        let gic12 = make_gict(&[11, 21, 31], &[0, 1, 2]);
        let gic121 = make_gict(&[11, 12, 21, 22, 23, 31], &[0, 2, 5]);

        let columns = vec![gic1, gic11, gic12, gic121];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 9);
    }

    #[test]
    fn test_count_rows_cartesian() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, DataTypeName::U64, 0);
        fts.add_entry(2, DataTypeName::U64, 0);
        fts.add_entry(11, DataTypeName::U64, 1);
        fts.add_entry(21, DataTypeName::U64, 2);

        let gic1 = make_gict(&[1, 2, 3], &[0]);
        let gic2 = make_gict(&[1, 2], &[0]);
        let gic11 = make_gict(&[11, 21, 31], &[0, 1, 2]);
        let gic21 = make_gict(&[11, 12, 21, 22], &[0, 2]);

        let columns = vec![gic1, gic2, gic11, gic21];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 12);
    }
}
