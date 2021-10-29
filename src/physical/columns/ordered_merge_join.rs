use super::ColumnScan;
use std::fmt::Debug;

/// Implementation of [`ColumnScan`] for the result of joining a list of [`ColumnScan`] structs.
#[derive(Debug)]
pub struct OrderedMergeJoin<T> {
    column_scans: Vec<Box<dyn ColumnScan<T>>>,
    active_scan: usize,
    active_max: Option<T>,
    pos: Option<usize>,
    current: Option<T>,
}

impl<T> OrderedMergeJoin<T> {
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(column_scans: Vec<Box<dyn ColumnScan<T>>>) -> OrderedMergeJoin<T> {
        OrderedMergeJoin {
            column_scans,
            active_scan: 0,
            active_max: None,
            pos: None,
            current: None,
        }
    }
}

impl<T: Eq + Debug + Copy> Iterator for OrderedMergeJoin<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.active_max = self.column_scans[self.active_scan].next();
        if self.active_max.is_none() {
            return None;
        }
        let mut matched_scans: usize = 1;

        loop {
            self.active_scan = (self.active_scan + 1) % self.column_scans.len();
            if self.active_max == self.column_scans[self.active_scan].seek(self.active_max.unwrap())
            {
                matched_scans += 1;
                if matched_scans == self.column_scans.len() {
                    self.current = self.active_max;
                    let pos = self.pos.map_or_else(Default::default, |pos| pos + 1);
                    self.pos = Some(pos);
                    return self.current;
                }
            } else {
                self.active_max = self.column_scans[self.active_scan].current();
                matched_scans = 1;
                if self.active_max.is_none() {
                    self.current = None;
                    return None;
                }
            }
        }
    }
}

impl<T: Ord + Copy + Debug> ColumnScan<T> for OrderedMergeJoin<T> {
    /// TODO: Seek could be more efficient by using seeks of underlying iterators.
    /// However, we will then not know the position any more (number of matches in between).
    /// The ability to get a position should not be part of all iterators we consider.
    fn seek(&mut self, value: T) -> Option<T> {
        // Brute-force scan:
        self.pos.is_none().then(|| self.next());

        loop {
            if self.current.is_none() {
                return None;
            } else if self.current.unwrap() >= value {
                return self.current;
            }

            self.next();
            let pos = self.pos.get_or_insert(0);
            *pos += 1;
        }
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn pos(&mut self) -> Option<usize> {
        self.pos
    }
}

#[cfg(test)]
mod test {
    use super::super::{GenericColumnScan, VectorColumn};
    use super::{ColumnScan, OrderedMergeJoin}; // < TODO: is this a nice way to write this use?

    #[test]
    fn test_u64_simple_join() {
        let data1: Vec<u64> = vec![1, 3, 5, 7, 9];
        let gcs1: GenericColumnScan<u64> =
            GenericColumnScan::new(Box::new(VectorColumn::new(data1)));
        let data2: Vec<u64> = vec![1, 5, 6, 7];
        let gcs2: GenericColumnScan<u64> =
            GenericColumnScan::new(Box::new(VectorColumn::new(data2)));
        let data3: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let gcs3: GenericColumnScan<u64> =
            GenericColumnScan::new(Box::new(VectorColumn::new(data3)));

        let mut omj: OrderedMergeJoin<u64> =
            OrderedMergeJoin::new(vec![Box::new(gcs1), Box::new(gcs2), Box::new(gcs3)]);

        assert_eq!(omj.next(), Some(1));
        assert_eq!(omj.current(), Some(1));
        assert_eq!(omj.next(), Some(5));
        assert_eq!(omj.current(), Some(5));
        assert_eq!(omj.next(), Some(7));
        assert_eq!(omj.current(), Some(7));
        assert_eq!(omj.next(), None);
        assert_eq!(omj.current(), None);
        assert_eq!(omj.next(), None);
    }
}
