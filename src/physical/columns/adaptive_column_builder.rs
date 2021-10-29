use super::Column;
use super::ColumnBuilder;
use super::VectorColumn;
use std::fmt::Debug;

/// Implementation of [`ColumnBuilder`] that may adaptively decide for the
/// best possible column implementation for the given data.
///
/// TODO Currently just supports one type of column.
#[derive(Debug, Default)]
pub struct AdaptiveColumnBuilder<T> {
    data: Vec<T>,
}

impl<T> AdaptiveColumnBuilder<T> {
    /// Constructor.
    pub fn new() -> AdaptiveColumnBuilder<T> {
        AdaptiveColumnBuilder { data: Vec::new() }
    }
}

impl<T: Debug + Copy> ColumnBuilder<T> for AdaptiveColumnBuilder<T> {
    fn add(&mut self, value: T) {
        self.data.push(value);
    }

    fn finalize<'a>(self) -> Box<dyn Column<T> + 'a>
    where
        T: 'a,
    {
        Box::new(VectorColumn::new(self.data))
    }
}

#[cfg(test)]
mod test {
    use super::{AdaptiveColumnBuilder, ColumnBuilder};

    #[test]
    fn test_build_u64_column() {
        let mut acb: AdaptiveColumnBuilder<u64> = AdaptiveColumnBuilder::new();
        acb.add(1);
        acb.add(2);
        acb.add(3);

        let vc = acb.finalize();
        assert_eq!(vc.len(), 3);
        assert_eq!(vc.get(0), 1);
        assert_eq!(vc.get(1), 2);
        assert_eq!(vc.get(2), 3);
    }
}
