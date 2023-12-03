use crate::columnar::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::{
    casting::{ImplicitCastError, ImplicitCastFrom, ImplicitCastInto},
    ColumnDataType,
};
use std::{fmt::Debug, marker::PhantomData, ops::Range};

/// [`ColumnScan`] which is takes the values of its sub scan and casts them into a different type.
///
/// This is intended to be used only for types that use the same number representation format
/// but where the range of one is smaller than the range of the other. E.g. u32 and u64.
/// As such, the only possible errors that could occur while casting are either overflow or underflow.
///
/// Values of the referenced sub scan that are not representable in the target type (either because they are
/// to small or to large) will be ignored and no error is emitted.
/// This behavior is useful when implementing data base operations
/// between columns of different (but compatible in the above sense) types.
/// E.g. performing a join of u32 columns with u64 columns.
/// Since no non-u32 value from the u64 column can appear in u32 column they can be ignored.
///
/// When casting from a smaller to a larger type, however, it is guaranteed that no value will be skipped.
#[derive(Debug)]
pub struct ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType,
{
    /// Necessary to bind the ToType.
    _phantom: PhantomData<ToType>,

    /// Scan from whom the values will be converted.
    reference_scan: Box<ColumnScanCell<'a, FromType>>,
}

impl<'a, FromType, ToType> ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + ImplicitCastFrom<FromType> + ImplicitCastInto<FromType>,
{
    /// Constructs a new [`ColumnScanCast`] given a reference scan.
    pub fn new(reference_scan: ColumnScanCell<'a, FromType>) -> Self {
        Self {
            _phantom: PhantomData,
            reference_scan: Box::new(reference_scan),
        }
    }

    /// Returns the appropriate result for `next` or `seek` in case the casting fails.
    fn handle_error(&mut self, error: ImplicitCastError) -> Option<ToType> {
        match error {
            ImplicitCastError::Overflow => {
                // In this case the next value is larger then the largest value representable in the target type
                // Hence, we move the reference scan to the end and return None

                self.reference_scan.seek(FromType::max_value());
                while self.reference_scan.next().is_some() {}

                None
            }
            ImplicitCastError::Underflow => {
                // In this case the next value is smaller then the smallest value representable in the target type
                // Hence, we move the reference scan to the first value that is representable in the target type

                self.seek(ToType::min_value())
            }
            ImplicitCastError::NonCastable => {
                panic!("Trying to implicitly cast incompatible values.")
            }
        }
    }
}

impl<'a, FromType, ToType> Iterator for ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + ImplicitCastFrom<FromType> + ImplicitCastInto<FromType>,
{
    type Item = ToType;

    fn next(&mut self) -> Option<ToType> {
        let next_value = self.reference_scan.next()?;

        match ToType::cast_from(next_value) {
            Ok(value) => Some(value),
            Err(error) => self.handle_error(error),
        }
    }
}

impl<'a, FromType, ToType> ColumnScan for ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + ImplicitCastFrom<FromType> + ImplicitCastInto<FromType>,
{
    fn seek(&mut self, value: ToType) -> Option<ToType> {
        match value.cast_into() {
            Ok(casted_value) => match ToType::cast_from(self.reference_scan.seek(casted_value)?) {
                Ok(v) => Some(v),
                Err(error) => self.handle_error(error),
            },
            Err(error) => match error {
                ImplicitCastError::Overflow => {
                    // In this case the target value is larger then the largest value reresentable in the target type
                    // Hence, we move the reference scan to the end and return None

                    self.reference_scan.seek(FromType::max_value());
                    while self.reference_scan.next().is_some() {}

                    None
                }
                ImplicitCastError::Underflow => {
                    // In this case the target value is smaller then the smallest value reresentable in the target type
                    // Hence, we move the reference scan to the first position

                    self.reference_scan.reset();
                    self.next()
                }
                ImplicitCastError::NonCastable => {
                    panic!("Trying to implicitlly cast incompatble values.")
                }
            },
        }
    }

    fn current(&self) -> Option<ToType> {
        match ToType::cast_from(self.reference_scan.current()?) {
            Ok(value) => Some(value),
            Err(_) => panic!("The way seek and next are implemented should prevent a situation where reference scan points to value outside the range of ToType."),
        }
    }

    fn reset(&mut self) {
        self.reference_scan.reset()
    }

    fn pos(&self) -> Option<usize> {
        self.reference_scan.pos()
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.reference_scan.narrow(interval);
    }
}

/// A specialised version of the `generate_forwarder` macro for the possible
/// variants of castable types of [`crate::datatypes::storage_value::StorageValueT`].
#[macro_export]
macro_rules! generate_castable_forwarder {
    ($name:ident) => {
        $crate::generate_forwarder!($name; Id32, Id64, Int64);
    }
}

/// Enum which contains one variant of [`ColumnScanCast`]
/// for each supported conversion.
#[derive(Debug)]
pub enum ColumnScanCastEnum<'a, ToType>
where
    ToType: 'a + ColumnDataType,
{
    /// Cast from u32 to ToType
    Id32(ColumnScanCast<'a, u32, ToType>),
    /// Cast from u64 to ToType
    Id64(ColumnScanCast<'a, u64, ToType>),
    /// Cast from i64 to ToType
    Int64(ColumnScanCast<'a, i64, ToType>),
}

// Generate a macro forward_to_columnscan_cell!, which takes a [`ColumnScanT`] and a function as arguments
// and unfolds into a `match` statement that calls the datatype specific variant that function.
// See `physical/util.rs` for a more detailed description of this macro.
generate_castable_forwarder!(forward_to_column_scan_cast);

impl<'a, T> Iterator for ColumnScanCastEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_column_scan_cast!(self, next)
    }
}

impl<'a, T> ColumnScan for ColumnScanCastEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        forward_to_column_scan_cast!(self, seek(value))
    }

    fn current(&self) -> Option<Self::Item> {
        forward_to_column_scan_cast!(self, current)
    }

    fn reset(&mut self) {
        forward_to_column_scan_cast!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_column_scan_cast!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_column_scan_cast!(self, narrow(interval))
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanCast;
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = ColumnVector::new(vec![0u32, 4, 7]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut cast_scan = ColumnScanCast::<u32, u64>::new(ref_col_iter);

        assert_eq!(cast_scan.current(), None);
        assert_eq!(cast_scan.next(), Some(0u64));
        assert_eq!(cast_scan.current(), Some(0u64));
        assert_eq!(cast_scan.seek(6), Some(7u64));
        assert_eq!(cast_scan.current(), Some(7u64));
        assert_eq!(cast_scan.next(), None);
        assert_eq!(cast_scan.current(), None);
    }

    #[test]
    fn test_flow_next() {
        let ref_col = ColumnVector::new(vec![-1000i64, -270, -100, 0, 5, 100, 1000, 1200]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut cast_scan = ColumnScanCast::<i64, i8>::new(ref_col_iter);

        assert_eq!(cast_scan.current(), None);
        assert_eq!(cast_scan.next(), Some(-100));
        assert_eq!(cast_scan.current(), Some(-100));
        assert_eq!(cast_scan.next(), Some(0));
        assert_eq!(cast_scan.current(), Some(0));
        assert_eq!(cast_scan.next(), Some(5));
        assert_eq!(cast_scan.current(), Some(5));
        assert_eq!(cast_scan.next(), Some(100));
        assert_eq!(cast_scan.current(), Some(100));
        assert_eq!(cast_scan.next(), None);
        assert_eq!(cast_scan.current(), None);
    }

    #[test]
    fn test_flow_seek_1() {
        let ref_col = ColumnVector::new(vec![-1000i64, -270, -100, 0, 5, 100, 1000, 1200]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut cast_scan = ColumnScanCast::<i64, i8>::new(ref_col_iter);

        assert_eq!(cast_scan.current(), None);
        assert_eq!(cast_scan.seek(-110), Some(-100));
        assert_eq!(cast_scan.current(), Some(-100));
        assert_eq!(cast_scan.seek(100), Some(100));
        assert_eq!(cast_scan.current(), Some(100));
        assert_eq!(cast_scan.seek(120), None);
        assert_eq!(cast_scan.current(), None);
    }

    #[test]
    fn test_flow_seek_2() {
        let ref_col = ColumnVector::new(vec![-100, 0, 5, 100]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut cast_scan = ColumnScanCast::<i32, i64>::new(ref_col_iter);

        assert_eq!(cast_scan.current(), None);
        assert_eq!(cast_scan.seek(-1000), Some(-100));
        assert_eq!(cast_scan.current(), Some(-100));
        assert_eq!(cast_scan.seek(3), Some(5));
        assert_eq!(cast_scan.current(), Some(5));
        assert_eq!(cast_scan.seek(300), None);
        assert_eq!(cast_scan.current(), None);
    }
}
