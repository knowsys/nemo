use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::{fmt::Debug, marker::PhantomData, ops::Range};

/// [`ColumnScan`] which is takes the values of its sub scan but casts them into a different type.
#[derive(Debug)]
pub struct ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + TryFrom<FromType> + TryInto<FromType>,
{
    /// Necessary to bind the ToType.
    _phantom: PhantomData<ToType>,

    /// Scan from whom the values will be converted.
    reference_scan: Box<ColumnScanCell<'a, FromType>>,
}

impl<'a, FromType, ToType> ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + TryFrom<FromType> + TryInto<FromType>,
{
    /// Constructs a new [`ColumnScanCast`] given a reference scan.
    pub fn new(reference_scan: ColumnScanCell<'a, FromType>) -> Self {
        Self {
            _phantom: PhantomData,
            reference_scan: Box::new(reference_scan),
        }
    }
}

impl<'a, FromType, ToType> Iterator for ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + TryFrom<FromType> + TryInto<FromType>,
{
    type Item = ToType;

    // TODO: we should be able to distinguish whether there was no next value or if the cast failed
    // one could change Item to be Result<ToType, ...> but then e.g. seek also expects Result<ToType, ...> as an argument, which is not so nice...
    fn next(&mut self) -> Option<ToType> {
        self.reference_scan
            .next()
            .and_then(|val| ToType::try_from(val).ok())
    }
}

impl<'a, FromType, ToType> ColumnScan for ColumnScanCast<'a, FromType, ToType>
where
    FromType: 'a + ColumnDataType,
    ToType: 'a + ColumnDataType + TryFrom<FromType> + TryInto<FromType>,
{
    fn seek(&mut self, value: ToType) -> Option<ToType> {
        // TODO: The code below is not correct for all types
        // Casting from ToType to FromType should not simply return None if it fails
        // as this would indicate that there is no larger value.
        // This is a problem for signed datatypes.
        self.reference_scan
            .seek(value.try_into().ok()?)
            .and_then(|val| ToType::try_from(val).ok())
    }

    fn current(&mut self) -> Option<ToType> {
        self.reference_scan
            .current()
            .and_then(|val| ToType::try_from(val).ok())
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

/// A specialised version of [`generate_forwarder`] for the possible
/// variants of castable types of [`crate::physical::datatypes::data_value::DataValueT`].
#[macro_export]
macro_rules! generate_castable_forwarder {
    ($name:ident) => {
        $crate::generate_forwarder!($name;
                                    U32);
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
    U32(ColumnScanCast<'a, u32, ToType>),
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

    fn current(&mut self) -> Option<Self::Item> {
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
    use crate::physical::columnar::{
        column_types::vector::ColumnVector,
        traits::{
            column::Column,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
    };

    use super::ColumnScanCast;
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = ColumnVector::new(vec![0u32, 4, 7]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut pass_scan = ColumnScanCast::<u32, u64>::new(ref_col_iter);

        assert_eq!(pass_scan.current(), None);
        assert_eq!(pass_scan.next(), Some(0u64));
        assert_eq!(pass_scan.current(), Some(0u64));
        assert_eq!(pass_scan.seek(6), Some(7u64));
        assert_eq!(pass_scan.current(), Some(7u64));
        assert_eq!(pass_scan.next(), None);
        assert_eq!(pass_scan.current(), None);
    }
}
