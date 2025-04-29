//! This module defines [ColumnScanFilterHook].

use std::{cell::RefCell, ops::Range, rc::Rc};

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
    datavalues::AnyDataValue,
    management::database::Dict,
    util::hook::{FilterHook, FilterResult},
};

/// [ColumnScan], which filters values of a "value" scan based on a [FilterHook]
#[derive(Debug)]
pub(crate) struct ColumnScanFilterHook<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Current scan whose values are being filtered
    value_scan: &'a ColumnScanCell<'a, T>,
    /// Dictionary used for translating values of `value_scan` into [AnyDataValue]
    dictionary: &'a RefCell<Dict>,

    /// Filter-function that is applied
    hook: FilterHook,

    /// Values referenced by the program
    referenced_values: Rc<RefCell<Vec<AnyDataValue>>>,
    /// String that will be passed to `filter`
    string: String,
    /// The index in `referenced_values` the current value of `value_scan` belongs to
    reference_index: usize,

    /// Current value
    current_value: Option<T>,
}

impl<'a, T> ColumnScanFilterHook<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Create a new [ColumnScanFilterHook].
    pub(crate) fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        hook: FilterHook,
        string: String,
        reference_index: usize,
        referenced_values: Rc<RefCell<Vec<AnyDataValue>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Self {
        Self {
            value_scan,
            hook,
            string,
            reference_index,
            referenced_values,
            dictionary,
            current_value: None,
        }
    }

    /// Check whether a given value passes the filter defined by `self.filter`
    /// and return the result as a boolean.
    fn check_value(&self, value: T) -> FilterResult {
        let datavalue = value
            .into_datavalue(&self.dictionary.borrow())
            .expect("It is assumed that all id already in columns are present in the dictionary.");
        let referenced = &mut self.referenced_values.borrow_mut();
        referenced[self.reference_index] = datavalue;

        self.hook.call(&self.string, &referenced)
    }

    /// Loop through `self.value_scan` until the next value passing
    /// the filter defined in `self.program` is found.
    ///
    /// Note that this will pass over values
    /// for which the result of the filter could not have been computed.
    ///
    /// Returns that value or `None` if there is none.
    fn find_next(&mut self) -> Option<T> {
        while let Some(next) = self.value_scan.next() {
            match self.check_value(next) {
                FilterResult::Accept => return Some(next),
                FilterResult::Reject => continue,
                FilterResult::Abort => return None,
            }
        }

        None
    }
}

impl<'a, T> Iterator for ColumnScanFilterHook<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.current_value = self.find_next();
        self.current_value
    }
}

impl<'a, T> ColumnScan for ColumnScanFilterHook<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let seeked_value = self.value_scan.seek(value)?;

        self.current_value = match self.check_value(seeked_value) {
            FilterResult::Accept => Some(seeked_value),
            FilterResult::Reject => self.find_next(),
            FilterResult::Abort => None,
        };

        self.current_value
    }

    fn current(&self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.current_value = None;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }

    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        columnar::{
            column::{vector::ColumnVector, Column},
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
        datavalues::{AnyDataValue, DataValue},
        management::database::Dict,
        util::hook::{FilterHook, FilterResult},
    };

    use super::ColumnScanFilterHook;

    #[test]
    fn columnscan_filter_hook() {
        let dictionary = RefCell::new(Dict::default());

        let value_column = ColumnVector::new(vec![0i64, 7, 14, 17, 18, 21]);
        let value_scan = ColumnScanCell::new(ColumnScanEnum::Vector(value_column.iter()));

        let reference_values = vec![
            AnyDataValue::new_integer_from_i64(10),
            AnyDataValue::new_plain_string(String::from("placeholder")),
        ];

        let function = FilterHook::from(|_string: &str, values: &[AnyDataValue]| {
            if values[1].to_i64_unchecked() == 17 {
                return FilterResult::Abort;
            }

            if values[1].to_i64_unchecked() % 2 == 0 {
                FilterResult::Accept
            } else {
                FilterResult::Reject
            }
        });

        let mut scan_filter = ColumnScanFilterHook::new(
            &value_scan,
            function,
            String::default(),
            1,
            Rc::new(RefCell::new(reference_values)),
            &dictionary,
        );

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), Some(0));
        assert_eq!(scan_filter.next(), Some(14));
        assert_eq!(scan_filter.next(), None);

        scan_filter.reset();
        value_scan.reset();

        assert_eq!(scan_filter.seek(7), Some(14));
        assert_eq!(scan_filter.seek(12), Some(14));
        assert_eq!(scan_filter.seek(15), None);
    }
}
