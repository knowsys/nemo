//! This module defines [ColumnScanFilter].

use std::{cell::RefCell, ops::Range, rc::Rc};

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
    datavalues::AnyDataValue,
    function::evaluation::StackProgram,
    management::database::Dict,
};

/// [`ColumnScan`], which filters values of a "value" scan based on a [StackProgram]
#[derive(Debug)]
pub(crate) struct ColumnScanFilter<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Current scan whose values are being filtered
    value_scan: &'a ColumnScanCell<'a, T>,

    /// [StackProgram] based on which the values will be filtered
    program: StackProgram,

    /// Values referenced by the program
    referenced_values: Rc<RefCell<Vec<AnyDataValue>>>,

    /// Dictionary used for translating values of `value_scan` into [AnyDataValue]
    dictionary: &'a RefCell<Dict>,

    /// Current value
    current_value: Option<T>,
}

impl<'a, T> ColumnScanFilter<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Create a new [ColumnScanFilter].
    pub(crate) fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        program: StackProgram,
        referenced_values: Rc<RefCell<Vec<AnyDataValue>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Self {
        Self {
            value_scan,
            program,
            referenced_values,
            dictionary,
            current_value: None,
        }
    }

    /// Check whether a given value passes the filter defined by `self.program`
    /// and return the result as a boolean.
    /// Returns `None` if the filter could not have been evaluated.
    fn check_value(&self, value: T) -> Option<bool> {
        let datavalue = value
            .into_datavalue(&self.dictionary.borrow())
            .expect("It is assumed that all id already in columns are present in the dictionary.");
        let referenced = &self.referenced_values.borrow();

        self.program.evaluate_bool(referenced, Some(datavalue))
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
            if self.check_value(next) == Some(true) {
                return Some(next);
            }
        }

        None
    }
}

impl<'a, T> Iterator for ColumnScanFilter<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.current_value = self.find_next();
        self.current_value
    }
}

impl<'a, T> ColumnScan for ColumnScanFilter<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let seeked_value = self.value_scan.seek(value)?;

        self.current_value = if self.check_value(seeked_value) == Some(true) {
            Some(seeked_value)
        } else {
            self.find_next()
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
        datavalues::AnyDataValue,
        function::{evaluation::StackProgram, tree::FunctionTree},
        management::database::Dict,
        tabular::operations::OperationTable,
    };

    use super::ColumnScanFilter;

    #[test]
    fn columnscan_filter_basic() {
        let dictionary = RefCell::new(Dict::default());

        let value_column = ColumnVector::new(vec![0i64, 7, 14, 21]);
        let value_scan = ColumnScanCell::new(ColumnScanEnum::Vector(value_column.iter()));

        let reference_values = vec![
            AnyDataValue::new_integer_from_i64(10),
            AnyDataValue::new_plain_string(String::from("test")),
        ];

        let operation_table = OperationTable::new_unique(3);
        let marker_int = *operation_table.get(0);
        let marker_str = *operation_table.get(1);
        let marker_value = *operation_table.get(2);

        let function = FunctionTree::equals(
            FunctionTree::reference(marker_value),
            FunctionTree::numeric_addition(
                FunctionTree::reference(marker_int),
                FunctionTree::string_length(FunctionTree::reference(marker_str)),
            ),
        );

        let program = StackProgram::from_function_tree(
            &function,
            &[(marker_int, 0), (marker_str, 1)].into_iter().collect(),
            Some(marker_value),
        );

        let mut scan_filter = ColumnScanFilter::new(
            &value_scan,
            program,
            Rc::new(RefCell::new(reference_values)),
            &dictionary,
        );

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), Some(14));
        assert_eq!(scan_filter.next(), None);

        scan_filter.reset();
        value_scan.reset();

        assert_eq!(scan_filter.seek(7), Some(14));
        assert_eq!(scan_filter.seek(12), Some(14));
        assert_eq!(scan_filter.seek(15), None);
    }
}
