//! This module defines [ColumnScanFilter].

use std::{cell::RefCell, ops::Range, rc::Rc};

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
    datavalues::AnyDataValue,
    dictionary::meta_dv_dict::MetaDvDictionary,
    function::evaluation::StackProgram,
};

/// [`ColumnScan`], which filters values of a "value" scan based on a [StackProgram]
#[derive(Debug)]
pub struct ColumnScanFilter<'a, T>
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
    /// TODO: Lifetime probably not correct
    dictionary: &'a MetaDvDictionary,

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
        dictionary: &'a MetaDvDictionary,
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
            .into_datavalue(self.dictionary)
            .expect("It is assumed that all id already in columns are present in the dictionary.");
        let referenced = &self.referenced_values.borrow();

        self.program.evaluate_bool(referenced, datavalue)
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
