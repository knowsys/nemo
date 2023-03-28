use crate::{
    error::Error,
    physical::{dictionary::Dictionary},
};

use super::{
    adaptive_column_builder::{ColumnBuilderAdaptive, ColumnBuilderAdaptiveT},
    traits::columnbuilder::ColumnBuilder,
};

#[cfg(feature = "no-prefixed-string-dictionary")]
/// Dictionary Implementation used in the current configuration
pub type Dict = crate::physical::dictionary::StringDictionary;
#[cfg(not(feature = "no-prefixed-string-dictionary"))]
/// Dictionary Implementation used in the current configuration
pub type Dict = crate::physical::dictionary::PrefixedStringDictionary;

/// Trait for a Proxy builder, which handles the parsing and translation from [`string`] to [`StorageType`][crate::physical::datatypes::StorageType] Column elements
pub trait ProxyBuilder: std::fmt::Debug {
    /// Prepare another value to be added to the ColumnBuilder. If another value is already prepared, this one is actually added to the ColumnBuilder before the new value is checked
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error>;
    /// Forgets an already prepared value, to rollback that information
    fn rollback(&mut self);
    /// Writes a prepared value to the ColumnBuilder, if one exists
    fn write(&mut self);
    /// Writes the remaining prepared value and returns an Adaptive Column Builder
    fn finalize(self: Box<Self>) -> ColumnBuilderAdaptiveT;
}

/// ProxyBuilder to add Strings
#[derive(Default, Debug)]
pub struct ProxyStringBuilder {
    value: Option<u64>,
    column_builder: ColumnBuilderAdaptive<u64>,
}

impl ProxyBuilder for ProxyStringBuilder {
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(
            dictionary
                .expect("ProxyStringBuilder expects a Dictionary to be provided!")
                .add(string.to_string())
                .try_into()?,
        );

        Ok(())
    }

    fn rollback(&mut self) {
        self.value = None;
    }

    fn finalize(mut self: Box<Self>) -> ColumnBuilderAdaptiveT {
        self.as_mut().write();
        ColumnBuilderAdaptiveT::U64(self.column_builder)
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.column_builder.add(value);
        }
    }
}
