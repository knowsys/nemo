use crate::{
    error::Error,
    physical::management::database::Dict,
    physical::{datatypes::storage_value::VecT, dictionary::Dictionary},
};

/// Trait for a Proxy builder, which handles the parsing and translation from [`string`] to [`StorageType`][crate::physical::datatypes::StorageType] Column elements
pub trait ProxyColumnBuilder: std::fmt::Debug {
    /// Prepare another value to be added to the ColumnBuilder. If another value is already prepared, this one is actually added to the ColumnBuilder before the new value is checked
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error>;
    /// Forgets an already prepared value, to rollback that information
    fn rollback(&mut self);
    /// Writes a prepared value to the ColumnBuilder, if one exists
    fn write(&mut self);
    /// Writes the remaining prepared value and returns a VecT
    fn finalize(self: Box<Self>) -> VecT;
}

/// ProxyBuilder to add Strings
#[derive(Default, Debug)]
pub struct ProxyStringColumnBuilder {
    value: Option<u64>,
    vec: Vec<u64>,
}

impl ProxyColumnBuilder for ProxyStringColumnBuilder {
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(
            dictionary
                .expect("ProxyStringColumnBuilder expects a Dictionary to be provided!")
                .add(string.to_string())
                .try_into()?,
        );

        Ok(())
    }

    fn rollback(&mut self) {
        self.value = None;
    }

    fn finalize(mut self: Box<Self>) -> VecT {
        self.as_mut().write();
        VecT::U64(self.vec)
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.vec.push(value);
        }
    }
}
