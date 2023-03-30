//! Adaptive Builder to create [`VecT`] columns, based on streamed data
use crate::{
    error::Error,
    physical::{datatypes::Float, management::database::Dict},
    physical::{
        datatypes::{storage_value::VecT, Double},
        dictionary::Dictionary,
    },
};

/// Trait for a Proxy builder, which handles the parsing and translation from [`string`] to [`StorageType`][crate::physical::datatypes::StorageType] Column elements
pub trait ColumnBuilderProxy: std::fmt::Debug {
    /// Prepare another value to be added to the ColumnBuilder. If another value is already prepared, this one is actually added to the ColumnBuilder before the new value is checked
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error>;
    /// Forgets an already prepared value
    fn forget(&mut self);
    /// Writes a prepared value to the ColumnBuilder, if one exists
    fn write(&mut self);
    /// Writes the remaining prepared value and returns a VecT
    fn finalize(self: Box<Self>) -> VecT;
}

/// ProxyBuilder to add Strings
#[derive(Default, Debug)]
pub struct StringColumnBuilderProxy {
    value: Option<u64>,
    vec: Vec<u64>,
}

impl ColumnBuilderProxy for StringColumnBuilderProxy {
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

    fn forget(&mut self) {
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

/// ProxyBuilder to add U64
#[derive(Default, Debug)]
pub struct U64ColumnBuilderProxy {
    value: Option<u64>,
    vec: Vec<u64>,
}

impl ColumnBuilderProxy for U64ColumnBuilderProxy {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(string.parse::<u64>()?);
        Ok(())
    }

    fn forget(&mut self) {
        self.value = None;
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.vec.push(value);
        }
    }

    fn finalize(mut self: Box<Self>) -> VecT {
        self.as_mut().write();
        VecT::U64(self.vec)
    }
}

/// ProxyBuilder to add U32
#[derive(Default, Debug)]
pub struct U32ColumnBuilderProxy {
    value: Option<u32>,
    vec: Vec<u32>,
}

impl ColumnBuilderProxy for U32ColumnBuilderProxy {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(string.parse::<u32>()?);
        Ok(())
    }

    fn forget(&mut self) {
        self.value = None;
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.vec.push(value);
        }
    }

    fn finalize(mut self: Box<Self>) -> VecT {
        self.as_mut().write();
        VecT::U32(self.vec)
    }
}

/// ProxyBuilder to add Float
#[derive(Default, Debug)]
pub struct FloatColumnBuilderProxy {
    value: Option<Float>,
    vec: Vec<Float>,
}

impl ColumnBuilderProxy for FloatColumnBuilderProxy {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        let val = string.parse::<f32>()?;
        self.value = Some(Float::new(val)?);
        Ok(())
    }

    fn forget(&mut self) {
        self.value = None;
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.vec.push(value);
        }
    }

    fn finalize(mut self: Box<Self>) -> VecT {
        self.as_mut().write();
        VecT::Float(self.vec)
    }
}

/// ProxyBuilder to add Double
#[derive(Default, Debug)]
pub struct DoubleColumnBuilderProxy {
    value: Option<Double>,
    vec: Vec<Double>,
}

impl ColumnBuilderProxy for DoubleColumnBuilderProxy {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        let val = string.parse::<f64>()?;
        self.value = Some(Double::new(val)?);
        Ok(())
    }

    fn forget(&mut self) {
        self.value = None;
    }

    fn write(&mut self) {
        if let Some(value) = self.value {
            self.vec.push(value);
        }
    }

    fn finalize(mut self: Box<Self>) -> VecT {
        self.as_mut().write();
        VecT::Double(self.vec)
    }
}
