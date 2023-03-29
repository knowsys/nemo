use crate::{
    error::Error,
    physical::{datatypes::Float, management::database::Dict},
    physical::{
        datatypes::{storage_value::VecT, Double},
        dictionary::Dictionary,
    },
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

/// ProxyBuilder to add U64
#[derive(Default, Debug)]
pub struct ProxyU64ColumnBuilder {
    value: Option<u64>,
    vec: Vec<u64>,
}

impl ProxyColumnBuilder for ProxyU64ColumnBuilder {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(string.parse::<u64>()?);
        Ok(())
    }

    fn rollback(&mut self) {
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
pub struct ProxyU32ColumnBuilder {
    value: Option<u32>,
    vec: Vec<u32>,
}

impl ProxyColumnBuilder for ProxyU32ColumnBuilder {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        self.value = Some(string.parse::<u32>()?);
        Ok(())
    }

    fn rollback(&mut self) {
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
pub struct ProxyFloatColumnBuilder {
    value: Option<Float>,
    vec: Vec<Float>,
}

impl ProxyColumnBuilder for ProxyFloatColumnBuilder {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        let val = string.parse::<f32>()?;
        self.value = Some(Float::new(val)?);
        Ok(())
    }

    fn rollback(&mut self) {
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
pub struct ProxyDoubleColumnBuilder {
    value: Option<Double>,
    vec: Vec<Double>,
}

impl ProxyColumnBuilder for ProxyDoubleColumnBuilder {
    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.write();
        let val = string.parse::<f64>()?;
        self.value = Some(Double::new(val)?);
        Ok(())
    }

    fn rollback(&mut self) {
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
