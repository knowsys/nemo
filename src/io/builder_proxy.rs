//! Adaptive Builder to create [`VecT`] columns, based on streamed data
use std::collections::HashMap;

use crate::{
    error::Error,
    logical::types::LogicalTypeEnum,
    physical::{
        datatypes::{storage_value::VecT, Double},
        dictionary::Dictionary,
    },
    physical::{
        datatypes::{DataValueT, Float},
        management::database::Dict,
    },
};

use super::parser::{all_input_consumed, parse_ground_term};

#[macro_use]
mod macros;

/// Trait for a Proxy builder, which handles the parsing and translation from [`string`] to [`StorageType`][crate::physical::datatypes::StorageType] Column elements
pub trait ColumnBuilderProxy: std::fmt::Debug {
    /// Cache a value to be added to the ColumnBuilder. If a value is already cached, this one is actually added to the ColumnBuilder before the new value is checked
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error>;
    /// Forgets a cached value
    fn forget(&mut self);
    /// Commits the data, cleaning the cached value while adding it to the respective ColumnBuilder
    fn commit(&mut self);
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
    generic_trait_impl!(VecT::U64);
    fn add(&mut self, string: &str, dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.commit();

        // TODO: DSV parsing should depend on logical types
        let parsed =
            all_input_consumed(|input| parse_ground_term(&HashMap::new(), input))(string.trim())
                .map(|gt| {
                    LogicalTypeEnum::Any.ground_term_to_data_value_t(gt).expect(
                "LogicalTypeEnum::Any should work with every possible term we can get here.",
            )
                })
                .unwrap_or(DataValueT::String(format!("\"{string}\""))); // Treat term as string literal by default

        let DataValueT::String(parsed_string) = parsed else { unreachable!("LogicalTypeEnum::Any should always be treated as String at the moment.") };

        self.value = Some(
            dictionary
                .expect("StringColumnBuilderProxy expects a Dictionary to be provided!")
                .add(parsed_string)
                .try_into()?,
        );

        Ok(())
    }
}

/// ProxyBuilder to add U64
#[derive(Default, Debug)]
pub struct U64ColumnBuilderProxy {
    value: Option<u64>,
    vec: Vec<u64>,
}

impl ColumnBuilderProxy for U64ColumnBuilderProxy {
    generic_trait_impl!(VecT::U64);

    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.commit();
        self.value = Some(string.parse::<u64>()?);
        Ok(())
    }
}

/// ProxyBuilder to add U32
#[derive(Default, Debug)]
pub struct U32ColumnBuilderProxy {
    value: Option<u32>,
    vec: Vec<u32>,
}

impl ColumnBuilderProxy for U32ColumnBuilderProxy {
    generic_trait_impl!(VecT::U32);

    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.commit();
        self.value = Some(string.parse::<u32>()?);
        Ok(())
    }
}

/// ProxyBuilder to add Float
#[derive(Default, Debug)]
pub struct FloatColumnBuilderProxy {
    value: Option<Float>,
    vec: Vec<Float>,
}

impl ColumnBuilderProxy for FloatColumnBuilderProxy {
    generic_trait_impl!(VecT::Float);

    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.commit();
        let val = string.parse::<f32>()?;
        self.value = Some(Float::new(val)?);
        Ok(())
    }
}

/// ProxyBuilder to add Double
#[derive(Default, Debug)]
pub struct DoubleColumnBuilderProxy {
    value: Option<Double>,
    vec: Vec<Double>,
}

impl ColumnBuilderProxy for DoubleColumnBuilderProxy {
    generic_trait_impl!(VecT::Double);

    fn add(&mut self, string: &str, _dictionary: Option<&mut Dict>) -> Result<(), Error> {
        self.commit();
        let val = string.parse::<f64>()?;
        self.value = Some(Double::new(val)?);
        Ok(())
    }
}
