//! Adaptive Builder to create [`VecT`] columns, based on streamed data

use crate::{
    error::Error,
    physical::{datatypes::Float, management::database::Dict},
    physical::{
        datatypes::{storage_value::VecT, Double},
        dictionary::Dictionary,
    },
};
use std::cell::RefCell;

#[macro_use]
mod macros;

// TODO: the first things until the next such comment can be moved to the physical layer

/// Trait for a Builder Proxy, which translates a value for a particular [`DataTypeName`] to the corresponding [`StorageType`][crate::physical::datatypes::StorageType] Column elements
pub trait ColumnBuilderProxy<T>: std::fmt::Debug {
    /// Cache a value to be added to the ColumnBuilder. If a value is already cached, this one is actually added to the ColumnBuilder before the new value is checked
    fn add(&mut self, input: T) -> Result<(), Error>;
    /// Forgets a cached value
    fn forget(&mut self);
    /// Commits the data, cleaning the cached value while adding it to the respective ColumnBuilder
    fn commit(&mut self);
}

/// Trait for Builder Proxy for the physical layer
pub trait PhysicalColumnBuilderProxy<T>: ColumnBuilderProxy<T> {
    /// Writes the remaining prepared value and returns a VecT
    fn finalize(self) -> VecT;
}

/// PhysicalBuilderProxy to add Strings
#[derive(Debug)]
pub struct PhysicalStringColumnBuilderProxy<'a> {
    dict: &'a RefCell<Dict>,
    value: Option<u64>,
    vec: Vec<u64>,
}

impl<'a> PhysicalStringColumnBuilderProxy<'a> {
    /// Create a new PhysicalStringColumnBuilderProxy with the given dictionary
    pub fn new(dict: &'a RefCell<Dict>) -> Self {
        Self {
            dict,
            value: Default::default(),
            vec: Default::default(),
        }
    }
}

impl ColumnBuilderProxy<String> for PhysicalStringColumnBuilderProxy<'_> {
    generic_trait_impl_without_add!(VecT::U64);
    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();
        self.value = Some(self.dict.borrow_mut().add(input).try_into()?);
        Ok(())
    }
}

impl PhysicalColumnBuilderProxy<String> for PhysicalStringColumnBuilderProxy<'_> {
    fn finalize(mut self) -> VecT {
        self.commit();
        VecT::U64(self.vec)
    }
}

/// PhysicalBuilderProxy to add types without special requirements (e.g. dictionary)
#[derive(Default, Debug)]
pub struct PhysicalGenericColumnBuilderProxy<T> {
    value: Option<T>,
    vec: Vec<T>,
}

physical_generic_trait_impl!(u64, VecT::U64);
physical_generic_trait_impl!(u32, VecT::U32);
physical_generic_trait_impl!(Float, VecT::Float);
physical_generic_trait_impl!(Double, VecT::Double);

/// Enum Collection of all physical builder proxies
#[derive(Debug)]
pub enum PhysicalBuilderProxyEnum<'a> {
    /// Proxy for String Type
    String(PhysicalStringColumnBuilderProxy<'a>),
    /// Proxy for U64 Type
    U64(PhysicalGenericColumnBuilderProxy<u64>),
    /// Proxy for U32 Type
    U32(PhysicalGenericColumnBuilderProxy<u32>),
    /// Proxy for Float Type
    Float(PhysicalGenericColumnBuilderProxy<Float>),
    /// Proxy for Double Type
    Double(PhysicalGenericColumnBuilderProxy<Double>),
}

// TODO: from here on things can be moved to the logical layer

/// Trait capturing builder proxies that use plain string (used for parsing in logical layer)
pub trait LogicalColumnBuilderProxy<'a, 'b>: ColumnBuilderProxy<String> {
    /// create a new LogicalColumnBuilerProxy from the physical equivalent
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self
    where
        Self: Sized;
}

/// LogicalBuilderProxy to add Any
#[derive(Debug)]
pub struct LogicalAnyColumnBuilderProxy<'a: 'b, 'b> {
    physical: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl ColumnBuilderProxy<String> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();

        // TODO: this is all super hacky but parsing proper ground terms is too slow...
        let trimmed_string = input.trim();

        let string_to_add = if trimmed_string.is_empty() {
            "\"\"".to_string()
        } else if trimmed_string.starts_with('<') && trimmed_string.ends_with('>') {
            trimmed_string[1..trimmed_string.len() - 1].to_string()
        } else {
            trimmed_string.to_string()
        };

        self.physical.add(string_to_add)
    }
}

impl<'a, 'b> LogicalColumnBuilderProxy<'a, 'b> for LogicalAnyColumnBuilderProxy<'a, 'b> {
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::String(physical) => Self { physical },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

/// LogicalBuilderProxy to add Integer
#[derive(Debug)]
pub struct LogicalIntegerColumnBuilderProxy<'b> {
    physical: &'b mut PhysicalGenericColumnBuilderProxy<u64>,
}

impl ColumnBuilderProxy<String> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();
        self.physical.add(input.parse::<u64>()?)
    }
}

impl<'a, 'b> LogicalColumnBuilderProxy<'a, 'b> for LogicalIntegerColumnBuilderProxy<'b> {
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::U64(physical) => Self { physical },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

/// LogicalBuilderProxy to add Float64
#[derive(Debug)]
pub struct LogicalFloat64ColumnBuilderProxy<'b> {
    physical: &'b mut PhysicalGenericColumnBuilderProxy<Double>,
}

impl ColumnBuilderProxy<String> for LogicalFloat64ColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();
        self.physical.add(Double::new(input.parse::<f64>()?)?)
    }
}

impl<'a, 'b> LogicalColumnBuilderProxy<'a, 'b> for LogicalFloat64ColumnBuilderProxy<'b> {
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::Double(physical) => Self { physical },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}
