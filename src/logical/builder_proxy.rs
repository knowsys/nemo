//! The logical builder proxy concept allows to transform a given String, representing some data in a logical datatype
//! into some value, which can be given to the physical layer to store the data accordingly to its type
use crate::{
    error::Error,
    physical::{
        builder_proxy::{
            ColumnBuilderProxy, PhysicalBuilderProxyEnum, PhysicalGenericColumnBuilderProxy,
            PhysicalStringColumnBuilderProxy,
        },
        datatypes::Double,
    },
};

/// Trait capturing builder proxies that use plain string (used for parsing in logical layer)
pub trait LogicalColumnBuilderProxy<'a, 'b>: ColumnBuilderProxy<String> {
    /// Create a new [`LogicalColumnBuilderProxy`] which nests its physical target equivalent [`BuilderProxy`][crate::physical::builder_proxy::PhysicalBuilderProxyEnum].
    ///
    /// This offers a representation of the parsing from a given logical type to the physical type, without exposing details from one layer to the other.
    /// # Panics
    /// If the logical and the nested physical type are not compatible, an `unreachable` panic will be thrown.
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self
    where
        Self: Sized;
}

/// Implements the type-independent [`ColumnBuilderProxy`] trait methods.
macro_rules! logical_generic_trait_impl {
    () => {
        fn commit(&mut self) {
            self.physical.commit()
        }

        fn forget(&mut self) {
            self.physical.forget()
        }
    };
}

/// [`LogicalColumnBuilderProxy`] to add Any
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

/// [`LogicalColumnBuilderProxy`] to add Integer
#[derive(Debug)]
pub struct LogicalIntegerColumnBuilderProxy<'b> {
    physical: &'b mut PhysicalGenericColumnBuilderProxy<i64>,
}

impl ColumnBuilderProxy<String> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();
        self.physical.add(input.parse::<i64>()?)
    }
}

impl<'a, 'b> LogicalColumnBuilderProxy<'a, 'b> for LogicalIntegerColumnBuilderProxy<'b> {
    fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::I64(physical) => Self { physical },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

/// [`LogicalColumnBuilderProxy`] to add Float64
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
