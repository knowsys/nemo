//! The logical builder proxy concept allows to transform a given String, representing some data in a logical datatype
//! into some value, which can be given to the physical layer to store the data accordingly to its type

use std::cell::RefCell;
use std::collections::HashMap;

use nemo_physical::{
    builder_proxy::{
        ColumnBuilderProxy, PhysicalBuilderProxyEnum, PhysicalGenericColumnBuilderProxy,
        PhysicalStringColumnBuilderProxy,
    },
    datatypes::{DataValueT, Double},
};

use crate::io::parser::{all_input_consumed, parse_ground_term};

use super::{model::PrimitiveType, model::Term};
use crate::error::ReadingError;

/// Trait capturing builder proxies that use plain string (used for parsing in logical layer)
///
/// This parses from a given logical type to the physical type, without exposing details from one layer to the other.
pub trait LogicalColumnBuilderProxy<'a, 'b>: ColumnBuilderProxy<String> {
    /// Create a new [`LogicalColumnBuilderProxy`] from a given [`BuilderProxy`][nemo_physical::builder_proxy::PhysicalBuilderProxyEnum].
    ///
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

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        self.commit();

        let parsed_term =
            all_input_consumed(parse_ground_term(&RefCell::new(HashMap::new())))(input.trim())
                .unwrap_or(Term::StringLiteral(input.clone()));

        let parsed_datavalue = PrimitiveType::Any
            .ground_term_to_data_value_t(parsed_term)
            .expect("PrimitiveType::Any should work with every possible term we can get here.");

        let DataValueT::String(parsed_string) = parsed_datavalue else {
            unreachable!("PrimitiveType::Any should always be treated as String at the moment.")
        };

        self.physical.add(parsed_string)
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

/// [`LogicalColumnBuilderProxy`] to add String
#[derive(Debug)]
pub struct LogicalStringColumnBuilderProxy<'a: 'b, 'b> {
    physical: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl ColumnBuilderProxy<String> for LogicalStringColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        self.commit();
        // NOTE: we just pipe the string through as is, in particular we do not parse potential RDF terms
        // NOTE: we store the string in the same format as it would be stored in an any column;
        // this is important since right now we sometimes use the LogicalStringColumnBuilderProxy to directly write data that is known to only be strings into an any column and not only into string columns
        self.physical.add(format!("\"{input}\""))
    }
}

impl<'a, 'b> LogicalColumnBuilderProxy<'a, 'b> for LogicalStringColumnBuilderProxy<'a, 'b> {
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

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
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

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
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
