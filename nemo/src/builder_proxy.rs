//! The logical builder proxy concept allows to transform a given String, representing some data in a logical datatype
//! into some value, which can be given to the physical layer to store the data accordingly to its type

use nemo_physical::{
    builder_proxy::{
        ColumnBuilderProxy, PhysicalBuilderProxyEnum, PhysicalGenericColumnBuilderProxy,
        PhysicalStringColumnBuilderProxy,
    },
    datatypes::Double,
};

use oxiri::Iri;

use crate::{
    error::ReadingError,
    io::{
        formats::rdf_triples::{
            TurtleEncodedRDFTerm, INITIAL_FOR_SIMPLE_NUMERIC_LITERAL, XSD_STRING,
        },
        parser::{
            parse_bare_name, span_from_str,
            sparql::blank_node_label,
            turtle::{is_valid_rdf_literal, numeric_literal},
        },
    },
};

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

impl LogicalAnyColumnBuilderProxy<'_, '_> {
    fn normalize_string(input: String) -> String {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return r#""""#.to_string();
        }

        if trimmed.starts_with('<') && trimmed.ends_with('>') {
            // an absolute IRI, drop angle brackets
            return trimmed[1..trimmed.len() - 1].to_string();
        }

        if trimmed.starts_with('"') && trimmed.ends_with('>') {
            // potentially an RDF literal
            if trimmed.starts_with('"') && trimmed.ends_with(XSD_STRING) {
                // an XSD string literal, drop the datatype
                return trimmed[..(trimmed.len() - XSD_STRING.len() - 2)].to_string();
            }

            // otherwise it might still be a valid RDF literal, make sure it is.
            if is_valid_rdf_literal(trimmed) {
                // it's an RDF literal, return as-is
                return trimmed.to_string();
            }
        }

        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            // already a quoted string, pass as-is
            return trimmed.to_string();
        }

        if trimmed.starts_with(INITIAL_FOR_SIMPLE_NUMERIC_LITERAL) {
            // possibly a simple numeric literal, might need to normalise
            if let Ok((remainder, literal)) = numeric_literal(span_from_str(trimmed)) {
                if remainder.is_empty() {
                    // convert to typed literal representation
                    return literal.into_rdf_term_literal();
                }
            }
        }

        if trimmed.starts_with("_:") {
            // potentially a bnode label
            if let Ok((remainder, _)) = blank_node_label(span_from_str(trimmed)) {
                if remainder.is_empty() {
                    return trimmed.to_string();
                }
            }
        }

        // check if it's a valid bare name
        if let Ok((remainder, _)) = parse_bare_name(span_from_str(trimmed)) {
            if remainder.is_empty() {
                // it is, pass as-is
                return trimmed.to_string();
            }
        } else if Iri::parse(trimmed).is_ok() {
            // it is, pass as-is
            return trimmed.to_string();
        }

        // otherwise it needs to be quoted
        format!(r#""{trimmed}""#).to_string()
    }
}

impl ColumnBuilderProxy<String> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <LogicalAnyColumnBuilderProxy<'_, '_> as ColumnBuilderProxy<String>>::commit(self);

        self.physical.add(Self::normalize_string(input))
    }
}

impl ColumnBuilderProxy<TurtleEncodedRDFTerm> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: TurtleEncodedRDFTerm) -> Result<(), ReadingError> {
        <LogicalAnyColumnBuilderProxy<'_, '_> as ColumnBuilderProxy<TurtleEncodedRDFTerm>>::commit(
            self,
        );

        self.physical.add(input.into_normalized_string())
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
