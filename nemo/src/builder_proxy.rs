//! The logical builder proxy concept allows to transform a given String, representing some data in a logical datatype
//! into some value, which can be given to the physical layer to store the data accordingly to its type

use std::io::BufReader;
use std::marker::PhantomData;

use num::FromPrimitive;

use oxiri::Iri;
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleParser;

use nemo_physical::{
    builder_proxy::{
        ColumnBuilderProxy, PhysicalBuilderProxyEnum, PhysicalGenericColumnBuilderProxy,
        PhysicalStringColumnBuilderProxy,
    },
    datatypes::Double,
};

use crate::{
    error::ReadingError,
    io::{
        formats::rdf_triples::rio_term_to_term,
        parser::{parse_bare_name, span_from_str},
    },
};

use super::model::types::primitive_logical_value::{
    any_double_to_physical_double, any_integer_to_physical_integer, any_string_to_physical_string,
    any_term_to_physical_string, logical_double_to_physical_string,
    logical_integer_to_physical_string, logical_string_to_physical_string,
};

use super::model::{PrimitiveType, Term};

/// Implements the type-independent [`ColumnBuilderProxy`] trait methods.
macro_rules! logical_generic_trait_impl {
    () => {
        fn commit(&mut self) {
            self.inner.commit()
        }

        fn forget(&mut self) {
            self.inner.forget()
        }
    };
}

#[derive(Debug)]
pub enum LogicalColumnBuilderProxyT<'a, 'b> {
    Any(LogicalAnyColumnBuilderProxy<'a, 'b>),
    String(LogicalStringColumnBuilderProxy<'a, 'b>),
    Integer(LogicalIntegerColumnBuilderProxy<'b>),
    Float64(LogicalFloat64ColumnBuilderProxy<'b>),
}
///
/// [`LogicalColumnBuilderProxy`] to add Any
#[derive(Debug)]
pub struct LogicalAnyColumnBuilderProxy<'a: 'b, 'b> {
    inner: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl<'a, 'b> LogicalAnyColumnBuilderProxy<'a, 'b> {
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::String(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl ColumnBuilderProxy<Term> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Term) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<Term>>::commit(self);
        let parsed_string = any_term_to_physical_string(input)?;
        self.inner.add(parsed_string)
    }
}

impl ColumnBuilderProxy<String> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        self.inner.add(logical_string_to_physical_string(input))
    }
}

impl ColumnBuilderProxy<i64> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: i64) -> Result<(), ReadingError> {
        self.inner.add(logical_integer_to_physical_string(input))
    }
}

impl ColumnBuilderProxy<Double> for LogicalAnyColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Double) -> Result<(), ReadingError> {
        self.inner.add(logical_double_to_physical_string(input))
    }
}

/// [`LogicalColumnBuilderProxy`] to add String
#[derive(Debug)]
pub struct LogicalStringColumnBuilderProxy<'a: 'b, 'b> {
    inner: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl<'a, 'b> LogicalStringColumnBuilderProxy<'a, 'b> {
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::String(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl ColumnBuilderProxy<Term> for LogicalStringColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Term) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<Term>>::commit(self);
        self.inner.add(any_string_to_physical_string(input)?)
    }
}

impl ColumnBuilderProxy<String> for LogicalStringColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        // NOTE: we just pipe the string through as is, in particular we do not parse potential RDF terms
        // NOTE: we store the string in the same format as it would be stored in an any column;
        // this is important since right now we sometimes use the LogicalStringColumnBuilderProxy to directly write data that is known to only be strings into an any column and not only into string columns
        self.inner.add(logical_string_to_physical_string(input))
    }
}

impl ColumnBuilderProxy<i64> for LogicalStringColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: i64) -> Result<(), ReadingError> {
        self.add(input.to_string())
    }
}

impl ColumnBuilderProxy<Double> for LogicalStringColumnBuilderProxy<'_, '_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Double) -> Result<(), ReadingError> {
        self.add(input.to_string())
    }
}

/// [`LogicalColumnBuilderProxy`] to add Integer
#[derive(Debug)]
pub struct LogicalIntegerColumnBuilderProxy<'b> {
    inner: &'b mut PhysicalGenericColumnBuilderProxy<i64>,
}

impl<'a, 'b> LogicalIntegerColumnBuilderProxy<'b> {
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::I64(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl ColumnBuilderProxy<Term> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Term) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<Term>>::commit(self);
        self.inner.add(any_integer_to_physical_integer(input)?)
    }
}

impl ColumnBuilderProxy<i64> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: i64) -> Result<(), ReadingError> {
        self.inner.add(input)
    }
}

impl ColumnBuilderProxy<String> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(input.parse()?)
    }
}

impl ColumnBuilderProxy<Double> for LogicalIntegerColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Double) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<Double>>::commit(self);
        self.inner.add(
            i64::from_f64(input.into()).ok_or(ReadingError::TypeConversionError(
                input.to_string(),
                PrimitiveType::Integer.to_string(),
            ))?,
        )
    }
}

/// [`LogicalColumnBuilderProxy`] to add Float64
#[derive(Debug)]
pub struct LogicalFloat64ColumnBuilderProxy<'b> {
    inner: &'b mut PhysicalGenericColumnBuilderProxy<Double>,
}

impl<'a, 'b> LogicalFloat64ColumnBuilderProxy<'b> {
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::Double(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl ColumnBuilderProxy<Term> for LogicalFloat64ColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Term) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<Term>>::commit(self);
        self.inner.add(any_double_to_physical_double(input)?)
    }
}

impl ColumnBuilderProxy<Double> for LogicalFloat64ColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: Double) -> Result<(), ReadingError> {
        self.inner.add(input)
    }
}

impl ColumnBuilderProxy<String> for LogicalFloat64ColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(Double::new(input.parse()?)?)
    }
}

impl ColumnBuilderProxy<i64> for LogicalFloat64ColumnBuilderProxy<'_> {
    logical_generic_trait_impl!();

    fn add(&mut self, input: i64) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(Double::new(f64::from_i64(input).ok_or(
            ReadingError::TypeConversionError(
                input.to_string(),
                PrimitiveType::Integer.to_string(),
            ),
        )?)?)
    }
}

fn parse_rdf_term_from_string(input: String) -> Term {
    const BASE: &str = "a:";

    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Term::StringLiteral(trimmed.to_string());
    }

    let data = format!("<> <> {trimmed}.");
    let parser = TurtleParser::new(
        BufReader::new(data.as_bytes()),
        Iri::parse(BASE.to_string()).ok(),
    );

    let terms = parser
        .into_iter(|triple| {
            let term = rio_term_to_term(triple.object)?;
            let base_stripped = if let Term::Constant(c) = &term {
                c.to_string()
                    .strip_prefix(BASE)
                    .map(|stripped| Term::Constant(stripped.to_string().into()))
                    .unwrap_or(term)
            } else {
                term
            };
            Ok(base_stripped)
        })
        .collect::<Result<Vec<_>, ReadingError>>();

    if let Ok(terms) = terms {
        // make sure this really parsed as a single triple
        if terms.len() == 1 {
            return terms.first().expect("is not empty").clone();
        }
    }

    // not a valid RDF term.
    // check if it's a valid bare name
    if let Ok((remainder, _)) = parse_bare_name(span_from_str(trimmed)) {
        if remainder.is_empty() {
            // it is, pass as-is
            return Term::Constant(trimmed.to_string().into());
        }
    }

    // might still be a full IRI
    if Iri::parse(trimmed).is_ok() {
        // it is, pass as-is
        return Term::Constant(trimmed.to_string().into());
    }

    // otherwise we treat the input as a string literal
    Term::StringLiteral(trimmed.to_string())
}

#[derive(Debug)]
pub struct GenericLogicalParser<Intermediate, T>
where
    T: ColumnBuilderProxy<Intermediate>,
{
    inner: T,
    phantom: PhantomData<Intermediate>,
}

impl<Intermediate, T> GenericLogicalParser<Intermediate, T>
where
    T: ColumnBuilderProxy<Intermediate>,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            phantom: PhantomData,
        }
    }
}

impl<Input, T> ColumnBuilderProxy<Input> for GenericLogicalParser<Input, T>
where
    T: ColumnBuilderProxy<Input>,
    Input: std::fmt::Debug,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: Input) -> Result<(), ReadingError> {
        self.inner.add(input)
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<Term, T>
where
    T: ColumnBuilderProxy<Term>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        self.inner.add(parse_rdf_term_from_string(input))
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<i64, T>
where
    T: ColumnBuilderProxy<i64>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(input.parse()?)
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<Double, T>
where
    T: ColumnBuilderProxy<Double>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(Double::new(input.parse()?)?)
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::model::RdfLiteral;

    use super::*;

    #[test]
    fn any_parsing() {
        assert_eq!(
            parse_rdf_term_from_string("".to_string()),
            Term::StringLiteral("".to_string())
        );

        assert_eq!(
            parse_rdf_term_from_string("<http://example.org>".to_string()),
            Term::Constant("http://example.org".to_string().into())
        );

        assert_eq!(
            parse_rdf_term_from_string(
                r#""23"^^<http://www.w3.org/2001/XMLSchema#string>"#.to_string()
            ),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "23".to_string(),
                datatype: "http://www.w3.org/2001/XMLSchema#string".to_string()
            })
        );

        assert_eq!(
            parse_rdf_term_from_string(
                r#""12345"^^<http://www.w3.org/2001/XMLSchema#integer>"#.to_string()
            ),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "12345".to_string(),
                datatype: "http://www.w3.org/2001/XMLSchema#integer".to_string()
            }),
        );

        assert_eq!(
            parse_rdf_term_from_string(r#""quoted""#.to_string()),
            Term::StringLiteral("quoted".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("12345".to_string()),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "12345".to_string(),
                datatype: "http://www.w3.org/2001/XMLSchema#integer".to_string()
            })
        );

        assert_eq!(
            parse_rdf_term_from_string("_:foo".to_string()),
            Term::Constant("_:foo".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("bare_name".to_string()),
            Term::Constant("bare_name".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("http://example.org".to_string()),
            Term::Constant("http://example.org".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("with space".to_string()),
            Term::Constant("with space".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("with_question_mark?".to_string()),
            Term::StringLiteral("with_question_mark?".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("a. a a a".to_string()),
            Term::StringLiteral("a. a a a".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("<a>. <a> <a> <a>".to_string()),
            Term::StringLiteral("<a>. <a> <a> <a>".to_string()),
        );
    }
}
