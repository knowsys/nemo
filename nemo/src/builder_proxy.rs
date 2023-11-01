//! The logical builder proxy concept allows to transform a given String, representing some data in a logical datatype
//! into some value, which can be given to the physical layer to store the data accordingly to its type

use std::io::BufReader;
use std::marker::PhantomData;

use oxiri::Iri;
use rio_api::parser::TriplesParser;
use rio_turtle::TurtleParser;

use nemo_physical::{
    builder_proxy::{
        ColumnBuilderProxy, PhysicalBuilderProxyEnum, PhysicalGenericColumnBuilderProxy,
        PhysicalStringColumnBuilderProxy,
    },
    datatypes::{data_value::PhysicalString, Double},
};

use crate::{
    error::ReadingError,
    io::parser::{parse_bare_name, span_from_str},
    model::{
        types::primitive_logical_value::{LogicalFloat64, LogicalInteger, LogicalString},
        Constant,
    },
};

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

/// LogicalColumnBuilderProxy Variants for diffent (primitive) logical types
#[derive(Debug)]
pub enum LogicalColumnBuilderProxyT<'a, 'b> {
    /// Any variant
    Any(LogicalAnyColumnBuilderProxy<'a, 'b>),
    /// String variant
    String(LogicalStringColumnBuilderProxy<'a, 'b>),
    /// Integer variant
    Integer(LogicalIntegerColumnBuilderProxy<'b>),
    /// Float64 variant
    Float64(LogicalFloat64ColumnBuilderProxy<'b>),
}

impl<'a, 'b, T> ColumnBuilderProxy<T> for LogicalColumnBuilderProxyT<'a, 'b>
where
    LogicalAnyColumnBuilderProxy<'a, 'b>: ColumnBuilderProxy<T>,
    LogicalStringColumnBuilderProxy<'a, 'b>: ColumnBuilderProxy<T>,
    LogicalIntegerColumnBuilderProxy<'b>: ColumnBuilderProxy<T>,
    LogicalFloat64ColumnBuilderProxy<'b>: ColumnBuilderProxy<T>,
{
    fn commit(&mut self) {
        match self {
            Self::Any(lcbp) => {
                <LogicalAnyColumnBuilderProxy as ColumnBuilderProxy<T>>::commit(lcbp)
            }
            Self::String(lcbp) => {
                <LogicalStringColumnBuilderProxy as ColumnBuilderProxy<T>>::commit(lcbp)
            }
            Self::Integer(lcbp) => {
                <LogicalIntegerColumnBuilderProxy as ColumnBuilderProxy<T>>::commit(lcbp)
            }
            Self::Float64(lcbp) => {
                <LogicalFloat64ColumnBuilderProxy as ColumnBuilderProxy<T>>::commit(lcbp)
            }
        }
    }

    fn forget(&mut self) {
        match self {
            Self::Any(lcbp) => {
                <LogicalAnyColumnBuilderProxy as ColumnBuilderProxy<T>>::forget(lcbp)
            }
            Self::String(lcbp) => {
                <LogicalStringColumnBuilderProxy as ColumnBuilderProxy<T>>::forget(lcbp)
            }
            Self::Integer(lcbp) => {
                <LogicalIntegerColumnBuilderProxy as ColumnBuilderProxy<T>>::forget(lcbp)
            }
            Self::Float64(lcbp) => {
                <LogicalFloat64ColumnBuilderProxy as ColumnBuilderProxy<T>>::forget(lcbp)
            }
        }
    }

    fn add(&mut self, input: T) -> Result<(), ReadingError> {
        match self {
            Self::Any(lcbp) => lcbp.add(input),
            Self::String(lcbp) => lcbp.add(input),
            Self::Integer(lcbp) => lcbp.add(input),
            Self::Float64(lcbp) => lcbp.add(input),
        }
    }
}

/// Logical [`ColumnBuilderProxy`] to add Any
#[derive(Debug)]
pub struct LogicalAnyColumnBuilderProxy<'a: 'b, 'b> {
    inner: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl<'a, 'b> LogicalAnyColumnBuilderProxy<'a, 'b> {
    /// Create new LogicalAnyColumnBuilderProxy from PhysicalStringColumnBuilderProxy (wrapped in enum)
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::String(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    /// wrap LogicalAnyColumnBuilderProxy into GenericLogicalParser
    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl<T> ColumnBuilderProxy<T> for LogicalAnyColumnBuilderProxy<'_, '_>
where
    PhysicalString: TryFrom<T>,
    Constant: TryFrom<T>,
    ReadingError: From<<PhysicalString as TryFrom<T>>::Error>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: T) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<T>>::commit(self);
        self.inner.add(input.try_into()?)
    }
}

/// Logical [`ColumnBuilderProxy`] to add String
#[derive(Debug)]
pub struct LogicalStringColumnBuilderProxy<'a: 'b, 'b> {
    inner: &'b mut PhysicalStringColumnBuilderProxy<'a>,
}

impl<'a, 'b> LogicalStringColumnBuilderProxy<'a, 'b> {
    /// Create new LogicalStringColumnBuilderProxy from PhysicalStringColumnBuilderProxy (wrapped in enum)
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::String(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    /// wrap LogicalStringColumnBuilderProxy into GenericLogicalParser
    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl<T> ColumnBuilderProxy<T> for LogicalStringColumnBuilderProxy<'_, '_>
where
    LogicalString: TryFrom<T>,
    ReadingError: From<<LogicalString as TryFrom<T>>::Error>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: T) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<T>>::commit(self);
        self.inner.add(LogicalString::try_from(input)?.into())
    }
}

/// Logical [`ColumnBuilderProxy`] to add Integer
#[derive(Debug)]
pub struct LogicalIntegerColumnBuilderProxy<'b> {
    inner: &'b mut PhysicalGenericColumnBuilderProxy<i64>,
}

impl<'a, 'b> LogicalIntegerColumnBuilderProxy<'b> {
    /// Create new LogicalIntegerColumnBuilderProxy from PhysicalI64ColumnBuilderProxy (wrapped in enum)
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::I64(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    /// wrap LogicalIntegerColumnBuilderProxy into GenericLogicalParser
    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl<T> ColumnBuilderProxy<T> for LogicalIntegerColumnBuilderProxy<'_>
where
    LogicalInteger: TryFrom<T>,
    ReadingError: From<<LogicalInteger as TryFrom<T>>::Error>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: T) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<T>>::commit(self);
        self.inner.add(LogicalInteger::try_from(input)?.into())
    }
}

/// Logical [`ColumnBuilderProxy`] to add Float64
#[derive(Debug)]
pub struct LogicalFloat64ColumnBuilderProxy<'b> {
    inner: &'b mut PhysicalGenericColumnBuilderProxy<Double>,
}

impl<'a, 'b> LogicalFloat64ColumnBuilderProxy<'b> {
    /// Create new LogicalFloat64ColumnBuilderProxy from PhysicalDoubleColumnBuilderProxy (wrapped in enum)
    pub fn new(physical_builder_proxy: &'b mut PhysicalBuilderProxyEnum<'a>) -> Self {
        match physical_builder_proxy {
            PhysicalBuilderProxyEnum::Double(inner) => Self { inner },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }

    /// wrap LogicalFloat64ColumnBuilderProxy into GenericLogicalParser
    pub fn into_parser<Intermediate>(self) -> GenericLogicalParser<Intermediate, Self>
    where
        Self: ColumnBuilderProxy<Intermediate>,
    {
        GenericLogicalParser::new(self)
    }
}

impl<T> ColumnBuilderProxy<T> for LogicalFloat64ColumnBuilderProxy<'_>
where
    LogicalFloat64: TryFrom<T>,
    ReadingError: From<<LogicalFloat64 as TryFrom<T>>::Error>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: T) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<T>>::commit(self);
        self.inner.add(LogicalFloat64::try_from(input)?.into())
    }
}

fn parse_rdf_term_from_string(input: String) -> Constant {
    const BASE: &str = "a:";

    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Constant::StringLiteral(trimmed.to_string());
    }

    let data = format!("<> <> {trimmed}.");
    let parser = TurtleParser::new(
        BufReader::new(data.as_bytes()),
        Iri::parse(BASE.to_string()).ok(),
    );

    let constants = parser
        .into_iter(|triple| {
            let term = triple.object.try_into()?;
            let base_stripped = if let Constant::Abstract(c) = &term {
                c.to_string()
                    .strip_prefix(BASE)
                    .map(|stripped| Constant::Abstract(stripped.to_string().into()))
                    .unwrap_or(term)
            } else {
                term
            };
            Ok(base_stripped)
        })
        .collect::<Result<Vec<_>, ReadingError>>();

    if let Ok(constant) = constants {
        // make sure this really parsed as a single triple
        if constant.len() == 1 {
            return constant.first().expect("Length of constant is 1.").clone();
        }
    }

    // Not a valid RDF term.
    // Check if it's a valid bare name
    if let Ok((remainder, _)) = parse_bare_name(span_from_str(trimmed)) {
        if remainder.is_empty() {
            // it is, pass as-is
            return Constant::Abstract(trimmed.to_string().into());
        }
    }

    // Might still be a full IRI
    if Iri::parse(trimmed).is_ok() {
        // it is, pass as-is
        return Constant::Abstract(trimmed.to_string().into());
    }

    // Otherwise we treat the input as a string literal
    Constant::StringLiteral(trimmed.to_string())
}

/// Generic Struct to extend LogicalColumnBuilderProxies with Parsing functionality
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
    /// Create a GenericLogicalParser from its inner (generic) value
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
        <Self as ColumnBuilderProxy<Input>>::commit(self);
        self.inner.add(input)
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<Constant, T>
where
    T: ColumnBuilderProxy<Constant>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(parse_rdf_term_from_string(input))
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<LogicalString, T>
where
    T: ColumnBuilderProxy<LogicalString>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(input.into())
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<LogicalInteger, T>
where
    T: ColumnBuilderProxy<LogicalInteger>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(input.parse::<i64>()?.into())
    }
}

impl<T> ColumnBuilderProxy<String> for GenericLogicalParser<LogicalFloat64, T>
where
    T: ColumnBuilderProxy<LogicalFloat64>,
{
    logical_generic_trait_impl!();

    fn add(&mut self, input: String) -> Result<(), ReadingError> {
        <Self as ColumnBuilderProxy<String>>::commit(self);
        self.inner.add(Double::new(input.parse()?)?.into())
    }
}

#[cfg(test)]
mod test {
    use nemo_physical::{
        datatypes::storage_value::VecT,
        dictionary::{Dictionary, PrefixedStringDictionary},
    };
    use test_log::test;

    use crate::model::{
        rule_model::{XSD_DECIMAL, XSD_DOUBLE, XSD_INTEGER, XSD_STRING},
        NumericLiteral, RdfLiteral,
    };

    use super::*;

    #[test]
    fn any_parsing() {
        assert_eq!(
            parse_rdf_term_from_string("".to_string()),
            Constant::StringLiteral("".to_string())
        );

        assert_eq!(
            parse_rdf_term_from_string("<http://example.org>".to_string()),
            Constant::Abstract("http://example.org".to_string().into())
        );

        assert_eq!(
            parse_rdf_term_from_string(
                r#""23"^^<http://www.w3.org/2001/XMLSchema#string>"#.to_string()
            ),
            Constant::StringLiteral("23".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string(
                r#""12345"^^<http://www.w3.org/2001/XMLSchema#integer>"#.to_string()
            ),
            Constant::NumericLiteral(NumericLiteral::Integer(12345)),
        );

        assert_eq!(
            parse_rdf_term_from_string(r#""quoted""#.to_string()),
            Constant::StringLiteral("quoted".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("12345".to_string()),
            Constant::NumericLiteral(NumericLiteral::Integer(12345)),
        );

        assert_eq!(
            parse_rdf_term_from_string("_:foo".to_string()),
            Constant::Abstract("_:foo".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("bare_name".to_string()),
            Constant::Abstract("bare_name".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("http://example.org".to_string()),
            Constant::Abstract("http://example.org".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("with space".to_string()),
            Constant::Abstract("with space".to_string().into()),
        );

        assert_eq!(
            parse_rdf_term_from_string("with_question_mark?".to_string()),
            Constant::StringLiteral("with_question_mark?".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("a. a a a".to_string()),
            Constant::StringLiteral("a. a a a".to_string()),
        );

        assert_eq!(
            parse_rdf_term_from_string("<a>. <a> <a> <a>".to_string()),
            Constant::StringLiteral("<a>. <a> <a> <a>".to_string()),
        );
    }

    #[test]
    fn build_columns_from_logical_values() {
        let string = LogicalString::from("my string".to_string());
        let integer = LogicalInteger::from(42);
        let double = LogicalFloat64::from(Double::new(3.41).unwrap());
        let constant = Constant::Abstract("my constant".to_string().into());
        let string_literal = Constant::StringLiteral("string literal".to_string());
        let num_int_literal = Constant::NumericLiteral(NumericLiteral::Integer(45));
        let num_decimal_literal = Constant::NumericLiteral(NumericLiteral::Decimal(4, 2));
        let num_double_literal =
            Constant::NumericLiteral(NumericLiteral::Double(Double::new(2.99).unwrap()));
        let language_string_literal = Constant::try_from(RdfLiteral::LanguageString {
            value: "language string".to_string(),
            tag: "en".to_string(),
        })
        .unwrap();
        let random_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "some random datavalue".to_string(),
            datatype: "a datatype that I totally did not just make up".to_string(),
        })
        .unwrap();
        let string_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "string datavalue".to_string(),
            datatype: XSD_STRING.to_string(),
        })
        .unwrap();
        let integer_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "73".to_string(),
            datatype: XSD_INTEGER.to_string(),
        })
        .unwrap();
        let decimal_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        })
        .unwrap();
        let signed_decimal_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "+1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        })
        .unwrap();
        let negative_decimal_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "-1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        })
        .unwrap();
        let pointless_decimal_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        })
        .unwrap();
        let signed_pointless_decimal_datavalue_literal =
            Constant::try_from(RdfLiteral::DatatypeValue {
                value: "+23".to_string(),
                datatype: XSD_DECIMAL.to_string(),
            })
            .unwrap();
        let negative_pointless_decimal_datavalue_literal =
            Constant::try_from(RdfLiteral::DatatypeValue {
                value: "-23".to_string(),
                datatype: XSD_DECIMAL.to_string(),
            })
            .unwrap();
        let double_datavalue_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "3.33".to_string(),
            datatype: XSD_DOUBLE.to_string(),
        })
        .unwrap();
        let large_integer_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "9950000000000000000".to_string(),
            datatype: XSD_INTEGER.to_string(),
        })
        .unwrap();
        let large_decimal_literal = Constant::try_from(RdfLiteral::DatatypeValue {
            value: "9950000000000000001".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        })
        .unwrap();

        let mut dict = std::cell::RefCell::new(PrefixedStringDictionary::default());

        let physical_builder_for_any_column = PhysicalStringColumnBuilderProxy::new(&dict);
        let physical_builder_for_string_column = PhysicalStringColumnBuilderProxy::new(&dict);
        let physical_builder_for_integer_column =
            PhysicalGenericColumnBuilderProxy::<i64>::default();
        let physical_builder_for_double_column =
            PhysicalGenericColumnBuilderProxy::<Double>::default();

        let mut phys_enum_for_any =
            PhysicalBuilderProxyEnum::String(physical_builder_for_any_column);
        let mut phys_enum_for_string =
            PhysicalBuilderProxyEnum::String(physical_builder_for_string_column);
        let mut phys_enum_for_integer =
            PhysicalBuilderProxyEnum::I64(physical_builder_for_integer_column);
        let mut phys_enum_for_double =
            PhysicalBuilderProxyEnum::Double(physical_builder_for_double_column);

        let mut any_lbp = LogicalAnyColumnBuilderProxy::new(&mut phys_enum_for_any);
        let mut string_lbp = LogicalStringColumnBuilderProxy::new(&mut phys_enum_for_string);
        let mut integer_lbp = LogicalIntegerColumnBuilderProxy::new(&mut phys_enum_for_integer);
        let mut double_lbp = LogicalFloat64ColumnBuilderProxy::new(&mut phys_enum_for_double);

        any_lbp.add(string.clone()).unwrap();
        string_lbp.add(string).unwrap();

        any_lbp.add(integer).unwrap();
        string_lbp.add(integer).unwrap();
        integer_lbp.add(integer).unwrap();

        any_lbp.add(double).unwrap();
        string_lbp.add(double).unwrap();
        double_lbp.add(double).unwrap();

        any_lbp.add(constant).unwrap();

        any_lbp.add(string_literal.clone()).unwrap();
        string_lbp.add(string_literal).unwrap();

        any_lbp.add(num_int_literal.clone()).unwrap();
        integer_lbp.add(num_int_literal).unwrap();

        any_lbp.add(num_decimal_literal).unwrap();

        any_lbp.add(num_double_literal.clone()).unwrap();
        double_lbp.add(num_double_literal).unwrap();

        any_lbp.add(language_string_literal.clone()).unwrap();

        any_lbp.add(random_datavalue_literal).unwrap();

        any_lbp.add(string_datavalue_literal.clone()).unwrap();
        string_lbp.add(string_datavalue_literal).unwrap();

        any_lbp.add(integer_datavalue_literal.clone()).unwrap();
        integer_lbp.add(integer_datavalue_literal).unwrap();

        any_lbp.add(decimal_datavalue_literal).unwrap();
        any_lbp.add(signed_decimal_datavalue_literal).unwrap();
        any_lbp.add(negative_decimal_datavalue_literal).unwrap();
        any_lbp.add(pointless_decimal_datavalue_literal).unwrap();
        any_lbp
            .add(signed_pointless_decimal_datavalue_literal)
            .unwrap();
        any_lbp
            .add(negative_pointless_decimal_datavalue_literal)
            .unwrap();

        any_lbp.add(double_datavalue_literal.clone()).unwrap();
        double_lbp.add(double_datavalue_literal).unwrap();

        any_lbp.add(large_integer_literal).unwrap();
        any_lbp.add(large_decimal_literal).unwrap();

        let VecT::U64(any_result_indices) = phys_enum_for_any.finalize() else {
            unreachable!()
        };
        let VecT::U64(string_result_indices) = phys_enum_for_string.finalize() else {
            unreachable!()
        };

        let any_result: Vec<String> = any_result_indices
            .into_iter()
            .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
            .collect();
        let string_result: Vec<String> = string_result_indices
            .into_iter()
            .map(|idx| dict.get_mut().get(idx.try_into().unwrap()).unwrap())
            .collect();
        let VecT::I64(integer_result) = phys_enum_for_integer.finalize() else {
            unreachable!()
        };
        let VecT::Double(double_result) = phys_enum_for_double.finalize() else {
            unreachable!()
        };

        assert_eq!(
            any_result,
            [
                "ST:my string",
                "IN:42",
                "DO:3.41",
                "CO:my constant",
                "ST:string literal",
                "IN:45",
                "DE:4.2",
                "DO:2.99",
                "LS:language string@en",
                "DV:some random datavalue^^a datatype that I totally did not just make up",
                "ST:string datavalue",
                "IN:73",
                "DE:1.23",
                "DE:1.23",
                "DE:-1.23",
                "DE:23.0",
                "DE:23.0",
                "DE:-23.0",
                "DO:3.33",
                "DV:9950000000000000000^^http://www.w3.org/2001/XMLSchema#integer",
                "DV:9950000000000000001^^http://www.w3.org/2001/XMLSchema#decimal",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );

        assert_eq!(
            string_result,
            [
                "ST:my string",
                "ST:42",
                "ST:3.41",
                "ST:string literal",
                "ST:string datavalue",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );

        assert_eq!(
            integer_result,
            [42, 45, 73,].into_iter().collect::<Vec<i64>>()
        );

        assert_eq!(
            double_result,
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
            .into_iter()
            .collect::<Vec<Double>>()
        );
    }
}
