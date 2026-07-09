//! This module defines [N3Literal].

use nemo_physical::datavalues::{AnyDataValue, DataValueCreationError, syntax::XSD_PREFIX};
use nom::{branch::alt, combinator::map};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            expression::basic::{
                boolean::Boolean,
                number::{Number, NumberValue},
                rdf_literal::RdfLiteral,
                string::StringLiteral,
            },
        },
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::translation::ASTProgramTranslation,
};

/// A Notation3 variable
#[derive(Clone, Debug)]
pub enum N3Literal<'a> {
    /// An RDF literal
    Rdf(RdfLiteral<'a>),
    /// A numeric literal
    Numeric(Number<'a>),
    /// A boolean literal
    Boolean(Boolean<'a>),
    /// A bare string literal
    String(StringLiteral<'a>),
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Literal);

impl N3Literal<'_> {
    /// Return the [ParserContext] of the underlying literal type.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.context(),
            Self::Numeric(number) => number.context(),
            Self::Boolean(boolean) => boolean.context(),
            Self::String(string_literal) => string_literal.context(),
        }
    }

    pub(crate) fn to_any_data_value(
        &self,
        translation: &mut ASTProgramTranslation,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(match self {
            N3Literal::Rdf(rdf_literal) => {
                let lexical_value = rdf_literal.content();
                let datatype_iri = translation
                    .resolve_tag(rdf_literal.tag())
                    .unwrap_or_else(|| rdf_literal.tag().to_string());

                AnyDataValue::new_from_typed_literal(lexical_value, datatype_iri)?
            }
            N3Literal::Numeric(number) => match number.value() {
                NumberValue::Integer(value) => AnyDataValue::new_integer_from_i64(value),
                NumberValue::Float(value) => AnyDataValue::new_float_from_f32(value)?,
                NumberValue::Double(value) => AnyDataValue::new_double_from_f64(value)?,
                NumberValue::Large(value) => {
                    AnyDataValue::new_other(value, format!("{}decimal", XSD_PREFIX))
                }
            },
            N3Literal::Boolean(boolean) => AnyDataValue::new_boolean(bool::from(boolean.value())),
            N3Literal::String(string_literal) => {
                let content = string_literal
                    .content()
                    .replace(r#"\t"#, "\t")
                    .replace(r#"\b"#, "\0u008")
                    .replace(r#"\n"#, "\n")
                    .replace(r#"\r"#, "\r")
                    .replace(r#"\f"#, "\0u00c")
                    .replace(r#"\""#, r#"""#)
                    .replace(r#"\'"#, "'")
                    .replace(r#"\\"#, r#"\"#);

                if let Some(lang_tag) = string_literal.language_tag() {
                    AnyDataValue::new_language_tagged_string(content, lang_tag)
                } else {
                    AnyDataValue::new_plain_string(content)
                }
            }
        })
    }
}

impl<'a> ProgramAST<'a> for N3Literal<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.children(),
            Self::Numeric(number) => number.children(),
            Self::Boolean(boolean) => boolean.children(),
            Self::String(string_literal) => string_literal.children(),
        }
    }

    fn span(&self) -> Span<'a> {
        match self {
            Self::Rdf(rdf_literal) => rdf_literal.span(),
            Self::Numeric(number) => number.span(),
            Self::Boolean(boolean) => boolean.span(),
            Self::String(string_literal) => string_literal.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            CONTEXT,
            alt((
                map(Boolean::parse, Self::Boolean),
                map(Number::parse, Self::Numeric),
                map(RdfLiteral::parse, Self::Rdf),
                map(StringLiteral::parse_n3, Self::String),
            )),
        )(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
