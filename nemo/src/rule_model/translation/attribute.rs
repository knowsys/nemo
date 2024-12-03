//! This module contains functions
//! for processing [Attribute][crate::parser::ast::attribute::Attribute]s.

use std::collections::HashMap;

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::{
    parser::{
        ast::{self, ProgramAST},
        span::Span,
    },
    rule_model::{
        components::{
            term::{value_type::ValueType, Term},
            ProgramComponent, ProgramComponentKind,
        },
        error::{
            hint::Hint, info::Info, translation_error::TranslationErrorKind, ComplexErrorLabelKind,
            TranslationError,
        },
    },
};

use super::ASTProgramTranslation;

/// All recognized [Attribute][crate::parser::ast::attribute::Attribute]s
#[derive(Assoc, EnumIter, Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
#[func(pub fn schema(&self) -> Vec<(Option<ProgramComponentKind>, Option<ValueType>)>)]
pub(crate) enum KnownAttributes {
    /// Name associated with a component
    #[assoc(name = "name")]
    #[assoc(from_name = "name")]
    #[assoc(schema = vec![(Some(ProgramComponentKind::PlainString), None)])]
    Name,
    /// User-defined way of displaying the component
    #[assoc(name = "display")]
    #[assoc(from_name = "display")]
    #[assoc(schema = vec![(None, Some(ValueType::String))])]
    Display,
}

impl<'a> ASTProgramTranslation<'a> {
    /// Evaluates a list of attributes, checking for errors,
    /// and returns a map from an attribute to a list of parameters.
    pub(crate) fn process_attributes(
        &mut self,
        attributes: impl Iterator<Item = &'a ast::attribute::Attribute<'a>>,
        expected: &[KnownAttributes],
    ) -> Result<HashMap<KnownAttributes, Vec<Term>>, TranslationError> {
        let mut result = HashMap::new();
        let mut previous_attributes = HashMap::<KnownAttributes, Span<'a>>::new();

        for attribute in attributes {
            let tag = attribute.content().tag();
            let name = tag.to_string();

            if let Some(attribute_kind) = KnownAttributes::from_name(&name) {
                if !expected.contains(&attribute_kind) {
                    return Err(TranslationError::new(
                        tag.span(),
                        TranslationErrorKind::AttributeUnexpected(name.clone()),
                    ));
                }

                if let Some(previous_span) = previous_attributes.get(&attribute_kind) {
                    let error = TranslationError::new(
                        attribute.span(),
                        TranslationErrorKind::AttributeRedefined,
                    )
                    .add_label(
                        ComplexErrorLabelKind::Information,
                        previous_span.range(),
                        Info::FirstDefinition,
                    );

                    return Err(error);
                }

                let mut terms = Vec::<Term>::new();

                let count_expected = attribute_kind.schema().len();
                let count_found = attribute.content().expressions().count();
                if count_found != count_expected {
                    return Err(TranslationError::new(
                        attribute.span(),
                        TranslationErrorKind::AttributeInvalidParameterCount {
                            expected: count_expected,
                            found: count_found,
                        },
                    ));
                }

                for (expression, schema) in attribute
                    .content()
                    .expressions()
                    .zip(attribute_kind.schema())
                {
                    let term = self.build_inner_term(expression)?;

                    if let Some(expected_component) = schema.0 {
                        if term.kind() != expected_component {
                            return Err(TranslationError::new(
                                expression.span(),
                                TranslationErrorKind::AttributeParameterWrongComponent {
                                    expected: expected_component.name().to_string(),
                                    found: term.kind().name().to_string(),
                                },
                            ));
                        }
                    }

                    if let Some(expected_type) = schema.1 {
                        if term.value_type() != expected_type {
                            return Err(TranslationError::new(
                                expression.span(),
                                TranslationErrorKind::AttributeParameterWrongType {
                                    expected: expected_type.name().to_string(),
                                    found: term.value_type().name().to_string(),
                                },
                            ));
                        }
                    }

                    terms.push(term);
                }

                result.insert(attribute_kind, terms);
                previous_attributes.insert(attribute_kind, attribute.span());
            } else {
                let mut error = TranslationError::new(
                    tag.span(),
                    TranslationErrorKind::AttributeUnknown(name.clone()),
                );
                error.add_hint_option(Hint::similar_attribute(&name));

                return Err(error);
            }
        }

        Ok(result)
    }
}
