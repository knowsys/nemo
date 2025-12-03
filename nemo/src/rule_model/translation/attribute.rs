//! This module contains functions
//! for processing [Attribute][crate::parser::ast::attribute::Attribute]s.

use std::collections::HashMap;

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::{
    parser::ast::{self},
    rule_model::{
        components::{
            ComponentBehavior, ProgramComponentKind,
            term::{Term, value_type::ValueType},
        },
        error::{hint::Hint, info::Info, translation_error::TranslationError},
    },
    util::bag::Bag,
};

use super::{ASTProgramTranslation, TranslationComponent};

/// All recognized [Attribute][crate::parser::ast::attribute::Attribute]s
#[derive(Assoc, EnumIter, Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
#[func(pub fn unique(&self) -> bool)]
#[func(pub fn schema(&self) -> Vec<(Option<ProgramComponentKind>, Option<ValueType>)>)]
pub(crate) enum KnownAttributes {
    /// Name associated with a component
    #[assoc(name = "name")]
    #[assoc(from_name = "name")]
    #[assoc(unique = true)]
    #[assoc(schema = vec![(Some(ProgramComponentKind::PlainString), None)])]
    Name,
    /// User-defined way of displaying the component
    #[assoc(name = "display")]
    #[assoc(from_name = "display")]
    #[assoc(unique = true)]
    #[assoc(schema = vec![(None, Some(ValueType::String))])]
    Display,
}

/// Evaluates a list of attributes, checking for errors,
/// and returns a map from an attribute to a list of parameters.
pub(crate) fn process_attributes<'a>(
    translation: &mut ASTProgramTranslation,
    attributes: impl Iterator<Item = &'a ast::attribute::Attribute<'a>>,
    expected: &[KnownAttributes],
) -> Option<Bag<KnownAttributes, Vec<Term>>> {
    let mut result = Bag::default();
    let mut previous_attributes =
        HashMap::<KnownAttributes, &'a ast::attribute::Attribute<'a>>::new();

    for attribute in attributes {
        let tag = attribute.content().tag();
        let name = tag.to_string();

        let Some(attribute_kind) = KnownAttributes::from_name(&name) else {
            translation
                .report
                .add(
                    tag,
                    TranslationError::AttributeUnknown {
                        attribute: name.clone(),
                    },
                )
                .add_hint_option(Hint::similar_attribute(&name));

            continue;
        };

        if !expected.contains(&attribute_kind) {
            translation.report.add(
                tag,
                TranslationError::AttributeUnexpected {
                    attribute: name.clone(),
                },
            );

            continue;
        }

        if attribute_kind.unique()
            && let Some(previous) = previous_attributes.get(&attribute_kind)
        {
            translation
                .report
                .add(attribute, TranslationError::AttributeRedefined)
                .add_context(*previous, Info::FirstDefinition);

            continue;
        }

        let mut terms = Vec::<Term>::new();

        let count_expected = attribute_kind.schema().len();
        let count_found = attribute.content().expressions().count();
        if count_found != count_expected {
            translation.report.add(
                attribute,
                TranslationError::AttributeInvalidParameterCount {
                    expected: count_expected,
                    found: count_found,
                },
            );
        }

        for (expression, schema) in attribute
            .content()
            .expressions()
            .zip(attribute_kind.schema())
        {
            let term = Term::build_component(translation, expression)?;

            if let Some(expected_component) = schema.0
                && term.kind() != expected_component
            {
                translation.report.add(
                    expression,
                    TranslationError::AttributeParameterWrongComponent {
                        expected: expected_component.name().to_string(),
                        found: term.kind().name().to_string(),
                    },
                );

                continue;
            }

            if let Some(expected_type) = schema.1
                && term.value_type() != expected_type
            {
                translation.report.add(
                    expression,
                    TranslationError::AttributeParameterWrongType {
                        expected: expected_type.name().to_string(),
                        found: term.value_type().name().to_string(),
                    },
                );
            }

            terms.push(term);
        }

        result.get_mut(attribute_kind).push(terms);
        previous_attributes.insert(attribute_kind, attribute);
    }

    Some(result)
}
