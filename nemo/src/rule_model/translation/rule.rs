//! This module contains functions for creating a [Rule] from the corresponding ast node.

use nemo_physical::datavalues::DataValue;

use crate::{
    parser::ast,
    rule_model::{
        components::{
            literal::Literal,
            rule::Rule,
            term::{primitive::Primitive, Term},
        },
        origin::Origin,
    },
};

use super::{
    attribute::KnownAttributes, literal::HeadAtom, ASTProgramTranslation, TranslationComponent,
};

impl Rule {
    pub(crate) const EXPECTED_ATTRIBUTES: &'static [KnownAttributes] =
        &[KnownAttributes::Name, KnownAttributes::Display];
}

impl TranslationComponent for Rule {
    type Ast<'a> = ast::rule::Rule<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        rule: &Self::Ast<'a>,
    ) -> Option<Self> {
        let mut result = Origin::ast(Rule::empty(), rule);
        let attributes = translation.statement_attributes();

        if let Some(rule_name) = attributes.get_unique(&KnownAttributes::Name) {
            if let Term::Primitive(Primitive::Ground(ground)) = &rule_name[0] {
                if let Some(name) = ground.value().to_plain_string() {
                    result.set_name(&name);
                }
            }
        }

        if let Some(rule_display) = attributes.get_unique(&KnownAttributes::Display) {
            result.set_display(rule_display[0].clone());
        }

        for expression in rule.head() {
            result
                .head_mut()
                .push(HeadAtom::build_component(translation, expression)?.into_inner());
        }

        for expression in rule.body() {
            result
                .body_mut()
                .push(Literal::build_component(translation, expression)?);
        }

        Some(result)
    }
}
