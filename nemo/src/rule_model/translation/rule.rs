//! This module contains functions for creating a [Rule] from the corresponding ast node.

use nemo_physical::datavalues::DataValue;

use crate::{
    parser::ast,
    rule_model::{
        components::{
            literal::Literal,
            rule::{Rule, RuleBuilder},
            term::{primitive::Primitive, Term},
        },
        error::TranslationError,
    },
};

use super::{
    attribute::KnownAttributes, literal::HeadAtom, ASTProgramTranslation, TranslationComponent,
};

impl Rule {
    pub(crate) const EXPECTED_ATTRIBUTES: &'static [KnownAttributes] = &[
        KnownAttributes::Name,
        KnownAttributes::Display,
        KnownAttributes::External,
    ];
}

impl TranslationComponent for Rule {
    type Ast<'a> = ast::rule::Rule<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        rule: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let mut rule_builder = RuleBuilder::default().origin(translation.register_node(rule));

        let attributes = translation.statement_attributes();

        if let Some(rule_name) = attributes.get_unique(KnownAttributes::Name) {
            if let Term::Primitive(Primitive::Ground(ground)) = &rule_name[0] {
                if let Some(name) = ground.value().to_plain_string() {
                    rule_builder.name_mut(&name);
                }
            }
        }

        if let Some(rule_display) = attributes.get_unique(KnownAttributes::Display) {
            rule_builder.display_mut(rule_display[0].clone());
        }

        for (variable, expansion) in translation.external_variables() {
            rule_builder.add_external_variable(variable.clone(), expansion.clone());
        }

        for expression in rule.head() {
            rule_builder.add_head_atom_mut(
                HeadAtom::build_component(translation, expression)?.into_inner(),
            );
        }

        for expression in rule.body() {
            rule_builder.add_body_literal_mut(Literal::build_component(translation, expression)?);
        }

        let rule = rule_builder.finalize();

        Ok(rule)
    }
}
