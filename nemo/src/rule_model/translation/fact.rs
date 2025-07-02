//! This module contains functions for translating ast nodes into facts.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::{fact::Fact, tag::Tag, term::Term},
        error::translation_error::TranslationError,
        origin::Origin,
    },
};

use super::TranslationComponent;

impl TranslationComponent for Fact {
    type Ast<'a> = ast::guard::Guard<'a>;

    fn build_component<'a>(
        translation: &mut super::ASTProgramTranslation,
        fact: &Self::Ast<'a>,
    ) -> Option<Self> {
        if let ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) = fact {
            let predicate =
                Origin::ast(Tag::from(translation.resolve_tag(atom.tag())?), atom.tag());

            let mut subterms = Vec::new();
            for expression in atom.expressions() {
                subterms.push(Term::build_component(translation, expression)?);
            }

            Some(Origin::ast(Fact::new(predicate, subterms), atom))
        } else {
            translation.report.add(
                fact,
                TranslationError::ExpressionAsFact {
                    found: fact.context_type().name().to_string(),
                },
            );

            None
        }
    }
}
