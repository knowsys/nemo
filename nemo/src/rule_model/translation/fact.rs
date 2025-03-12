//! This module contains functions for translating ast nodes into facts.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{fact::Fact, tag::Tag, term::Term},
        error::{translation_error::TranslationErrorKind, TranslationError},
        substitution::Substitution,
    },
};

use super::TranslationComponent;

impl TranslationComponent for Fact {
    type Ast<'a> = ast::guard::Guard<'a>;

    fn build_component<'a, 'b>(
        translation: &mut super::ASTProgramTranslation<'a, 'b>,
        fact: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let mut substitution = Substitution::default();

        for (variable, expansion) in translation.external_variables() {
            substitution.insert(variable.clone(), expansion.clone());
        }

        let fact =
            if let ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) = fact {
                let predicate = Tag::from(translation.resolve_tag(atom.tag())?)
                    .set_origin(translation.register_node(atom.tag()));
                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    let mut term = Term::build_component(translation, expression)?;
                    substitution.apply(&mut term);
                    subterms.push(term);
                }

                translation.register_component(Fact::new(predicate, subterms), atom)
            } else {
                return Err(TranslationError::new(
                    fact.span(),
                    TranslationErrorKind::ExpressionAsFact {
                        found: fact.context_type().name().to_string(),
                    },
                ));
            };

        Ok(fact)
    }
}
