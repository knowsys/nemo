//! This module contains a function to create a tuple term
//! from the corresponding ast node.

use crate::{
    parser::ast,
    rule_model::{
        components::term::{tuple::Tuple, Term},
        error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Tuple {
    type Ast<'a> = ast::expression::complex::tuple::Tuple<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        tuple: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let mut subterms = Vec::new();
        for expression in tuple.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Ok(translation.register_component(Tuple::new(subterms), tuple))
    }
}
