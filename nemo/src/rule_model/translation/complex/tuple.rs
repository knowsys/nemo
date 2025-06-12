//! This module contains a function to create a tuple term
//! from the corresponding ast node.

use crate::{
    parser::ast,
    rule_model::{
        components::term::{tuple::Tuple, Term},
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Tuple {
    type Ast<'a> = ast::expression::complex::tuple::Tuple<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        tuple: &Self::Ast<'a>,
    ) -> Option<Self> {
        let mut subterms = Vec::new();
        for expression in tuple.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Some(Origin::ast(Tuple::new(subterms), tuple))
    }
}
