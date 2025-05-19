//! This module contains a function to create a function term
//! from the corresponding ast node.

use crate::rule_model::components::tag::Tag;
use crate::rule_model::components::term::function::FunctionTerm;
use crate::rule_model::components::term::Term;
use crate::rule_model::origin::Origin;
use crate::rule_model::translation::TranslationComponent;
use crate::{
    parser::ast::{self},
    rule_model::translation::ASTProgramTranslation,
};

impl TranslationComponent for FunctionTerm {
    type Ast<'a> = ast::expression::complex::atom::Atom<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        function: &Self::Ast<'a>,
    ) -> Option<Self> {
        let tag = Origin::ast(
            Tag::from(translation.resolve_tag(function.tag())?),
            function.tag(),
        );

        let mut subterms = Vec::new();
        for expression in function.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Some(Origin::ast(
            FunctionTerm::new_tagged(tag, subterms),
            function,
        ))
    }
}
