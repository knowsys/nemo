//! This module contains a function to create a function term
//! from the corresponding ast node.

use crate::rule_model::components::tag::Tag;
use crate::rule_model::components::term::function::FunctionTerm;
use crate::rule_model::components::term::Term;
use crate::rule_model::translation::TranslationComponent;
use crate::{
    parser::ast::{self},
    rule_model::{error::TranslationError, translation::ASTProgramTranslation},
};

impl TranslationComponent for FunctionTerm {
    type Ast<'a> = ast::expression::complex::atom::Atom<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        function: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let tag = Tag::from(translation.resolve_tag(function.tag())?)
            .set_origin(translation.register_node(function.tag()));

        let mut subterms = Vec::new();
        for expression in function.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Ok(translation.register_component(FunctionTerm::new_tag(tag, subterms), function))
    }
}
