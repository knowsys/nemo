//! This module contains a function to create an operation term
//! from the corresponding ast node.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::term::{operation::Operation, Term},
        error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct FunctionLikeOperation(Operation);

impl FunctionLikeOperation {
    pub(crate) fn into_inner(self) -> Operation {
        self.0
    }
}

impl TranslationComponent for FunctionLikeOperation {
    type Ast<'a> = ast::expression::complex::operation::Operation<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        operation: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let kind = operation.kind();
        let mut subterms = Vec::new();
        for expression in operation.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Ok(FunctionLikeOperation(translation.register_component(
            Operation::new(kind, subterms),
            operation,
        )))
    }
}
