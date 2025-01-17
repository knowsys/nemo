//! This module contains a function to create an operation term
//! from the corresponding ast node.

use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{operation::Operation, Term},
        error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct FunctionLikeOperation(Operation);
newtype_wrapper!(FunctionLikeOperation: Operation);

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
