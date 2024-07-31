//! This module contains a function to obtain a variable from the corresponding AST node.

use crate::parser::ast::{self, ProgramAST};

use crate::rule_model::components::term::primitive::variable::Variable;
use crate::rule_model::components::ProgramComponent;
use crate::rule_model::error::hint::Hint;
use crate::rule_model::error::translation_error::TranslationErrorKind;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a variable term from the corresponding AST node.
    pub(crate) fn build_variable(
        &mut self,
        variable: &'a ast::expression::basic::variable::Variable,
    ) -> Result<Variable, TranslationError> {
        Ok(match variable.kind() {
            ast::expression::basic::variable::VariableType::Universal => {
                if let Some(variable_name) = variable.name() {
                    Variable::universal(&variable_name).set_origin(self.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::UnnamedVariable,
                    )
                    .add_hint(Hint::AnonymousVariables));
                }
            }
            ast::expression::basic::variable::VariableType::Existential => {
                if let Some(variable_name) = variable.name() {
                    Variable::existential(&variable_name).set_origin(self.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::UnnamedVariable,
                    ));
                }
            }
            ast::expression::basic::variable::VariableType::Anonymous => {
                if variable.name().is_none() {
                    Variable::anonymous().set_origin(self.register_node(variable))
                } else {
                    return Err(TranslationError::new(
                        variable.span(),
                        TranslationErrorKind::NamedAnonymous(variable.span().0.to_string()),
                    ));
                }
            }
        })
    }
}
