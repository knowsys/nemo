//! This module contains functions for translating directive ast nodes.

use crate::{parser::ast, rule_model::error::TranslationError};

use super::ASTProgramTranslation;

pub(crate) mod base;
pub(crate) mod declare;
pub(crate) mod import_export;
pub(crate) mod output;
pub(crate) mod prefix;
pub(crate) mod unknown;

impl<'a> ASTProgramTranslation<'a> {
    /// Handle directive nodes that define names.
    pub fn handle_define_directive(
        &mut self,
        directive: &'a ast::directive::Directive,
    ) -> Result<(), TranslationError> {
        match directive {
            ast::directive::Directive::Base(base) => self.handle_base(base),
            ast::directive::Directive::Prefix(prefix) => self.handle_prefix(prefix),
            ast::directive::Directive::Declare(declare) => self.handle_declare(declare),
            ast::directive::Directive::Export(_)
            | ast::directive::Directive::Import(_)
            | ast::directive::Directive::Output(_)
            | ast::directive::Directive::Unknown(_) => Ok(()),
        }
    }

    /// Handle directive nodes that may use defined names.
    pub fn handle_use_directive(
        &mut self,
        directive: &'a ast::directive::Directive,
    ) -> Result<(), TranslationError> {
        match directive {
            ast::directive::Directive::Export(export) => self.handle_export(export),
            ast::directive::Directive::Import(import) => self.handle_import(import),
            ast::directive::Directive::Output(output) => self.handle_output(output),
            ast::directive::Directive::Unknown(unknown) => self.handle_unknown_directive(unknown),
            ast::directive::Directive::Base(_)
            | ast::directive::Directive::Declare(_)
            | ast::directive::Directive::Prefix(_) => Ok(()),
        }
    }
}
