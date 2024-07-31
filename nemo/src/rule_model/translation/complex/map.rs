//! This module contains a function to create a map term
//! from the corresponding ast node.

use crate::parser::ast;

use crate::rule_model::components::term::map::Map;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a map term from the corresponding AST node.
    pub(crate) fn build_map(
        &mut self,
        map: &'a ast::expression::complex::map::Map,
    ) -> Result<Map, TranslationError> {
        let mut subterms = Vec::new();
        for (key, value) in map.key_value() {
            let key = self.build_inner_term(key)?;
            let value = self.build_inner_term(value)?;

            subterms.push((key, value));
        }

        Ok(match map.tag() {
            Some(tag) => Map::new(&self.resolve_tag(tag)?, subterms),
            None => Map::new_unnamed(subterms),
        })
    }
}
