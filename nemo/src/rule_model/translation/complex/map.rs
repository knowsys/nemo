//! This module contains a function to create a map term
//! from the corresponding ast node.

use crate::{
    parser::ast,
    rule_model::{
        components::term::{map::Map, Term},
        error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Map {
    type Ast<'a> = ast::expression::complex::map::Map<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        map: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let mut subterms = Vec::new();
        for (key, value) in map.key_value() {
            let key = Term::build_component(translation, key)?;
            let value = Term::build_component(translation, value)?;

            subterms.push((key, value));
        }

        let result = match map.tag() {
            Some(tag) => Map::new(&translation.resolve_tag(tag)?, subterms),
            None => Map::new_unnamed(subterms),
        };

        Ok(translation.register_component(result, map))
    }
}
