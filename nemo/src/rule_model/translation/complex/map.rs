//! This module contains a function to create a map term
//! from the corresponding ast node.

use crate::{
    parser::ast,
    rule_model::{
        components::{
            tag::Tag,
            term::{Term, map::Map},
        },
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Map {
    type Ast<'a> = ast::expression::complex::map::Map<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        map: &Self::Ast<'a>,
    ) -> Option<Self> {
        let mut subterms = Vec::new();
        for (key, value) in map.key_value() {
            let key = Term::build_component(translation, key)?;
            let value = Term::build_component(translation, value)?;

            subterms.push((key, value));
        }

        let result = match map.tag() {
            Some(tag) => {
                let tag = Origin::ast(Tag::new(translation.resolve_tag(tag)?), tag);
                Map::new_tagged(Some(tag), subterms)
            }
            None => Map::new_unnamed(subterms),
        };

        Some(Origin::ast(result, map))
    }
}
