//! This module contains functions for translating directive ast nodes.

use std::collections::hash_map::Entry;

use import_export::{handle_export, handle_import};

use crate::{
    parser::ast::{self, expression::Expression, ProgramAST},
    rule_model::{
        components::{atom::Atom, order::Order, tag::Tag, term::operation::Operation},
        error::{
            info::Info, translation_error::TranslationErrorKind, ComplexErrorLabelKind,
            TranslationError,
        },
    },
};

use super::{
    complex::{infix::InfixOperation, operation::FunctionLikeOperation},
    ASTProgramTranslation, TranslationComponent,
};

pub(crate) mod import_export;

/// Handle directive nodes that do not use names defined elsewhere.
pub fn handle_define_directive<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    directive: &'b ast::directive::Directive<'a>,
) -> Result<(), TranslationError> {
    match directive {
        ast::directive::Directive::Base(base) => handle_base(translation, base),
        ast::directive::Directive::Prefix(prefix) => handle_prefix(translation, prefix),
        ast::directive::Directive::Declare(declare) => handle_declare(translation, declare),
        ast::directive::Directive::Export(_)
        | ast::directive::Directive::Import(_)
        | ast::directive::Directive::Order(_)
        | ast::directive::Directive::Output(_)
        | ast::directive::Directive::Unknown(_) => Ok(()),
    }
}

/// Handle directive nodes that may use defined names.
pub fn handle_use_directive<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    directive: &'b ast::directive::Directive<'a>,
) -> Result<(), TranslationError> {
    match directive {
        ast::directive::Directive::Export(export) => handle_export(translation, export),
        ast::directive::Directive::Import(import) => handle_import(translation, import),
        ast::directive::Directive::Order(order) => handle_order(translation, order),
        ast::directive::Directive::Output(output) => handle_output(translation, output),
        ast::directive::Directive::Unknown(unknown) => handle_unknown_directive(unknown),
        ast::directive::Directive::Base(_)
        | ast::directive::Directive::Declare(_)
        | ast::directive::Directive::Prefix(_) => Ok(()),
    }
}

fn handle_unknown_directive(
    directive: &ast::directive::unknown::UnknownDirective,
) -> Result<(), TranslationError> {
    Err(TranslationError::new(
        directive.name_token().span(),
        TranslationErrorKind::DirectiveUnknown(directive.name()),
    ))
}

fn handle_base<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    base: &'b ast::directive::base::Base<'a>,
) -> Result<(), TranslationError> {
    if let Some((_, first_base)) = &translation.base {
        return Err(
            TranslationError::new(base.span(), TranslationErrorKind::BaseRedefinition).add_label(
                ComplexErrorLabelKind::Information,
                first_base.span().range(),
                Info::FirstDefinition,
            ),
        );
    }

    translation.base = Some((base.iri().content(), base));
    Ok(())
}

fn handle_declare<'a, 'b>(
    _translation: &mut ASTProgramTranslation<'a, 'b>,
    declare: &'b ast::directive::declare::Declare<'a>,
) -> Result<(), TranslationError> {
    Err(TranslationError::new(
        declare.span(),
        TranslationErrorKind::UnsupportedDeclare,
    ))
}

// TODO: This should be a translation component
fn build_guard<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    ast: &'b ast::guard::Guard<'a>,
) -> Result<Operation, TranslationError> {
    Ok(match ast {
        ast::guard::Guard::Expression(expression) => {
            if let Expression::Operation(operation) = expression {
                Operation::from(FunctionLikeOperation::build_component(
                    translation,
                    operation,
                )?)
            } else {
                todo!()
            }
        }
        ast::guard::Guard::Infix(infix_expression) => Operation::from(
            InfixOperation::build_component(translation, infix_expression)?,
        ),
    })
}

fn handle_order<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    order: &'b ast::directive::order::Order<'a>,
) -> Result<(), TranslationError> {
    let tag = Tag::new(translation.resolve_tag(order.atom_left().tag())?);
    let dominating = Atom::build_component(translation, order.atom_left())?;
    let dominated = Atom::build_component(translation, order.atom_right())?;

    let condition = order
        .condition()
        .map(|condition| build_guard(translation, condition))
        .collect::<Result<Vec<_>, TranslationError>>()?;

    translation
        .program_builder
        .add_order(Order::new(tag, dominating, dominated, condition));

    Ok(())
}

fn handle_output<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    output: &'b ast::directive::output::Output<'a>,
) -> Result<(), TranslationError> {
    for predicate in output.predicates() {
        let tag = Tag::new(translation.resolve_tag(predicate)?)
            .set_origin(translation.register_node(predicate));

        translation
            .program_builder
            .add_output(crate::rule_model::components::output::Output::new(tag));
    }

    Ok(())
}

fn handle_prefix<'a, 'b>(
    translation: &mut ASTProgramTranslation<'a, 'b>,
    prefix: &'b ast::directive::prefix::Prefix<'a>,
) -> Result<(), TranslationError> {
    match translation.prefix_mapping.entry(prefix.prefix()) {
        Entry::Occupied(entry) => {
            let (_, prefix_first) = entry.get();
            return Err(TranslationError::new(
                prefix.prefix_token().span(),
                TranslationErrorKind::PrefixRedefinition,
            )
            .add_label(
                ComplexErrorLabelKind::Information,
                prefix_first.prefix_token().span().range(),
                Info::FirstDefinition,
            ));
        }
        Entry::Vacant(entry) => {
            entry.insert((prefix.iri().content(), prefix));
        }
    }

    Ok(())
}
