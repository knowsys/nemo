//! This module contains functions for translating directive ast nodes.

use std::collections::hash_map::Entry;

use crate::{
    parser::ast::{self},
    rule_model::{
        components::{
            import_export::{ExportDirective, ImportDirective},
            output::Output,
            parameter::ParameterDeclaration,
            tag::Tag,
            ComponentSource,
        },
        error::{info::Info, translation_error::TranslationError},
        origin::Origin,
        program::ProgramWrite,
    },
};

use super::{ASTProgramTranslation, TranslationComponent};

pub(crate) mod import_export;
pub(crate) mod parmater_declaration;

/// Handle directive nodes that do not use names defined elsewhere.
pub fn handle_define_directive<'a>(
    translation: &mut ASTProgramTranslation,
    directive: &ast::directive::Directive<'a>,
) {
    match directive {
        ast::directive::Directive::Base(base) => handle_base(translation, base),
        ast::directive::Directive::Prefix(prefix) => handle_prefix(translation, prefix),
        ast::directive::Directive::Declare(declare) => handle_declare(translation, declare),
        ast::directive::Directive::Export(_)
        | ast::directive::Directive::Parameter(_)
        | ast::directive::Directive::Import(_)
        | ast::directive::Directive::Output(_)
        | ast::directive::Directive::Unknown(_) => {}
    }
}

/// Handle directive nodes that may use defined names.
pub fn handle_use_directive<'a, Writer: ProgramWrite>(
    translation: &mut ASTProgramTranslation,
    directive: &ast::directive::Directive<'a>,
    program: &mut Writer,
) {
    match directive {
        ast::directive::Directive::Export(export) => {
            if let Some(export_directive) = ExportDirective::build_component(translation, export) {
                program.add_export(export_directive);
            }
        }
        ast::directive::Directive::Import(import) => {
            if let Some(import_directive) = ImportDirective::build_component(translation, import) {
                program.add_import(import_directive);
            }
        }
        ast::directive::Directive::Output(output) => handle_output(translation, output, program),
        ast::directive::Directive::Unknown(unknown) => {
            handle_unknown_directive(translation, unknown)
        }
        ast::directive::Directive::Base(_)
        | ast::directive::Directive::Declare(_)
        | ast::directive::Directive::Prefix(_) => {}
        ast::directive::Directive::Parameter(parameter) => {
            if let Some(parameter_declaration) =
                ParameterDeclaration::build_component(translation, parameter)
            {
                program.add_parameter_declaration(parameter_declaration);
            }
        }
    }
}

/// Handle unknown directives.
fn handle_unknown_directive(
    translation: &mut ASTProgramTranslation,
    directive: &ast::directive::unknown::UnknownDirective,
) {
    translation.report.add(
        directive,
        TranslationError::DirectiveUnknown {
            directive: directive.name(),
        },
    );
}

/// Handle base directive.
fn handle_base<'a>(translation: &mut ASTProgramTranslation, base: &ast::directive::base::Base<'a>) {
    if let Some((_, first_base)) = &translation.base {
        translation
            .report
            .add(base, TranslationError::BaseRedefinition)
            .add_context_source(first_base.clone(), Info::FirstDefinition);
    }

    translation.base = Some((base.iri().content(), base.origin()));
}

/// Handle declare directive.
fn handle_declare<'a>(
    translation: &mut ASTProgramTranslation,
    declare: &ast::directive::declare::Declare<'a>,
) {
    translation
        .report
        .add(declare, TranslationError::UnsupportedDeclare);
}

/// Handle output directives.
fn handle_output<'a, Writer: ProgramWrite>(
    translation: &mut ASTProgramTranslation,
    output: &ast::directive::output::Output<'a>,
    program: &mut Writer,
) {
    for predicate in output.predicates() {
        if let Some(tag) = translation.resolve_tag(predicate) {
            program.add_output(Output::new(Origin::ast(Tag::new(tag), predicate)));
        }
    }
}

/// Handle prefix directives.
fn handle_prefix<'a>(
    translation: &mut ASTProgramTranslation,
    prefix: &ast::directive::prefix::Prefix<'a>,
) {
    match translation.prefix_mapping.entry(prefix.prefix()) {
        Entry::Occupied(entry) => {
            let (_, prefix_first) = entry.get();

            translation
                .report
                .add(prefix.prefix_token(), TranslationError::PrefixRedefinition)
                .add_context_source(prefix_first.clone(), Info::FirstDefinition);
        }
        Entry::Vacant(entry) => {
            entry.insert((prefix.iri().content(), prefix.origin()));
        }
    }
}
