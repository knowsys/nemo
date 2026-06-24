//! This module contains functions for translating directive ast nodes.
use core::panic;
use std::collections::{HashMap, hash_map::Entry};

use oxiri::{Iri, IriRef};
use spargebra::SparqlParser;

use crate::{
    parser::ast::{self},
    rule_model::{
        components::{
            ComponentSource,
            import_export::{ExportDirective, ImportDirective},
            output::Output,
            parameter::ParameterDeclaration,
            tag::Tag,
        },
        error::{info::Info, translation_error::TranslationError},
        origin::Origin,
        programs::ProgramWrite,
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

    // check if base is a valid iri
    if Iri::parse(base.iri().content()).is_err() {
        translation.report.add(base, TranslationError::BaseInvalid);
        // TODO add parse error as context
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
    if (|| {
        let rel_prefix = IriRef::parse(prefix.iri().content())?;
        if rel_prefix.is_absolute() {
            return Ok::<(), Box<dyn std::error::Error>>(());
        } else if let Some(base) = &translation.base {
            Iri::parse(base.0.to_string())
                .expect("base is not a valid IRI")
                .resolve(&rel_prefix)?;
            return Ok::<(), Box<dyn std::error::Error>>(());
        }
        Err(Box::new(TranslationError::PrefixInvalid))
    })()
    .is_err()
    {
        // parsing not successful
        translation
            .report
            .add(prefix.prefix_token(), TranslationError::PrefixInvalid);
    }

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

/// Wrapper for IRI base and prefixes
/// Automatically attempts to resolve all relative prefixes on the base
#[derive(Clone, Debug, Default)]
pub struct FormatContext {
    base: Option<Iri<String>>,
    prefixes: HashMap<String, Iri<String>>,
}

impl FormatContext {
    /// Returns an optional base IRI
    pub fn base(&self) -> &Option<Iri<String>> {
        &self.base
    }

    /// returns a Hashmap of all prefixes and their related IRIs
    pub fn prefixes(&self) -> &HashMap<String, Iri<String>> {
        &self.prefixes
    }

    /// adds a base if None was set before
    /// panics if a base is already set
    pub fn add_base(&mut self, base: String) {
        match self.base {
            None => self.base = Some(Iri::parse(base).expect("not a valid IRI")),
            Some(_) => panic!("attempted to set existing base"),
        }
    }

    /// adds an absolute prefix
    /// if the prefix given is relative attempts to resolve it on the base
    /// panics if
    /// - the iri is invalid
    /// - the prefix cannot be resolved
    /// - a relative prefix is given but no base is set
    pub fn add_prefix(&mut self, prefix: String, iri: String) {
        let rel_prefix = IriRef::parse(iri.clone()).expect("not a valid Iri");
        let abs_prefix = if rel_prefix.is_absolute() {
            Iri::parse(iri.clone()).expect("absolute IRI is not absolute")
        } else {
            if let Some(base) = &self.base {
                base.resolve(&rel_prefix.clone())
                    .expect("relative prefix not resolvable on base")
            } else {
                panic!(
                    "Relative base without prefixes! This should have been cought by the translator!"
                )
            }
        };

        self.prefixes.insert(prefix, abs_prefix);
    }

    /// prepares and returns a SparqlParser with the base and prefixes
    pub fn into_sparql_parser(self) -> SparqlParser {
        let mut parser = SparqlParser::new();

        if let Some(base) = &self.base {
            parser = parser
                .with_base_iri(base.to_string())
                .expect("valid base is invalid");
        }

        for (prefix_name, prefix_iri) in &self.prefixes {
            parser = parser
                .with_prefix(prefix_name, prefix_iri.to_string())
                .expect("valid prefix is invalid");
        }

        parser
    }
}
