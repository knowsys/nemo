//! This module defines [ASTProgramTranslation].

pub(crate) mod basic;
pub(crate) mod complex;
pub(crate) mod directive;
pub(crate) mod rule;

use std::{collections::HashMap, ops::Range};

use ariadne::{Report, ReportKind, Source};

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{origin::Origin, program::ProgramBuilder},
};

use super::{
    components::{fact::Fact, ProgramComponent},
    error::{
        translation_error::TranslationErrorKind, ProgramError, TranslationError,
        ValidationErrorBuilder,
    },
    program::Program,
};

/// Object for handling the translation of the ast representation
/// of a nemo program into its logical representation
#[derive(Debug)]
pub struct ASTProgramTranslation<'a> {
    /// Original input string
    input: &'a str,
    /// Label of the input file
    input_label: String,

    /// Mapping of [Origin] to [ProgramAST] nodes
    origin_map: HashMap<Origin, &'a dyn ProgramAST<'a>>,

    /// Prefix mapping
    prefix_mapping: HashMap<String, (String, &'a ast::directive::prefix::Prefix<'a>)>,
    /// Base
    base: Option<(String, &'a ast::directive::base::Base<'a>)>,

    /// Builder for the [Program]s
    program_builder: ProgramBuilder,
    /// Builder for validation errors
    validation_error_builder: ValidationErrorBuilder,

    /// Errors
    errors: Vec<ProgramError>,
}

impl<'a> ASTProgramTranslation<'a> {
    /// Initialize the [ASTProgramTranslation]
    pub fn initialize(input: &'a str, input_label: String) -> Self {
        Self {
            input,
            input_label,
            origin_map: HashMap::new(),
            prefix_mapping: HashMap::new(),
            base: None,
            validation_error_builder: ValidationErrorBuilder::default(),
            errors: Vec::default(),
            program_builder: ProgramBuilder::default(),
        }
    }

    /// Register a [ProgramAST] so that it can be associated with and later referenced by
    /// the returned [Origin].
    pub fn register_node(&mut self, node: &'a dyn ProgramAST<'a>) -> Origin {
        let new_origin = Origin::External(self.origin_map.len());
        self.origin_map.insert(new_origin, node);

        new_origin
    }

    /// Register a [ProgramComponent]
    pub fn register_component<Component: ProgramComponent>(
        &mut self,
        component: Component,
        node: &'a dyn ProgramAST<'a>,
    ) -> Component {
        component.set_origin(self.register_node(node))
    }
}

/// Report of all [ProgramError]s occurred
/// during the translation and validation of the AST
#[derive(Debug)]
pub struct ProgramErrorReport<'a> {
    /// Original input string
    input: &'a str,
    /// Label of the input file
    label: String,
    /// Mapping of [Origin] to [ProgramAST] nodes
    origin_map: HashMap<Origin, &'a dyn ProgramAST<'a>>,

    /// Errors
    errors: Vec<ProgramError>,
}

impl<'a> ProgramErrorReport<'a> {
    /// Print the given reports.
    pub fn eprint<'s, ReportIterator>(&self, reports: ReportIterator) -> Result<(), std::io::Error>
    where
        ReportIterator: Iterator<Item = Report<'s, (String, Range<usize>)>>,
    {
        for report in reports {
            report.eprint((self.label.clone(), Source::from(self.input)))?;
        }

        Ok(())
    }

    /// Build a [Report] for each error.
    pub fn build_reports(&self) -> Vec<Report<'_, (String, Range<usize>)>> {
        self.errors
            .iter()
            .map(move |error| {
                let translation = |origin: &Origin| {
                    self.origin_map
                        .get(origin)
                        .expect("map must contain origin")
                        .span()
                        .range()
                        .range()
                };

                let mut report = Report::build(
                    ReportKind::Error,
                    self.label.clone(),
                    error.range(translation).start,
                );

                report = error.report(report, self.label.clone(), translation);

                report.finish()
            })
            .collect()
    }
}

impl<'a> ASTProgramTranslation<'a> {
    /// Translate the given [ProgramAST] into a [Program].
    pub fn translate(
        mut self,
        ast: &'a ast::program::Program<'a>,
    ) -> Result<Program, ProgramErrorReport<'a>> {
        // First, handle all directives
        for statement in ast.statements() {
            if let ast::statement::StatementKind::Directive(directive) = statement.kind() {
                if let Err(error) = self.handle_define_directive(directive) {
                    self.errors.push(ProgramError::TranslationError(error));
                }
            }
        }

        // Now handle facts and rules
        for statement in ast.statements() {
            match statement.kind() {
                ast::statement::StatementKind::Fact(fact) => match self.build_head_atom(fact) {
                    Ok(atom) => self.program_builder.add_fact(Fact::from(atom)),
                    Err(error) => self.errors.push(ProgramError::TranslationError(error)),
                },
                ast::statement::StatementKind::Rule(rule) => match self.build_rule(rule) {
                    Ok(new_rule) => self.program_builder.add_rule(new_rule),
                    Err(translation_error) => self
                        .errors
                        .push(ProgramError::TranslationError(translation_error)),
                },
                ast::statement::StatementKind::Directive(directive) => {
                    if let Err(error) = self.handle_use_directive(directive) {
                        self.errors.push(ProgramError::TranslationError(error));
                    }
                }
                ast::statement::StatementKind::Error(_token) => {
                    todo!("Should faulty statements get ignored?")
                }
            }
        }

        self.errors.extend(
            self.validation_error_builder
                .finalize()
                .into_iter()
                .map(ProgramError::ValidationError),
        );

        if self.errors.is_empty() {
            Ok(self.program_builder.finalize())
        } else {
            Err(ProgramErrorReport {
                input: self.input,
                label: self.input_label,
                errors: self.errors,
                origin_map: self.origin_map,
            })
        }
    }

    /// Recreate the name from a [ast::tag::structure::StructureTag]
    /// by resolving prefixes or bases.
    fn resolve_tag(
        &self,
        tag: &'a ast::tag::structure::StructureTag<'a>,
    ) -> Result<String, TranslationError> {
        Ok(match tag.kind() {
            ast::tag::structure::StructureTagKind::Plain(token) => {
                let token_string = token.to_string();

                if let Some((base, _)) = &self.base {
                    format!("{base}{token_string}")
                } else {
                    token_string
                }
            }
            ast::tag::structure::StructureTagKind::Prefixed { prefix, tag } => {
                if let Some((expanded_prefix, _)) = self.prefix_mapping.get(&prefix.to_string()) {
                    format!("{expanded_prefix}{}", tag.to_string())
                } else {
                    return Err(TranslationError::new(
                        prefix.span(),
                        TranslationErrorKind::PrefixUnknown(prefix.to_string()),
                    ));
                }
            }
            ast::tag::structure::StructureTagKind::Iri(iri) => iri.content(),
        })
    }
}