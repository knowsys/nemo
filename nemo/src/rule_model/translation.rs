//! This module defines [ASTProgramTranslation].

pub(crate) mod attribute;
pub(crate) mod basic;
pub(crate) mod complex;
pub(crate) mod directive;
pub(crate) mod fact;
pub(crate) mod literal;
pub(crate) mod rule;
mod term;

use std::{collections::HashMap, ops::Range};

use ariadne::{Report, ReportKind, Source};
use attribute::{process_attributes, Bag, KnownAttributes};
use directive::{handle_define_directive, handle_use_directive};
use nom::InputLength;

use crate::{
    parser::{
        ast::{self, ProgramAST},
        input::ParserInput,
        ParserState,
    },
    rule_model::{origin::Origin, program::ProgramBuilder},
};

use super::{
    components::{
        fact::Fact,
        rule::Rule,
        term::{
            primitive::{
                ground::GroundTerm,
                variable::{universal::UniversalVariable, Variable},
                Primitive,
            },
            Term,
        },
        ProgramComponent,
    },
    error::{
        translation_error::TranslationErrorKind, ComponentParseError, ProgramError,
        TranslationError, ValidationErrorBuilder,
    },
    program::Program,
};

/// Object for handling the translation of the ast representation
/// of a nemo program into its logical representation
#[derive(Debug, Default)]
pub struct ASTProgramTranslation<'a, 'b> {
    /// Original input string
    input: &'a str,
    /// Label of the input file
    input_label: String,

    /// Mapping of [Origin] to [ProgramAST] nodes
    origin_map: HashMap<Origin, &'b dyn ProgramAST<'a>>,

    /// Prefix mapping
    prefix_mapping: HashMap<String, (String, &'b ast::directive::prefix::Prefix<'a>)>,
    /// Base
    base: Option<(String, &'b ast::directive::base::Base<'a>)>,

    /// Builder for the [Program]s
    program_builder: ProgramBuilder,
    /// Builder for validation errors
    validation_error_builder: ValidationErrorBuilder,
    /// Parameter expansions supplied externally (e.g. via the --param cli option)
    external_parameters: HashMap<String, GroundTerm>,
    /// Attributes for the statement currently being translated
    statement_attributes: Bag<KnownAttributes, Vec<Term>>,

    /// Errors
    errors: Vec<ProgramError>,
}

impl<'a, 'b> ASTProgramTranslation<'a, 'b> {
    /// Initialize the [ASTProgramTranslation]
    pub fn initialize(input: &'a str, input_label: String) -> Self {
        Self {
            input,
            input_label,
            ..Default::default()
        }
    }

    /// Add a value for an external parameter, that will be expanded during translation
    pub fn add_parameter(&mut self, key: String, value: GroundTerm) {
        self.external_parameters.insert(key, value);
    }

    /// Register a [ProgramAST] so that it can be associated with and later referenced by
    /// the returned [Origin].
    pub fn register_node(&mut self, node: &'b dyn ProgramAST<'a>) -> Origin {
        let new_origin = Origin::External(self.origin_map.len());
        self.origin_map.insert(new_origin, node);

        new_origin
    }

    /// Register a [ProgramComponent]
    pub fn register_component<Component: ProgramComponent>(
        &mut self,
        component: Component,
        node: &'b dyn ProgramAST<'a>,
    ) -> Component {
        component.set_origin(self.register_node(node))
    }
}

/// Report of all [ProgramError]s occurred
/// during the translation and validation of the AST
pub struct ProgramErrorReport<'a, 'b> {
    /// Original input string
    input: &'a str,
    /// Label of the input file::Program
    label: String,
    /// Mapping of [Origin] to [ProgramAST] nodes
    origin_map: HashMap<Origin, &'b dyn ProgramAST<'a>>,

    /// Errors
    errors: Vec<ProgramError>,
}

impl<'a, 'b> ProgramErrorReport<'a, 'b> {
    /// Print this report to standard error.
    pub fn eprint(&self) -> Result<(), std::io::Error> {
        let reports = self.build_reports();

        for report in reports {
            report.eprint((self.label.clone(), Source::from(self.input)))?;
        }

        Ok(())
    }

    /// Write this report to a given writer.
    pub fn write(&self, writer: &mut impl std::io::Write) -> Result<(), std::io::Error> {
        let reports = self.build_reports();

        for report in reports {
            report.write((self.label.clone(), Source::from(self.input)), &mut *writer)?
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

    /// Return the mapping from origins to AST nodes.
    pub fn origin_map(&self) -> &HashMap<Origin, &'b dyn ProgramAST<'a>> {
        &self.origin_map
    }

    /// Return raw [ProgramError]s.
    pub fn errors(&self) -> &Vec<ProgramError> {
        &self.errors
    }
}

impl std::fmt::Debug for ProgramErrorReport<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reports = self.build_reports();

        for report in reports {
            report.fmt(f)?
        }

        Ok(())
    }
}

impl std::fmt::Display for ProgramErrorReport<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buffer = Vec::new();
        if self.write(&mut buffer).is_err() {
            return Err(std::fmt::Error);
        }

        write!(f, "{}", String::from_utf8(buffer).expect("invalid string"))
    }
}

impl<'a, 'b> ASTProgramTranslation<'a, 'b> {
    fn process_attributes(&mut self, statement: &'b ast::statement::Statement<'a>) {
        let expected_attributes = match statement.kind() {
            ast::statement::StatementKind::Fact(_) => &[KnownAttributes::External],
            ast::statement::StatementKind::Rule(_) => Rule::EXPECTED_ATTRIBUTES,
            ast::statement::StatementKind::Directive(_) => &[KnownAttributes::External],
            ast::statement::StatementKind::Error(_) => &[],
        };

        match process_attributes(self, statement.attributes().iter(), expected_attributes) {
            Ok(attributes) => self.statement_attributes = attributes,
            Err(error) => self.errors.push(ProgramError::TranslationError(error)),
        }
    }

    pub(crate) fn external_variables(
        &self,
    ) -> impl Iterator<Item = Result<(&UniversalVariable, &GroundTerm), TranslationError>> + use<'a, '_>
    {
        self.statement_attributes
            .get(KnownAttributes::External)
            .iter()
            .map(|external_term| {
                let Term::Primitive(Primitive::Variable(variable)) = &external_term[0] else {
                    unreachable!("checked in process_attributes()")
                };

                let span = self
                    .origin_map
                    .get(variable.origin())
                    .map(|ast| ast.span())
                    .expect("should be part of the origin map");

                let Variable::Universal(variable) = variable else {
                    return Err(TranslationError::new(
                        span,
                        TranslationErrorKind::ExternalVariableAttribute,
                    ));
                };

                let Some(variable_name) = variable.name() else {
                    return Err(TranslationError::new(
                        span,
                        TranslationErrorKind::ExternalVariableAttribute,
                    ));
                };

                let Some(expansion) = self.external_parameters.get(&variable_name) else {
                    return Err(TranslationError::new(
                        span,
                        TranslationErrorKind::MissingExternalVariable,
                    ));
                };

                Ok((variable, expansion))
            })
    }

    /// Translate the given [ProgramAST] into a [Program].
    pub fn translate(
        mut self,
        ast: &'b ast::program::Program<'a>,
    ) -> Result<Program, ProgramErrorReport<'a, 'b>> {
        // First, handle definitions
        for statement in ast.statements() {
            if let ast::statement::StatementKind::Directive(directive) = statement.kind() {
                if let Err(error) = handle_define_directive(&mut self, directive) {
                    self.errors.push(ProgramError::TranslationError(error));
                }
            }
        }

        // Now handle facts and rules
        for statement in ast.statements() {
            self.process_attributes(statement);

            match statement.kind() {
                ast::statement::StatementKind::Fact(fact) => {
                    match Fact::build_component(&mut self, fact) {
                        Ok(fact) => self.program_builder.add_fact(fact),
                        Err(error) => self.errors.push(ProgramError::TranslationError(error)),
                    }
                }
                ast::statement::StatementKind::Rule(rule) => {
                    match Rule::build_component(&mut self, rule) {
                        Ok(new_rule) => self.program_builder.add_rule(new_rule),
                        Err(translation_error) => self
                            .errors
                            .push(ProgramError::TranslationError(translation_error)),
                    }
                }
                ast::statement::StatementKind::Directive(directive) => {
                    if let Err(error) = handle_use_directive(&mut self, directive) {
                        self.errors.push(ProgramError::TranslationError(error));
                    }
                }
                ast::statement::StatementKind::Error(_token) => {
                    unreachable!(
                        "Faulty statement should result in a parser error and not be propagated."
                    )
                }
            }

            self.statement_attributes.clear();
        }

        let _ = self
            .program_builder
            .validate(&mut self.validation_error_builder);

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

    pub(crate) fn statement_attributes(&self) -> &Bag<KnownAttributes, Vec<Term>> {
        &self.statement_attributes
    }

    /// Recreate the name from a [ast::tag::structure::StructureTag]
    /// by resolving prefixes or bases.
    pub(super) fn resolve_tag(
        &self,
        tag: &'b ast::tag::structure::StructureTag<'a>,
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
                    format!("{expanded_prefix}{tag}")
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

pub(crate) trait TranslationComponent: Sized {
    type Ast<'a>: 'a + ProgramAST<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        ast: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError>;

    /// Construct this object from a string.
    fn parse(input: &str) -> Result<Self, ComponentParseError> {
        let parser_input = ParserInput::new(input, ParserState::default());

        let Ok((tail, ast)) = Self::Ast::parse(parser_input) else {
            return Err(ComponentParseError::ParseError);
        };

        if tail.input_len() != 0 {
            return Err(ComponentParseError::ParseError);
        }

        let mut translation = ASTProgramTranslation::initialize(input, String::default());

        let res = Self::build_component(&mut translation, &ast);

        match res {
            Ok(component) => Ok(component),
            Err(error) => Err(ComponentParseError::TranslationError(error)),
        }
    }
}

/// Implements conversions commonly used when defining newtype wrappers
#[macro_export]
macro_rules! newtype_wrapper {
    ($wrapper:ty: $inner:ty) => {
        impl From<$wrapper> for $inner {
            fn from(value: $wrapper) -> $inner {
                value.0
            }
        }

        impl $wrapper {
            pub fn into_inner(self) -> $inner {
                self.0
            }
        }
    };
}
