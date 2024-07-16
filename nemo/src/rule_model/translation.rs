//! This module defines [ASTProgramTranslation].

use std::ops::Range;

use ariadne::{Color, Label, Report, ReportKind, Source};

use crate::{
    parser::ast,
    rule_model::{components::rule::RuleBuilder, origin::Origin, program::ProgramBuilder},
};

use super::{
    components::{atom::Atom, rule::Rule, ProgramComponent},
    error::{ProgramError, TranslationError, ValidationErrorBuilder},
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

    /// Builder for [ValidationError]s
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
            validation_error_builder: ValidationErrorBuilder::default(),
            errors: Vec::default(),
        }
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

    /// Errors
    errors: Vec<ProgramError>,
}

impl<'a> ProgramErrorReport<'a> {
    /// Print the given reports.
    pub fn eprint<'s, ReportIterator>(
        &'s self,
        reports: ReportIterator,
    ) -> Result<(), std::io::Error>
    where
        ReportIterator: Iterator<Item = Report<'a, (String, Range<usize>)>>,
    {
        for report in reports {
            report.eprint((self.label.clone(), Source::from(self.input)))?;
        }

        Ok(())
    }

    /// Build a [Report] for each error.
    pub fn build_reports(
        &'a self,
        ast: &'a ast::program::Program<'a>,
        color_error: Color,
        color_note: Color,
        color_hint: Color,
    ) -> impl Iterator<Item = Report<'a, (String, Range<usize>)>> {
        self.errors.iter().map(move |error| {
            let range = error.range(ast);

            let report = Report::build(ReportKind::Error, self.label.clone(), range.start.offset)
                .with_code(error.error_code())
                .with_message(error.message())
                .with_label(
                    Label::new((self.label.clone(), range.range()))
                        .with_message(error.message())
                        .with_color(color_error),
                );

            report.finish()
        })
    }
}

impl<'a> ASTProgramTranslation<'a> {
    /// Translate the given [ProgramAST] into a [Program].
    pub fn translate(
        mut self,
        ast: &ast::program::Program<'a>,
    ) -> Result<Program, ProgramErrorReport<'a>> {
        let mut program_builder = ProgramBuilder::default();

        for (statement_index, rule) in vec![ast.statements()].into_iter().enumerate() {
            let origin = Origin::External(statement_index);

            match self.build_rule(origin, rule) {
                Ok(new_rule) => program_builder.add_rule(new_rule),
                Err(translation_error) => self
                    .errors
                    .push(ProgramError::TranslationError(translation_error)),
            }
        }

        self.errors.extend(
            self.validation_error_builder
                .finalize()
                .into_iter()
                .map(ProgramError::ValidationError),
        );

        if self.errors.is_empty() {
            Ok(program_builder.finalize())
        } else {
            Err(ProgramErrorReport {
                input: self.input,
                label: self.input_label,
                errors: self.errors,
            })
        }
    }

    fn build_rule(
        &mut self,
        origin: Origin,
        rule: &ast::rule::Rule<'a>,
    ) -> Result<Rule, TranslationError> {
        self.validation_error_builder.push_origin(origin);
        let mut rule_builder = RuleBuilder::default().origin(origin);

        let mut expression_counter: usize = 0;
        for expression in rule.head() {
            let origin_expression = Origin::External(expression_counter);
            rule_builder.add_head_atom_mut(self.build_head_atom(origin_expression, expression)?);

            expression_counter += 1;
        }

        let rule = rule_builder.finalize();

        let _ = rule.validate(&mut self.validation_error_builder);
        self.validation_error_builder.pop_origin();
        Ok(rule)
    }

    fn build_head_atom(
        &mut self,
        origin: Origin,
        head: &ast::expression::Expression<'a>,
    ) -> Result<Atom, TranslationError> {
        self.validation_error_builder.push_origin(origin);

        let result = Atom::new("test", vec![]).set_origin(origin);

        self.validation_error_builder.pop_origin();
        Ok(result)
    }
}
