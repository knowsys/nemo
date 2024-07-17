//! This module defines [ASTProgramTranslation].

use std::{collections::HashMap, ops::Range};

use ariadne::{Color, Label, Report, ReportKind, Source};

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::rule::RuleBuilder, error::hint::Hint, origin::Origin, program::ProgramBuilder,
    },
};

use super::{
    components::{atom::Atom, literal::Literal, rule::Rule, term::Term, ProgramComponent},
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

    /// Prefix mapping
    prefix_mapping: HashMap<String, String>,
    /// Base
    base: Option<String>,

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
            prefix_mapping: HashMap::new(),
            base: None,
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
    ) -> impl Iterator<Item = Report<'a, (String, Range<usize>)>> {
        self.errors.iter().map(move |error| {
            let range = error.range(ast);

            let mut report =
                Report::build(ReportKind::Error, self.label.clone(), range.start.offset)
                    .with_code(error.error_code())
                    .with_message(error.message())
                    .with_label(
                        Label::new((self.label.clone(), range.range()))
                            .with_message(error.message())
                            .with_color(color_error),
                    );
            if let Some(note) = error.note() {
                report = report.with_note(note);
            }
            if !error.hints().is_empty() {
                for hint in error.hints() {
                    report = report.with_help(hint);
                }
            }

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

        for expression in rule.body() {
            let origin_expression = Origin::External(expression_counter);
            rule_builder
                .add_body_literal_mut(self.build_body_literal(origin_expression, expression)?);

            expression_counter += 1;
        }

        let rule = rule_builder.finalize().set_origin(origin);

        let _ = rule.validate(&mut self.validation_error_builder);
        self.validation_error_builder.pop_origin();
        Ok(rule)
    }

    fn build_body_literal(
        &mut self,
        origin: Origin,
        head: &ast::expression::Expression<'a>,
    ) -> Result<Literal, TranslationError> {
        self.validation_error_builder.push_origin(origin);

        let result = if let ast::expression::Expression::Atom(atom) = head {
            let mut subterms = Vec::new();
            for (expression_index, expression) in atom.expressions().enumerate() {
                let term_origin = Origin::External(expression_index);
                subterms.push(self.build_inner_term(term_origin, expression)?);
            }

            Literal::Positive(Atom::new(&self.resolve_tag(atom.tag())?, subterms))
        } else {
            return Err(TranslationError::new(
                head.span(),
                TranslationErrorKind::HeadNonAtom(head.context_type().name().to_string()),
                vec![],
            ));
        }
        .set_origin(origin);

        self.validation_error_builder.pop_origin();
        Ok(result)
    }

    fn build_head_atom(
        &mut self,
        origin: Origin,
        head: &ast::expression::Expression<'a>,
    ) -> Result<Atom, TranslationError> {
        self.validation_error_builder.push_origin(origin);

        let result = if let ast::expression::Expression::Atom(atom) = head {
            let mut subterms = Vec::new();
            for (expression_index, expression) in atom.expressions().enumerate() {
                let term_origin = Origin::External(expression_index);
                subterms.push(self.build_inner_term(term_origin, expression)?);
            }

            Atom::new(&self.resolve_tag(atom.tag())?, subterms)
        } else {
            return Err(TranslationError::new(
                head.span(),
                TranslationErrorKind::HeadNonAtom(head.context_type().name().to_string()),
                vec![],
            ));
        }
        .set_origin(origin);

        self.validation_error_builder.pop_origin();
        Ok(result)
    }

    fn build_inner_term(
        &self,
        origin: Origin,
        expression: &ast::expression::Expression,
    ) -> Result<Term, TranslationError> {
        Ok(match expression {
            ast::expression::Expression::Atom(atom) => todo!(),
            ast::expression::Expression::Blank(blank) => todo!(),
            ast::expression::Expression::Boolean(boolean) => todo!(),
            ast::expression::Expression::Constant(constant) => todo!(),
            ast::expression::Expression::Number(number) => todo!(),
            ast::expression::Expression::RdfLiteral(rdf_literal) => todo!(),
            ast::expression::Expression::String(string) => todo!(),
            ast::expression::Expression::Tuple(tuple) => todo!(),
            ast::expression::Expression::Variable(variable) => match variable.kind() {
                ast::expression::basic::variable::VariableType::Universal => {
                    if let Some(variable_name) = variable.name() {
                        Term::universal_variable(&variable_name)
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::UnnamedVariable,
                            vec![Hint::AnonymousVariables],
                        ));
                    }
                }
                ast::expression::basic::variable::VariableType::Existential => {
                    if let Some(variable_name) = variable.name() {
                        Term::existential_variable(&variable_name)
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::UnnamedVariable,
                            vec![],
                        ));
                    }
                }
                ast::expression::basic::variable::VariableType::Anonymous => {
                    if variable.name().is_none() {
                        Term::anonymous_variable()
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::NamedAnonymous(variable.span().0.to_string()),
                            vec![],
                        ));
                    }
                }
            },
        }
        .set_origin(origin))
    }

    fn resolve_tag(&self, tag: &ast::tag::Tag<'a>) -> Result<String, TranslationError> {
        Ok(match tag {
            ast::tag::Tag::Plain(token) => {
                let token_string = token.to_string();

                if let Some(base) = &self.base {
                    format!("{base}{token_string}")
                } else {
                    token_string
                }
            }
            ast::tag::Tag::Prefixed { prefix, tag } => {
                if let Some(expanded_prefix) = self.prefix_mapping.get(&prefix.to_string()) {
                    format!("{expanded_prefix}{}", tag.to_string())
                } else {
                    return Err(TranslationError::new(
                        prefix.span(),
                        TranslationErrorKind::UnknownPrefix(prefix.to_string()),
                        vec![],
                    ));
                }
            }
            ast::tag::Tag::Iri(iri) => iri.content(),
        })
    }
}
