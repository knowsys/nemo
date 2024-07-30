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

    /// Mapping of [Origin] to [ProgramAST] nodes
    origin_map: HashMap<Origin, &'a dyn ProgramAST<'a>>,

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
            origin_map: HashMap::new(),
            prefix_mapping: HashMap::new(),
            base: None,
            validation_error_builder: ValidationErrorBuilder::default(),
            errors: Vec::default(),
        }
    }

    /// Register a [ProgramAST] so that it can be associated with and later referenced by
    /// the returned [Origin].
    pub fn register_node(&mut self, node: &'a dyn ProgramAST<'a>) -> Origin {
        let new_origin = Origin::External(self.origin_map.len());
        self.origin_map.insert(new_origin, node);

        new_origin
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
        let mut program_builder = ProgramBuilder::default();

        for statement in ast.statements() {
            if let Some(statement) = statement {
                match statement.kind() {
                    ast::statement::StatementKind::Fact(_) => todo!(),
                    ast::statement::StatementKind::Rule(rule) => match self.build_rule(rule) {
                        Ok(new_rule) => program_builder.add_rule(new_rule),
                        Err(translation_error) => self
                            .errors
                            .push(ProgramError::TranslationError(translation_error)),
                    },
                    ast::statement::StatementKind::Directive(_) => todo!(),
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
            Ok(program_builder.finalize())
        } else {
            Err(ProgramErrorReport {
                input: self.input,
                label: self.input_label,
                errors: self.errors,
                origin_map: self.origin_map,
            })
        }
    }

    fn build_rule(&mut self, rule: &'a ast::rule::Rule<'a>) -> Result<Rule, TranslationError> {
        let mut rule_builder = RuleBuilder::default().origin(self.register_node(rule));

        for expression in rule.head() {
            rule_builder.add_head_atom_mut(self.build_head_atom(expression)?);
        }

        for expression in rule.body() {
            rule_builder.add_body_literal_mut(self.build_body_literal(expression)?);
        }

        let rule = rule_builder.finalize();

        let _ = rule.validate(&mut self.validation_error_builder);
        Ok(rule)
    }

    fn build_body_literal(
        &mut self,
        head: &'a ast::expression::Expression<'a>,
    ) -> Result<Literal, TranslationError> {
        let result = if let ast::expression::Expression::Atom(atom) = head {
            let mut subterms = Vec::new();
            for expression in atom.expressions() {
                subterms.push(self.build_inner_term(expression)?);
            }

            Literal::Positive(
                Atom::new(&self.resolve_tag(atom.tag())?, subterms)
                    .set_origin(self.register_node(atom)),
            )
        } else {
            todo!()
        }
        .set_origin(self.register_node(head));

        Ok(result)
    }

    fn build_head_atom(
        &mut self,
        head: &'a ast::expression::Expression<'a>,
    ) -> Result<Atom, TranslationError> {
        let result = if let ast::expression::Expression::Atom(atom) = head {
            let mut subterms = Vec::new();
            for expression in atom.expressions() {
                subterms.push(self.build_inner_term(expression)?);
            }

            Atom::new(&self.resolve_tag(atom.tag())?, subterms).set_origin(self.register_node(atom))
        } else {
            return Err(TranslationError::new(
                head.span(),
                TranslationErrorKind::HeadNonAtom(head.context_type().name().to_string()),
            ));
        };

        Ok(result)
    }

    fn build_inner_term(
        &mut self,
        expression: &'a ast::expression::Expression,
    ) -> Result<Term, TranslationError> {
        Ok(match expression {
            ast::expression::Expression::Arithmetic(_) => todo!(),
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
                            .set_origin(self.register_node(variable))
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::UnnamedVariable,
                        )
                        .add_hint(Hint::AnonymousVariables));
                    }
                }
                ast::expression::basic::variable::VariableType::Existential => {
                    if let Some(variable_name) = variable.name() {
                        Term::existential_variable(&variable_name)
                            .set_origin(self.register_node(variable))
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::UnnamedVariable,
                        ));
                    }
                }
                ast::expression::basic::variable::VariableType::Anonymous => {
                    if variable.name().is_none() {
                        Term::anonymous_variable().set_origin(self.register_node(variable))
                    } else {
                        return Err(TranslationError::new(
                            variable.span(),
                            TranslationErrorKind::NamedAnonymous(variable.span().0.to_string()),
                        ));
                    }
                }
            },
            ast::expression::Expression::Aggregation(_) => todo!(),
            ast::expression::Expression::Infix(_) => todo!(),
            ast::expression::Expression::Map(_) => todo!(),
            ast::expression::Expression::Negation(_) => todo!(),
            ast::expression::Expression::Operation(_) => todo!(),
        })
    }

    fn resolve_tag(
        &self,
        tag: &'a ast::tag::structure::StructureTag<'a>,
    ) -> Result<String, TranslationError> {
        Ok(match tag.kind() {
            ast::tag::structure::StructureTagKind::Plain(token) => {
                let token_string = token.to_string();

                if let Some(base) = &self.base {
                    format!("{base}{token_string}")
                } else {
                    token_string
                }
            }
            ast::tag::structure::StructureTagKind::Prefixed { prefix, tag } => {
                if let Some(expanded_prefix) = self.prefix_mapping.get(&prefix.to_string()) {
                    format!("{expanded_prefix}{}", tag.to_string())
                } else {
                    return Err(TranslationError::new(
                        prefix.span(),
                        TranslationErrorKind::UnknownPrefix(prefix.to_string()),
                    ));
                }
            }
            ast::tag::structure::StructureTagKind::Iri(iri) => iri.content(),
        })
    }
}
