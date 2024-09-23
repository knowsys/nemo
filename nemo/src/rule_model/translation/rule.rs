//! This module contains functions for creating a [Rule] from the corresponding ast node.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{
            atom::Atom,
            literal::Literal,
            rule::{Rule, RuleBuilder},
            tag::Tag,
            term::Term,
            ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
    },
};

use super::ASTProgramTranslation;

impl<'a> ASTProgramTranslation<'a> {
    /// Create a [Rule] from the corresponding AST node.
    pub(crate) fn build_rule(
        &mut self,
        rule: &'a ast::rule::Rule<'a>,
    ) -> Result<Rule, TranslationError> {
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

    /// Create a body [Literal] from the corresponding ast node.
    pub(crate) fn build_body_literal(
        &mut self,
        body: &'a ast::guard::Guard<'a>,
    ) -> Result<Literal, TranslationError> {
        let result = match body {
            ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) => {
                let predicate = Tag::from(self.resolve_tag(atom.tag())?)
                    .set_origin(self.register_node(atom.tag()));

                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(self.build_inner_term(expression)?);
                }

                Literal::Positive(self.register_component(Atom::new(predicate, subterms), atom))
            }
            ast::guard::Guard::Expression(ast::expression::Expression::Negation(negated)) => {
                let atom = if let ast::expression::Expression::Atom(atom) = negated.expression() {
                    atom
                } else {
                    return Err(TranslationError::new(
                        negated.span(),
                        TranslationErrorKind::NegatedNonAtom(
                            negated.expression().context_type().name().to_string(),
                        ),
                    ));
                };

                let predicate = Tag::from(self.resolve_tag(atom.tag())?)
                    .set_origin(self.register_node(atom.tag()));
                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(self.build_inner_term(expression)?);
                }

                Literal::Negative(self.register_component(Atom::new(predicate, subterms), atom))
            }
            ast::guard::Guard::Infix(infix) => Literal::Operation(self.build_infix(infix)?),
            ast::guard::Guard::Expression(ast::expression::Expression::Operation(operation)) => {
                Literal::Operation(self.build_operation(operation)?)
            }
            _ => {
                return Err(TranslationError::new(
                    body.span(),
                    TranslationErrorKind::BodyNonLiteral(body.context_type().name().to_string()),
                ))
            }
        }
        .set_origin(self.register_node(body));

        Ok(result)
    }

    /// Create a head [Atom] from the corresponding ast node.
    pub(crate) fn build_head_atom(
        &mut self,
        head: &'a ast::guard::Guard<'a>,
    ) -> Result<Atom, TranslationError> {
        let result =
            if let ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) = head {
                let predicate = Tag::from(self.resolve_tag(atom.tag())?)
                    .set_origin(self.register_node(atom.tag()));
                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(self.build_inner_term(expression)?);
                }

                self.register_component(Atom::new(predicate, subterms), atom)
            } else {
                return Err(TranslationError::new(
                    head.span(),
                    TranslationErrorKind::HeadNonAtom(head.context_type().name().to_string()),
                ));
            };

        Ok(result)
    }

    /// Create a [Term] that occurs within a head atom or body literal.
    pub(crate) fn build_inner_term(
        &mut self,
        expression: &'a ast::expression::Expression,
    ) -> Result<Term, TranslationError> {
        Ok(match expression {
            ast::expression::Expression::Arithmetic(arithmetic) => {
                self.build_arithmetic(arithmetic).map(Term::from)
            }
            ast::expression::Expression::Atom(function) => {
                self.build_function(function).map(Term::from)
            }
            ast::expression::Expression::Blank(blank) => self.build_blank(blank).map(Term::from),
            ast::expression::Expression::Boolean(boolean) => {
                self.build_boolean(boolean).map(Term::from)
            }
            ast::expression::Expression::Constant(constant) => {
                self.build_constant(constant).map(Term::from)
            }
            ast::expression::Expression::Number(number) => {
                self.build_number(number).map(Term::from)
            }
            ast::expression::Expression::RdfLiteral(rdf_literal) => {
                self.build_rdf(rdf_literal).map(Term::from)
            }
            ast::expression::Expression::String(string) => {
                self.build_string(string).map(Term::from)
            }
            ast::expression::Expression::Tuple(tuple) => self.build_tuple(tuple).map(Term::from),
            ast::expression::Expression::Variable(variable) => {
                self.build_variable(variable).map(Term::from)
            }
            ast::expression::Expression::Aggregation(aggregation) => {
                self.build_aggregation(aggregation).map(Term::from)
            }
            ast::expression::Expression::Map(map) => self.build_map(map).map(Term::from),
            ast::expression::Expression::Operation(operation) => {
                self.build_operation(operation).map(Term::from)
            }
            ast::expression::Expression::Negation(negation) => Err(TranslationError::new(
                negation.span(),
                TranslationErrorKind::InnerExpressionNegation,
            )),
            ast::expression::Expression::Parenthesized(parenthesized) => {
                self.build_inner_term(parenthesized.expression())
            }
        }?
        .set_origin(self.register_node(expression)))
    }
}
