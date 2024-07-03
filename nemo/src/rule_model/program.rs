//! This module defines [Program].

use nemo_physical::datavalues::AnyDataValue;

use crate::{io::parser::ast, rule_model::component::term::tuple::Tuple};

use super::{
    component::{
        atom::Atom,
        base::Base,
        fact::Fact,
        import_export::{ExportDirective, ImportDirective},
        literal::Literal,
        output::Output,
        rule::{Rule, RuleBuilder},
        term::{
            function::FunctionTerm,
            operation::{Operation, OperationKind},
            Term,
        },
        ProgramComponent,
    },
    origin::Origin,
};

/// Representation of a nemo program
#[derive(Debug, Default)]
pub struct Program {
    /// Imported resources
    imports: Vec<ImportDirective>,
    /// Exported resources
    exports: Vec<ExportDirective>,
    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Fact>,
    /// Base
    base: Option<Base>,
    /// Outputs
    outputs: Vec<Output>,
}

impl Program {
    /// Build a [Program] from an [ast::program::Program].
    pub fn from_ast(ast_program: ast::program::Program) -> Self {
        let mut program = Program::default();

        for (statement_index, statement) in ast_program.statements.iter().enumerate() {
            match statement {
                ast::statement::Statement::Directive(directive) => {
                    program.ast_build_directive(directive);
                }
                ast::statement::Statement::Fact {
                    span,
                    doc_comment,
                    atom,
                    dot,
                } => todo!(),
                ast::statement::Statement::Rule { head, body, .. } => {
                    program.ast_build_rule(head, body);
                }
                ast::statement::Statement::Comment(_) => todo!(),
                ast::statement::Statement::Error(_) => todo!(),
            }
        }

        program
    }

    fn ast_build_rule(
        &mut self,
        head: &ast::List<ast::atom::Atom>,
        body: &ast::List<ast::atom::Atom>,
    ) {
        let origin = Origin::External(self.rules.len());

        let rule_builder = RuleBuilder::default().origin(origin);
        let mut head_builder = rule_builder.head();

        // TODO: Implement a normal iterator to avoid cloning
        for (head_index, head_atom) in head.clone().into_iter().enumerate() {
            let origin = Origin::External(head_index);
            if let Literal::Positive(atom) = Self::ast_build_literal(origin, &head_atom) {
                head_builder = head_builder.add_atom(atom);
            } else {
                unreachable!("head must only contain positive atoms")
            }
        }

        let mut body_builder = head_builder.done().body();

        // TODO: Implement a normal iterator to avoid cloning
        for (body_index, body_atom) in body.clone().into_iter().enumerate() {
            let origin = Origin::External(body_index);
            body_builder = body_builder.add_literal(Self::ast_build_literal(origin, &body_atom));
        }

        self.rules.push(body_builder.done().finalize());
    }

    fn ast_build_literal(origin: Origin, atom: &ast::atom::Atom) -> Literal {
        match atom {
            ast::atom::Atom::Positive(positive_atom) => {
                Literal::Positive(Self::ast_build_atom(origin, positive_atom))
            }
            ast::atom::Atom::Negative {
                atom: negative_atom,
                ..
            } => Literal::Negative(Self::ast_build_atom(origin, negative_atom)),
            ast::atom::Atom::InfixAtom {
                lhs,
                operation,
                rhs,
                ..
            } => {
                let left = Self::ast_build_inner_term(Origin::External(0), lhs);
                let right = Self::ast_build_inner_term(Origin::External(1), rhs);

                Literal::Operation(
                    Operation::new_from_name(&operation.to_string(), vec![left, right])
                        .expect("unkown infix operation"),
                )
            }
            ast::atom::Atom::Map(_) => {
                // Return unsupported error
                todo!()
            }
        }
    }

    fn ast_build_atom(origin: Origin, atom: &ast::tuple::Tuple) -> Atom {
        let predicate_name = atom
            .identifier
            .expect("Atom must have a predicate name")
            .to_string();
        let subterms = match &atom.terms {
            Some(terms) => terms.to_vec(),
            None => vec![],
        };

        let mut translated_subterms = Vec::new();

        for (term_index, subterm) in subterms.into_iter().enumerate() {
            let origin = Origin::External(term_index);
            translated_subterms.push(Self::ast_build_inner_term(origin, &subterm));
        }

        Atom::new(&predicate_name, translated_subterms).set_origin(origin)
    }

    fn ast_build_inner_term(origin: Origin, term: &ast::term::Term) -> Term {
        match term {
            ast::term::Term::Primitive(primitive) => Self::ast_build_primitive(origin, primitive),
            ast::term::Term::UniversalVariable(name) => Term::universal_variable(&name.to_string()),
            ast::term::Term::ExistentialVariable(name) => {
                Term::existential_variable(&name.to_string())
            }
            ast::term::Term::UnaryPrefix {
                operation, term, ..
            } => {
                // TODO: Currently no associated function with this
                todo!()
            }
            ast::term::Term::Binary {
                lhs,
                operation,
                rhs,
                ..
            } => {
                let left = Self::ast_build_inner_term(Origin::External(0), lhs);
                let right = Self::ast_build_inner_term(Origin::External(1), rhs);

                Term::Operation(
                    Operation::new_from_name(&operation.to_string(), vec![left, right])
                        .expect("unrecognized binary operation"),
                )
            }
            ast::term::Term::Aggregation {
                operation, terms, ..
            } => {
                todo!()
            }
            ast::term::Term::Tuple(tuple) => Self::ast_build_inner_tuple(origin, tuple),
            ast::term::Term::Map(_) => todo!(),
            ast::term::Term::Blank(_) => todo!(),
        }
        .set_origin(origin)
    }

    fn ast_build_primitive(origin: Origin, primitive: &ast::term::Primitive) -> Term {
        match primitive {
            ast::term::Primitive::Constant(value) => {
                Term::ground(AnyDataValue::new_iri(value.to_string()))
            }
            ast::term::Primitive::PrefixedConstant {
                span,
                prefix,
                colon,
                constant,
            } => todo!(),
            ast::term::Primitive::Number {
                span,
                sign,
                before,
                dot,
                after,
                exponent,
            } => todo!(),
            ast::term::Primitive::String(string) => {
                Term::ground(AnyDataValue::new_plain_string(string.to_string()))
            }
            ast::term::Primitive::Iri(iri) => Term::ground(AnyDataValue::new_iri(iri.to_string())),
            ast::term::Primitive::RdfLiteral { string, iri, .. } => {
                Term::ground(AnyDataValue::new_other(string.to_string(), iri.to_string()))
            }
        }
        .set_origin(origin)
    }

    fn ast_build_inner_tuple(origin: Origin, tuple: &ast::tuple::Tuple) -> Term {
        let subterms = match &tuple.terms {
            Some(terms) => terms.to_vec(),
            None => vec![],
        };

        let mut translated_subterms = Vec::new();

        for (term_index, subterm) in subterms.into_iter().enumerate() {
            let origin = Origin::External(term_index);
            translated_subterms.push(Self::ast_build_inner_term(origin, &subterm));
        }

        match tuple.identifier {
            Some(name) => match OperationKind::from_name(&name.to_string()) {
                Some(kind) => Term::Operation(Operation::new(kind, translated_subterms)),
                None => {
                    Term::FunctionTerm(FunctionTerm::new(&name.to_string(), translated_subterms))
                }
            },
            None => Term::Tuple(Tuple::new(translated_subterms)),
        }
    }

    fn ast_build_directive(&mut self, directive: &ast::directive::Directive) {
        match directive {
            ast::directive::Directive::Base { base_iri, .. } => {
                self.base = Some(Base::new(base_iri.to_string()));
                // TODO: Set origin
            }
            ast::directive::Directive::Prefix {
                span,
                doc_comment,
                prefix,
                prefix_iri,
                dot,
            } => todo!(),
            ast::directive::Directive::Import {
                span,
                doc_comment,
                predicate,
                arrow,
                map,
                dot,
            } => todo!(),
            ast::directive::Directive::Export {
                span,
                doc_comment,
                predicate,
                arrow,
                map,
                dot,
            } => todo!(),
            ast::directive::Directive::Output {
                span,
                doc_comment,
                predicates,
                dot,
            } => todo!(),
        }
    }
}
