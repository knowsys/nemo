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

        for (_statement_index, statement) in ast_program.statements.iter().enumerate() {
            match statement {
                ast::statement::Statement::Directive(directive) => {
                    program.ast_build_directive(directive);
                }
                ast::statement::Statement::Fact {
                    span: _span,
                    doc_comment: _doc_comment,
                    fact: _atom,
                    dot: _dot,
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

        let mut rule_builder = RuleBuilder::default().origin(origin);

        // TODO: Implement a normal iterator to avoid cloning
        for (head_index, head_atom) in head.clone().into_iter().enumerate() {
            let origin = Origin::External(head_index);
            if let Literal::Positive(atom) = Self::ast_build_literal(origin, &head_atom) {
                rule_builder.add_head_atom_mut(atom);
            } else {
                unreachable!("head must only contain positive atoms")
            }
        }

        // TODO: Implement a normal iterator to avoid cloning
        for (body_index, body_atom) in body.clone().into_iter().enumerate() {
            let origin = Origin::External(body_index);
            rule_builder.add_body_literal_mut(Self::ast_build_literal(origin, &body_atom));
        }

        self.rules.push(rule_builder.finalize());
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

    fn ast_build_atom(origin: Origin, atom: &ast::named_tuple::NamedTuple) -> Atom {
        let predicate_name = atom.identifier.to_string();
        let subterms = match &atom.tuple.terms {
            Some(terms) => terms.to_item_vec(),
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
                operation: _,
                terms: _,
                ..
            } => {
                todo!()
            }
            ast::term::Term::Tuple(tuple) => Self::ast_build_inner_tuple(origin, tuple),
            ast::term::Term::NamedTuple(named_tuple) => {
                Self::ast_build_inner_named_tuple(origin, named_tuple)
            }
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
                span: _,
                prefix: _,
                colon: _,
                constant: _,
            } => todo!(),
            ast::term::Primitive::Number {
                span: _,
                sign: _,
                before: _,
                dot: _,
                after: _,
                exponent: _,
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

    fn ast_build_inner_tuple(_origin: Origin, tuple: &ast::tuple::Tuple) -> Term {
        let subterms = match &tuple.terms {
            Some(terms) => terms.to_item_vec(),
            None => vec![],
        };

        let mut translated_subterms = Vec::new();

        for (term_index, subterm) in subterms.into_iter().enumerate() {
            let origin = Origin::External(term_index);
            translated_subterms.push(Self::ast_build_inner_term(origin, &subterm));
        }

        Term::Tuple(Tuple::new(translated_subterms))
    }

    fn ast_build_inner_named_tuple(
        _origin: Origin,
        named_tuple: &ast::named_tuple::NamedTuple,
    ) -> Term {
        let subterms = match &named_tuple.tuple.terms {
            Some(terms) => terms.to_item_vec(),
            None => vec![],
        };

        let mut translated_subterms = Vec::new();

        for (term_index, subterm) in subterms.into_iter().enumerate() {
            let origin = Origin::External(term_index);
            translated_subterms.push(Self::ast_build_inner_term(origin, &subterm));
        }

        let name = &named_tuple.identifier.to_string();
        match OperationKind::from_name(name) {
            Some(kind) => Term::Operation(Operation::new(kind, translated_subterms)),
            None => Term::FunctionTerm(FunctionTerm::new(name, translated_subterms)),
        }
    }

    fn ast_build_directive(&mut self, directive: &ast::directive::Directive) {
        match directive {
            ast::directive::Directive::Base { base_iri, .. } => {
                self.base = Some(Base::new(base_iri.to_string()));
                // TODO: Set origin
            }
            ast::directive::Directive::Prefix {
                span: _,
                doc_comment: _,
                prefix: _,
                prefix_iri: _,
                dot: _,
            } => todo!(),
            ast::directive::Directive::Import {
                span: _,
                doc_comment: _,
                predicate: _,
                arrow: _,
                map: _,
                dot: _,
            } => todo!(),
            ast::directive::Directive::Export {
                span: _,
                doc_comment: _,
                predicate: _,
                arrow: _,
                map: _,
                dot: _,
            } => todo!(),
            ast::directive::Directive::Output {
                span: _,
                doc_comment: _,
                predicates: _,
                dot: _,
            } => todo!(),
        }
    }
}
