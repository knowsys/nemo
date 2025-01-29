use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{atom::Atom, literal::Literal, tag::Tag, term::Term, ProgramComponent},
        error::{translation_error::TranslationErrorKind, TranslationError},
    },
};

use super::{
    complex::{infix::InfixOperation, operation::FunctionLikeOperation},
    TranslationComponent,
};

impl TranslationComponent for Literal {
    type Ast<'a> = ast::guard::Guard<'a>;

    fn build_component<'a, 'b>(
        translation: &mut super::ASTProgramTranslation<'a, 'b>,
        body: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let result = match body {
            ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) => {
                let predicate = Tag::from(translation.resolve_tag(atom.tag())?)
                    .set_origin(translation.register_node(atom.tag()));

                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(Term::build_component(translation, expression)?);
                }

                Literal::Positive(
                    translation.register_component(Atom::new(predicate, subterms), atom),
                )
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

                let predicate = Tag::from(translation.resolve_tag(atom.tag())?)
                    .set_origin(translation.register_node(atom.tag()));
                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(Term::build_component(translation, expression)?);
                }

                Literal::Negative(
                    translation.register_component(Atom::new(predicate, subterms), atom),
                )
            }
            ast::guard::Guard::Infix(infix) => Literal::Operation(
                InfixOperation::build_component(translation, infix)?.into_inner(),
            ),
            ast::guard::Guard::Expression(ast::expression::Expression::Operation(operation)) => {
                Literal::Operation(
                    FunctionLikeOperation::build_component(translation, operation)?.into_inner(),
                )
            }
            _ => {
                return Err(TranslationError::new(
                    body.span(),
                    TranslationErrorKind::BodyNonLiteral(body.context_type().name().to_string()),
                ))
            }
        }
        .set_origin(translation.register_node(body));

        Ok(result)
    }
}

pub(crate) struct HeadAtom(Atom);

impl HeadAtom {
    pub(crate) fn into_inner(self) -> Atom {
        self.0
    }
}

impl TranslationComponent for HeadAtom {
    type Ast<'a> = ast::guard::Guard<'a>;

    fn build_component<'a, 'b>(
        translation: &mut super::ASTProgramTranslation<'a, 'b>,
        head: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let result =
            if let ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) = head {
                let predicate = Tag::from(translation.resolve_tag(atom.tag())?)
                    .set_origin(translation.register_node(atom.tag()));
                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(Term::build_component(translation, expression)?);
                }

                translation.register_component(Atom::new(predicate, subterms), atom)
            } else {
                return Err(TranslationError::new(
                    head.span(),
                    TranslationErrorKind::HeadNonAtom(head.context_type().name().to_string()),
                ));
            };

        Ok(HeadAtom(result))
    }
}

impl TranslationComponent for Atom {
    type Ast<'a> = ast::expression::complex::atom::Atom<'a>;

    fn build_component<'a, 'b>(
        translation: &mut super::ASTProgramTranslation<'a, 'b>,
        atom: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let predicate = Tag::from(translation.resolve_tag(atom.tag())?)
            .set_origin(translation.register_node(atom.tag()));

        let mut subterms = Vec::new();
        for expression in atom.expressions() {
            subterms.push(Term::build_component(translation, expression)?);
        }

        Ok(translation.register_component(Atom::new(predicate, subterms), atom))
    }
}
