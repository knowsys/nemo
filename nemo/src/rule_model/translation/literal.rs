use crate::{
    parser::ast::{self},
    rule_model::{
        components::{atom::Atom, literal::Literal, tag::Tag, term::Term},
        error::translation_error::TranslationError,
        origin::Origin,
    },
};

use super::{
    TranslationComponent,
    complex::{infix::InfixOperation, operation::FunctionLikeOperation},
};

impl TranslationComponent for Literal {
    type Ast<'a> = ast::guard::Guard<'a>;

    fn build_component<'a>(
        translation: &mut super::ASTProgramTranslation,
        body: &Self::Ast<'a>,
    ) -> Option<Self> {
        let result = match body {
            ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) => {
                let predicate =
                    Origin::ast(Tag::from(translation.resolve_tag(atom.tag())?), atom.tag());

                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(Term::build_component(translation, expression)?);
                }

                Literal::Positive(Origin::ast(Atom::new(predicate, subterms), atom))
            }
            ast::guard::Guard::Expression(ast::expression::Expression::Negation(negated)) => {
                let atom = if let ast::expression::Expression::Atom(atom) = negated.expression() {
                    atom
                } else {
                    translation.report.add(
                        negated,
                        TranslationError::NegatedNonAtom {
                            kind: negated.expression().context_type().name().to_string(),
                        },
                    );

                    return None;
                };

                let predicate =
                    Origin::ast(Tag::from(translation.resolve_tag(atom.tag())?), atom.tag());

                let mut subterms = Vec::new();
                for expression in atom.expressions() {
                    subterms.push(Term::build_component(translation, expression)?);
                }

                Literal::Negative(Origin::ast(Atom::new(predicate, subterms), atom))
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
                translation.report.add(
                    body,
                    TranslationError::BodyNonLiteral {
                        kind: body.context_type().name().to_string(),
                    },
                );

                return None;
            }
        };

        Some(result)
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

    fn build_component<'a>(
        translation: &mut super::ASTProgramTranslation,
        head: &Self::Ast<'a>,
    ) -> Option<Self> {
        if let ast::guard::Guard::Expression(ast::expression::Expression::Atom(atom)) = head {
            let predicate =
                Origin::ast(Tag::from(translation.resolve_tag(atom.tag())?), atom.tag());

            let mut subterms = Vec::new();
            for expression in atom.expressions() {
                subterms.push(Term::build_component(translation, expression)?);
            }

            Some(HeadAtom(Origin::ast(Atom::new(predicate, subterms), atom)))
        } else {
            translation.report.add(
                head,
                TranslationError::HeadNonAtom {
                    kind: head.context_type().name().to_string(),
                },
            );
            None
        }
    }
}
