//! This module defines [N3Expression].

use formula::N3Formula;
use path::{N3Path, N3PathDirection, N3PathItem, N3PathItemKind};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            expression::{
                Expression,
                basic::{
                    constant::Constant,
                    variable::{Variable, VariableType},
                },
            },
        },
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{components::term::primitive::Primitive, translation::ASTProgramTranslation},
};

pub mod formula;
pub mod literal;
pub mod path;
pub mod variable;

/// How to translate a blank node.
#[derive(Clone, Copy, Debug)]
pub enum BnodeTarget {
    /// Blank node should be turned into a constant (skolemised).
    Constant,
    /// Blank node should be turned into a universally quantified variable.
    Universal,
    /// Blank node should be turned into an existentially quantified variable.    
    Existential,
}

/// A Notation3 expression
#[derive(Clone, Debug)]
pub struct N3Expression<'a> {
    span: Span<'a>,

    pub(crate) path: N3Path<'a>,
}

impl<'a> N3Expression<'a> {
    pub(crate) fn from_span_and_iri(span: Span<'a>, iri: &'a str) -> Self {
        Self {
            span,
            path: N3Path::from_span_and_iri(span, iri),
        }
    }

    pub(crate) fn forward(expression: N3Expression<'a>) -> Self {
        Self {
            span: expression.span(),
            path: expression.path,
        }
    }

    pub(crate) fn backward(expression: N3Expression<'a>) -> Self {
        Self {
            span: expression.span(),
            path: N3Path::reverse(expression.path),
        }
    }

    pub(crate) fn to_primitive(
        &self,
        translation: &mut ASTProgramTranslation,
        bnode: BnodeTarget,
    ) -> Option<Primitive> {
        match &self.try_as_single_forward_item()?.kind {
            N3PathItemKind::Iri(structure_tag) => {
                log::debug!(
                    "resolving tag: {:?} ~> {:?}",
                    structure_tag.to_string(),
                    translation.resolve_tag(&structure_tag)
                );

                Some(Primitive::constant(
                    &translation.resolve_tag(&structure_tag)?,
                ))
            }
            N3PathItemKind::Bnode(blank) => {
                let name = format!("_bnode_{}", blank.name());
                match bnode {
                    BnodeTarget::Constant => Some(Primitive::constant(&name)),
                    BnodeTarget::Universal => Some(Primitive::universal_variable(&name)),
                    BnodeTarget::Existential => Some(Primitive::existential_variable(&name)),
                }
            }
            N3PathItemKind::Variable(n3_variable) => {
                Some(Primitive::universal_variable(&n3_variable.name()))
            }
            N3PathItemKind::Collection => None,
            N3PathItemKind::BnodePropertyList => None,
            N3PathItemKind::IriPropertyList => None,
            N3PathItemKind::Literal(n3_literal) => Some(Primitive::ground(
                n3_literal
                    .to_any_data_value(translation)
                    .expect("is a valid data value"),
            )),
            N3PathItemKind::Formula(_) => None,
        }
    }

    fn try_as_single_forward_item(&self) -> Option<&N3PathItem<'a>> {
        if self.path.items.len() == 1
            && let Some((N3PathDirection::Forward, item)) = self.path.items.first()
        {
            return Some(item);
        }

        None
    }

    fn try_into_single_forward_item(mut self) -> Option<N3PathItem<'a>> {
        if self.path.items.len() == 1
            && let (N3PathDirection::Forward, item) = self.path.items.remove(0)
        {
            return Some(item);
        }

        None
    }

    pub(crate) fn try_into_formula(self) -> Option<N3Formula<'a>> {
        if let Some(item) = self.try_into_single_forward_item()
            && let N3PathItemKind::Formula(formula) = item.kind
        {
            return Some(formula);
        }

        None
    }

    pub(crate) fn try_to_variable(&self) -> Option<Variable<'a>> {
        if let Some(item) = self.try_as_single_forward_item()
            && let N3PathItemKind::Variable(variable) = &item.kind
        {
            return Some(Variable::from(variable.clone()));
        }

        if let Some(item) = self.try_as_single_forward_item()
            && let N3PathItemKind::Bnode(bnode) = &item.kind
        {
            return Some(Variable::from_span_kind_and_name(
                bnode.span,
                VariableType::Existential,
                bnode.name.clone(),
            ));
        }

        None
    }

    pub(crate) fn try_to_constant(&self) -> Option<Constant<'a>> {
        if let Some(item) = self.try_as_single_forward_item()
            && let N3PathItemKind::Iri(iri) = &item.kind
        {
            return Some(Constant::from_span_and_tag(self.span, iri.clone()));
        }
        None
    }

    pub(crate) fn try_to_expression(&self) -> Option<Expression<'a>> {
        if let Some(variable) = self.try_to_variable() {
            return Some(Expression::Variable(variable));
        }

        if let Some(constant) = self.try_to_constant() {
            return Some(Expression::Constant(constant));
        }

        None
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Expression);

impl<'a> ProgramAST<'a> for N3Expression<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.path]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, N3Path::parse)(input).map(|(rest, path)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    path,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
