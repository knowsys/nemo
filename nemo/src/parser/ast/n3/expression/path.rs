//! This module defines [N3Path].

use std::{collections::VecDeque, iter::chain, ops::Not};

use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::map,
    multi::many0,
    sequence::{delimited, pair},
};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            expression::basic::blank::Blank,
            n3::{
                comment::WSoC,
                expression::{literal::N3Literal, variable::N3Variable},
            },
            tag::structure::StructureTag,
        },
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        translation::ASTProgramTranslation,
    },
    syntax::n3::translation::{DUMMY_PREDICATE, NAMED_BNODE_PREFIX, TRIPLES_PREDICATE},
};

use super::{
    TranslationFor,
    collection::N3Collection,
    formula::N3Formula,
    propertylist::{N3BnodePropertyList, N3IriPropertyList},
};

/// A Notation3 expression
#[derive(Clone, Debug)]
pub enum N3PathItemKind<'a> {
    /// IRI
    Iri(StructureTag<'a>),
    /// Anonymous blank node
    Anonymous(Span<'a>),
    /// Blank node
    Bnode(Blank<'a>),
    /// Variable
    Variable(N3Variable<'a>),
    /// Collection
    Collection(N3Collection<'a>),
    /// Blank Node property list
    BnodePropertyList(N3BnodePropertyList<'a>),
    /// IRI property list
    IriPropertyList(N3IriPropertyList<'a>),
    /// Literal
    Literal(N3Literal<'a>),
    /// Formula
    Formula(N3Formula<'a>),
}

impl N3PathItemKind<'_> {
    /// Return the [ParserContext] of the underlying path item kind.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Self::Iri(iri) => iri.context(),
            Self::Anonymous(_) => ParserContext::notation3(Notation3Context::Bnode),
            Self::Bnode(blank) => blank.context(),
            Self::Variable(variable) => variable.context(),
            Self::Collection(collection) => collection.context(),
            Self::BnodePropertyList(list) => list.context(),
            Self::IriPropertyList(list) => list.context(),
            Self::Literal(literal) => literal.context(),
            Self::Formula(formula) => formula.context(),
        }
    }

    fn parse_anonymous_bnode<'a>(input: ParserInput<'a>) -> ParserResult<'a, Span<'a>> {
        let input_span = input.span;

        delimited(tag("["), WSoC::parse, tag("]"))(input).map(|(rest, _)| {
            let rest_span = rest.span;

            (rest, input_span.until_rest(&rest_span))
        })
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> (Term, Vec<(Tag, Vec<Term>)>) {
        let mut terms = Vec::new();

        let primitive = match self {
            Self::Iri(structure_tag) => Primitive::constant(
                &translation
                    .resolve_tag(&structure_tag)
                    .unwrap_or_else(|| structure_tag.to_string()),
            ),
            Self::Anonymous(_) => {
                let name = translation.fresh_bnode_name();
                match target {
                    TranslationFor::Fact => Primitive::constant(&name),
                    TranslationFor::Body => Primitive::universal_variable(&name),
                    TranslationFor::Head => Primitive::existential_variable(&name),
                }
            }
            Self::Formula(formula) => {
                let primitive = match target {
                    TranslationFor::Fact => Primitive::constant(&translation.fresh_bnode_name()),
                    TranslationFor::Body | TranslationFor::Head => {
                        let mut formula_terms = formula.to_terms(translation, target);
                        terms.append(&mut formula_terms);

                        match formula_terms
                            .first()
                            .map(|(_, terms)| terms.first())
                            .flatten()
                        {
                            Some(Term::Primitive(primitive)) => primitive.clone(),
                            _ => Primitive::constant(&DUMMY_PREDICATE),
                        }
                    }
                };

                primitive
            }
            Self::BnodePropertyList(list) => {
                let (primitive, mut bnode_terms) = list.to_terms(translation, target);
                terms.append(&mut bnode_terms);

                primitive
            }
            Self::Bnode(blank) => {
                let name = format!("{NAMED_BNODE_PREFIX}_{}", blank.name());
                match target {
                    TranslationFor::Fact => Primitive::constant(&name),
                    TranslationFor::Body => Primitive::universal_variable(&name),
                    TranslationFor::Head => Primitive::existential_variable(&name),
                }
            }
            Self::Variable(variable) => Primitive::universal_variable(&variable.name()),
            Self::Collection(collection) => {
                let (primitive, mut collection_terms) = collection.to_terms(translation, target);
                terms.append(&mut collection_terms);

                primitive
            }
            Self::IriPropertyList(list) => {
                let (primitive, mut list_terms) = list.to_terms(translation, target);
                terms.append(&mut list_terms);

                primitive
            }
            Self::Literal(literal) => Primitive::ground(
                literal
                    .to_any_data_value(translation)
                    .expect("is a valid data value"),
            ),
        };

        (Term::Primitive(primitive), terms)
    }
}

impl<'a> ProgramAST<'a> for N3PathItemKind<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            Self::Iri(iri) => iri.children(),
            Self::Anonymous(_) => Vec::default(),
            Self::Bnode(blank) => blank.children(),
            Self::Variable(variable) => variable.children(),
            Self::Collection(collection) => collection.children(),
            Self::BnodePropertyList(list) => list.children(),
            Self::IriPropertyList(list) => list.children(),
            Self::Literal(literal) => literal.children(),
            Self::Formula(formula) => formula.children(),
        }
    }

    fn span(&self) -> Span<'a> {
        match self {
            Self::Iri(iri) => iri.span(),
            Self::Anonymous(span) => span.clone(),
            Self::Bnode(blank) => blank.span(),
            Self::Variable(variable) => variable.span(),
            Self::Collection(collection) => collection.span(),
            Self::BnodePropertyList(list) => list.span(),
            Self::IriPropertyList(list) => list.span(),
            Self::Literal(literal) => literal.span(),
            Self::Formula(formula) => formula.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        alt((
            map(N3IriPropertyList::parse, Self::IriPropertyList),
            map(N3BnodePropertyList::parse, Self::BnodePropertyList),
            map(N3Literal::parse, Self::Literal),
            map(
                context(
                    ParserContext::notation3(Notation3Context::Iri),
                    StructureTag::parse_hyphenated,
                ),
                Self::Iri,
            ),
            map(Self::parse_anonymous_bnode, Self::Anonymous),
            map(Blank::parse, Self::Bnode),
            map(N3Variable::parse, Self::Variable),
            map(N3Formula::parse, Self::Formula),
            map(N3Collection::parse, Self::Collection),
        ))(input)
    }

    fn context(&self) -> ParserContext {
        self.context_type()
    }
}

/// The direction of an element in an [N3Path].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum N3PathDirection {
    /// Forwards path segment.
    Forward,
    /// Backwards path segment.
    Backward,
}

impl N3PathDirection {
    fn parse<'a>(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(tag("!"), |_| Self::Forward),
            map(tag("^"), |_| Self::Backward),
        ))(input)
    }
}

impl Not for N3PathDirection {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Forward => Self::Backward,
            Self::Backward => Self::Forward,
        }
    }
}

/// A Notation3 path item
#[derive(Clone, Debug)]
pub struct N3PathItem<'a> {
    span: Span<'a>,

    pub(crate) kind: N3PathItemKind<'a>,
}

impl<'a> N3PathItem<'a> {
    /// Return the [ParserContext] of the underlying expression type.
    pub fn context_type(&self) -> ParserContext {
        self.kind.context_type()
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> (Term, Vec<(Tag, Vec<Term>)>) {
        self.kind.to_terms(translation, target)
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::PathItem);

impl<'a> ProgramAST<'a> for N3PathItem<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.kind.children()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, N3PathItemKind::parse)(input).map(|(rest, kind)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

/// The different kinds of Notation3 paths.
#[derive(Clone, Debug)]
pub enum N3PathKind<'a> {
    /// A path consisting of a single [N3PathItem].
    Single(N3PathItem<'a>),
    /// A path consisting of an [N3PathItem] with a forward edge to another [N3PathKind].
    Forward(N3PathItem<'a>, Box<N3PathKind<'a>>),
    /// A path consisting of an [N3PathItem] with a backward edge to another [N3PathKind].
    Backward(N3PathItem<'a>, Box<N3PathKind<'a>>),
}

impl<'a> N3PathKind<'a> {
    /// Create a new [N3PathKind] from an [N3PathItem] and a list of further path elements.
    pub fn new(
        first: N3PathItem<'a>,
        mut further: VecDeque<(N3PathDirection, N3PathItem<'a>)>,
    ) -> Self {
        match further.pop_front() {
            None => Self::Single(first),
            Some((direction, next)) => match direction {
                N3PathDirection::Forward => {
                    Self::Forward(first, Box::new(Self::new(next, further)))
                }
                N3PathDirection::Backward => {
                    Self::Backward(first, Box::new(Self::new(next, further)))
                }
            },
        }
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> (Term, Vec<(Tag, Vec<Term>)>) {
        match self {
            Self::Single(item) => item.to_terms(translation, target),
            Self::Forward(item, next) => {
                let (subject, mut facts) = item.to_terms(translation, target);
                let (end, mut rest_facts) =
                    next.to_terms_continuation(translation, target, subject, true);
                facts.append(&mut rest_facts);

                (end, facts)
            }
            Self::Backward(item, next) => {
                let (object, mut facts) = item.to_terms(translation, target);
                let (end, mut rest_facts) =
                    next.to_terms_continuation(translation, target, object, false);
                facts.append(&mut rest_facts);

                (end, facts)
            }
        }
    }

    pub(crate) fn to_terms_continuation(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
        term: Term,
        is_forward: bool,
    ) -> (Term, Vec<(Tag, Vec<Term>)>) {
        let tag = Tag::new(TRIPLES_PREDICATE.to_string());
        match self {
            Self::Single(item) => {
                let (predicate, mut facts) = item.to_terms(translation, target);
                let link = Term::Primitive(translation.fresh_bnode_or_variable(target));
                facts.push((
                    tag,
                    if is_forward {
                        vec![term, predicate, link.clone()]
                    } else {
                        vec![link.clone(), predicate, term]
                    },
                ));
                (link, facts)
            }
            Self::Forward(item, next) => {
                let (predicate, mut facts) = item.to_terms(translation, target);
                let link = Term::Primitive(translation.fresh_bnode_or_variable(target));
                facts.push((tag, vec![term, predicate, link.clone()]));
                let (end, mut rest_facts) =
                    next.to_terms_continuation(translation, target, link, true);
                facts.append(&mut rest_facts);

                (end, facts)
            }
            Self::Backward(item, next) => {
                let (predicate, mut facts) = item.to_terms(translation, target);
                let link = Term::Primitive(translation.fresh_bnode_or_variable(target));
                facts.push((tag, vec![link.clone(), predicate, term]));
                let (end, mut rest_facts) =
                    next.to_terms_continuation(translation, target, link, false);
                facts.append(&mut rest_facts);

                (end, facts)
            }
        }
    }
}

const PATH_CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Path);

impl<'a> ProgramAST<'a> for N3PathKind<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            N3PathKind::Single(item) => item.children(),
            N3PathKind::Forward(item, next) => chain(item.children(), next.children()).collect(),
            N3PathKind::Backward(item, next) => chain(item.children(), next.children()).collect(),
        }
    }

    fn span(&self) -> Span<'a> {
        match self {
            N3PathKind::Single(item) => item.span(),
            N3PathKind::Forward(item, _) => item.span(),
            N3PathKind::Backward(item, _) => item.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            PATH_CONTEXT,
            pair(
                N3PathItem::parse,
                many0(pair(N3PathDirection::parse, N3PathItem::parse)),
            ),
        )(input)
        .map(|(rest, (first, further))| (rest, Self::new(first, VecDeque::from(further))))
    }

    fn context(&self) -> ParserContext {
        PATH_CONTEXT
    }
}

/// A Notation3 path.
#[derive(Clone, Debug)]
pub struct N3Path<'a> {
    span: Span<'a>,

    kind: N3PathKind<'a>,
}

impl<'a> N3Path<'a> {
    /// Return the underlying [N3PathKind].
    pub fn kind(&'a self) -> &'a N3PathKind<'a> {
        &self.kind
    }
}

impl<'a> ProgramAST<'a> for N3Path<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.kind.children()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        N3PathKind::parse(input).map(|(rest, kind)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        PATH_CONTEXT
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::expression::path::{N3Path, N3PathItem, N3PathItemKind},
        },
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_paths() {
        let iri = r#":doerthe"#;

        let parser_input = ParserInput::new(iri, ParserState::default());
        let result = all_consuming(N3PathItem::parse)(parser_input);

        assert_matches!(
            result,
            Ok((
                _,
                N3PathItem {
                    kind: N3PathItemKind::Iri(_),
                    ..
                }
            ))
        );

        let path = r#":joe!:hasMother^:hasMother"#;

        let parser_input = ParserInput::new(path, ParserState::default());
        let result = all_consuming(N3Path::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
