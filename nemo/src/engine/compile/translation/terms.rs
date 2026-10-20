//! Term helpers: tests on a term's form, pattern desugaring, and the
//! evaluation of ground terms into values.

use std::collections::BTreeSet;

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{DataValue, MapValue, TupleValue},
};

use crate::{
    diagnostics::TranslationError,
    engine::compile::translation::{StackProgramBuilder, Translator, VariableScope, unsupported},
    model::{Aggregate, Operation, Operator, Span, Term, TermKind, VariableKind, Walk},
};

/// Collects the aggregate subterms of a term, recursively,
/// through operations and structures alike, but not into an aggregate.
pub(super) fn collect_aggregates<'t>(term: &'t Term, into: &mut Vec<&'t Aggregate>) {
    match &term.kind {
        TermKind::Aggregate(aggregate) => into.push(aggregate),
        _ => {
            for subterm in term.subterms() {
                collect_aggregates(subterm, into);
            }
        }
    }
}

/// Collects the global-variable names a term reads.
pub(super) fn collect_globals<'t>(term: &'t Term, into: &mut Vec<&'t str>) {
    into.extend(
        term.variables()
            .into_iter()
            .filter(|variable| variable.kind == VariableKind::Global)
            .map(|variable| variable.name.as_str()),
    );
}

/// Returns whether a term calls a nondeterministic function
/// (`RAND`, `UUID`, `STRUUID`, or `NOW`) anywhere inside,
/// so that no two evaluations of it need agree.
pub(super) fn is_volatile(term: &Term) -> bool {
    term.any_term(|term| {
        matches!(&term.kind, TermKind::Operation(operation) if operation.operator.is_nondeterministic())
    })
}

/// Collects the blank-node labels a component mentions.
pub(super) fn collect_blank_nodes<'t>(component: &'t impl Walk, into: &mut Vec<&'t str>) {
    component.walk(&mut |term| {
        if let TermKind::BlankNode(label) = &term.kind {
            into.push(label);
        }
    });
}

/// Returns whether a component mentions a variable of the given kind.
pub(super) fn mentions(component: &impl Walk, kind: VariableKind) -> bool {
    component
        .variables()
        .iter()
        .any(|variable| variable.kind == kind)
}

/// Returns whether a term is or contains a variable a pattern binds or skips: a
/// universal or an anonymous one.
fn has_pattern_variables(term: &Term) -> bool {
    mentions(term, VariableKind::Universal) || mentions(term, VariableKind::Anonymous)
}

/// Returns whether a term is a structured value: a tuple, a labeled tuple, or a map.
pub(super) fn is_structured(term: &Term) -> bool {
    matches!(
        &term.kind,
        TermKind::Tuple(_) | TermKind::Map { .. } | TermKind::Function { .. }
    )
}

/// Returns whether a term is a pattern: a structured value with variables in
/// it, which a body atom matches by destructuring.
pub(super) fn is_pattern(term: &Term) -> bool {
    is_structured(term) && has_pattern_variables(term)
}

/// Returns a pattern length or position as an integer term.
///
/// Arities stay far below what an `i64` holds.
fn integer(value: usize) -> Term {
    Term::integer(i64::try_from(value).expect("arities fit an i64"))
}

/// Returns an operation applied to arguments, as a term.
fn applied(operator: Operator, arguments: Vec<Term>, span: Option<Span>) -> Term {
    Term {
        kind: TermKind::Operation(Operation {
            operator,
            arguments,
            span,
        }),
        span,
    }
}

/// Desugars a structured body argument into a pattern over its column variable.
///
/// One guard condition proves the whole shape, reading nothing but the column variable:
/// domains, lengths, labels, ground parts, and key membership, nested structures included.
/// A mismatched row is an ordinary non-match, not an evaluation error,
/// since a false conjunct absorbs the errors the accessors raise on a wrong shape
/// (`And(false, error)` is false).
/// Since the guard proves the whole shape, the extractions cannot fail.
///
/// Each variable position becomes an equality between the term written there and a chain of
/// accessor calls.
/// The operation fixpoint makes it an assignment if it binds a fresh variable,
/// and a filter if the variable is already bound.
/// Matching is exact: a map pattern names every key its rows hold.
pub(super) fn destructure(
    target: &Term,
    pattern: &Term,
    synthesized: &mut Vec<Operation>,
) -> Result<(), Diagnostic> {
    let mut conditions = Vec::new();
    shape_conditions(target.clone(), pattern, &mut conditions)?;
    synthesized.push(Operation {
        operator: Operator::And,
        arguments: conditions
            .into_iter()
            .map(TermKind::Operation)
            .map(Term::new)
            .collect(),
        span: pattern.span,
    });
    extractions(target.clone(), pattern, synthesized);
    Ok(())
}

/// Collects the conditions that prove a value at the access term has a pattern's shape
/// (see [destructure]).
fn shape_conditions(
    access: Term,
    pattern: &Term,
    conditions: &mut Vec<Operation>,
) -> Result<(), Diagnostic> {
    let span = pattern.span;
    let equals = |left: Term, right: Term| Operation {
        operator: Operator::Equal,
        arguments: vec![left, right],
        span,
    };
    match &pattern.kind {
        TermKind::Tuple(elements) => {
            conditions.push(Operation {
                operator: Operator::IsTuple,
                arguments: vec![access.clone()],
                span,
            });
            conditions.push(equals(
                applied(Operator::Length, vec![access.clone()], span),
                integer(elements.len()),
            ));
            conditions.push(equals(
                applied(Operator::Label, vec![access.clone()], span),
                Term::string(""),
            ));
            for (index, element) in elements.iter().enumerate() {
                position_conditions(&access, integer(index), element, conditions)?;
            }
        }
        TermKind::Function {
            name,
            arguments,
            labels,
        } => {
            if labels.is_some() {
                return Err(TranslationError::LabeledTupleNamedFields.at(span));
            }
            conditions.push(Operation {
                operator: Operator::IsTuple,
                arguments: vec![access.clone()],
                span,
            });
            conditions.push(equals(
                applied(Operator::Length, vec![access.clone()], span),
                integer(arguments.len()),
            ));
            conditions.push(equals(
                applied(Operator::Label, vec![access.clone()], span),
                Term {
                    kind: TermKind::Constant(name.clone()),
                    span,
                },
            ));
            for (index, argument) in arguments.iter().enumerate() {
                position_conditions(&access, integer(index), argument, conditions)?;
            }
        }
        TermKind::Map { label, pairs } => {
            conditions.push(Operation {
                operator: Operator::IsMap,
                arguments: vec![access.clone()],
                span,
            });
            conditions.push(equals(
                applied(Operator::Length, vec![access.clone()], span),
                integer(pairs.len()),
            ));
            conditions.push(equals(
                applied(Operator::Label, vec![access.clone()], span),
                match label {
                    None => Term::string(""),
                    Some(name) => Term {
                        kind: TermKind::Constant(name.clone()),
                        span,
                    },
                },
            ));
            for (key, value) in pairs {
                if has_pattern_variables(key) {
                    return Err(unsupported("a variable in a map pattern's key", key.span));
                }
                conditions.push(Operation {
                    operator: Operator::HasKey,
                    arguments: vec![access.clone(), key.clone()],
                    span,
                });
                position_conditions(&access, key.clone(), value, conditions)?;
            }
        }
        _ => unreachable!("only structured terms are destructured"),
    }
    Ok(())
}

/// Returns the shape conditions one position of a pattern contributes.
///
/// A ground part is checked in place,
/// and a nested pattern recurses through a chained access.
/// A variable or expression position contributes nothing,
/// since its value is the extraction's concern.
fn position_conditions(
    access: &Term,
    key: Term,
    subterm: &Term,
    conditions: &mut Vec<Operation>,
) -> Result<(), Diagnostic> {
    if is_pattern(subterm) {
        let element = applied(Operator::Get, vec![access.clone(), key], subterm.span);
        return shape_conditions(element, subterm, conditions);
    }
    if has_pattern_variables(subterm) {
        return Ok(());
    }
    let element = applied(Operator::Get, vec![access.clone(), key], subterm.span);
    conditions.push(Operation {
        operator: Operator::Equal,
        arguments: vec![element, subterm.clone()],
        span: subterm.span,
    });
    Ok(())
}

/// Synthesizes the value equalities of a pattern's variable positions (see [destructure]).
///
/// It runs after [shape_conditions] proved the pattern well-formed, so it cannot fail.
fn extractions(access: Term, pattern: &Term, synthesized: &mut Vec<Operation>) {
    let positions: Vec<(Term, &Term)> = match &pattern.kind {
        TermKind::Tuple(elements) => elements
            .iter()
            .enumerate()
            .map(|(index, element)| (integer(index), element))
            .collect(),
        TermKind::Function { arguments, .. } => arguments
            .iter()
            .enumerate()
            .map(|(index, argument)| (integer(index), argument))
            .collect(),
        TermKind::Map { pairs, .. } => pairs
            .iter()
            .map(|(key, value)| (key.clone(), value))
            .collect(),
        _ => return,
    };
    for (key, subterm) in positions {
        if !(has_pattern_variables(subterm)) {
            // A ground part: the guard already checked it.
            continue;
        }
        if matches!(&subterm.kind, TermKind::Variable(variable)
            if variable.kind == VariableKind::Anonymous)
        {
            // `_` matches anything and binds nothing.
            continue;
        }
        let element = applied(Operator::Get, vec![access.clone(), key], subterm.span);
        if is_structured(subterm) {
            extractions(element, subterm, synthesized);
        } else {
            synthesized.push(Operation {
                operator: Operator::Equal,
                arguments: vec![subterm.clone(), element],
                span: subterm.span,
            });
        }
    }
}

impl<'a> Translator<'a> {
    /// Evaluates the ground terms a structured value is built from.
    fn elements(&self, terms: &[Term]) -> Result<Vec<DataValue>, Diagnostic> {
        terms.iter().map(|term| self.evaluate(term)).collect()
    }

    /// Evaluates a ground term into a value.
    pub(crate) fn evaluate(&self, term: &Term) -> Result<DataValue, Diagnostic> {
        match &term.kind {
            TermKind::Constant(name) => Ok(DataValue::iri(self.resolve(name, term.span)?)),
            TermKind::String(value) => Ok(DataValue::string(value.as_str())),
            TermKind::LanguageString { text, language } => {
                DataValue::language_string(text.as_str(), language).ok_or_else(|| {
                    TranslationError::InvalidLanguageTag {
                        language: language.clone(),
                    }
                    .at(term.span)
                })
            }
            TermKind::Boolean(value) => Ok(DataValue::boolean(*value)),
            TermKind::Number(value) => Ok(DataValue::from(value.clone())),
            TermKind::RdfLiteral { lexical, datatype } => {
                let datatype = self.resolve(datatype, term.span)?;
                DataValue::typed_literal(&datatype, lexical)
                    .ok_or_else(|| TranslationError::InvalidLiteral.at(term.span))
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                self.global(&variable.name, term.span)
            }
            TermKind::Variable(_) => Err(TranslationError::NotGround.at(term.span)),
            TermKind::BlankNode(label) => {
                let id = *self
                    .blanks
                    .get(label)
                    .expect("blank node labels get their nulls before their terms translate");
                Ok(DataValue::null(id))
            }
            TermKind::Tuple(elements) => {
                Ok(DataValue::tuple(TupleValue::new(self.elements(elements)?)))
            }
            // A function term is a labeled tuple: `point(1, 2)` names
            // the shape its elements have, by position. Named columns
            // belong to atoms, whose declaration resolves them.
            TermKind::Function {
                name,
                arguments,
                labels,
            } => {
                if labels.is_some() {
                    return Err(TranslationError::LabeledTupleNamedFields.at(term.span));
                }
                let label = self.resolve(name, term.span)?;
                Ok(DataValue::tuple(TupleValue::labeled(
                    label,
                    self.elements(arguments)?,
                )))
            }
            TermKind::Map { label, pairs } => {
                let mut entries = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    entries.push((self.evaluate(key)?, self.evaluate(value)?));
                }
                if let Some(position) = first_repeat(entries.iter().map(|(key, _)| key)) {
                    return Err(duplicate_key(&pairs[position].0, term.span));
                }
                let map = match label {
                    None => MapValue::new(entries),
                    Some(name) => MapValue::labeled(self.resolve(name, term.span)?, entries),
                };
                Ok(DataValue::map(map.expect("a map of distinct keys builds")))
            }
            TermKind::Operation(operation) => {
                // A ground expression evaluates right here.
                let mut builder = StackProgramBuilder::default();
                builder.operation(self, VariableScope::Ground, operation)?;
                let program = builder.finish(term.span)?;
                let mut stack = Vec::with_capacity(program.stack_bound());
                match program.evaluate(self.context, &mut stack, &[]).as_result() {
                    Ok(value) => Ok(value.to_owned()),
                    Err(error) => {
                        let problem = TranslationError::ConstantExpressionErrors.diagnostic();
                        Err(match term.span {
                            Some(span) => problem.with_label(span, error.to_string()),
                            None => problem.with_note(error.to_string()),
                        })
                    }
                }
            }
            TermKind::Aggregate(aggregate) => {
                Err(TranslationError::AggregateOutsideHead.at(aggregate.span))
            }
        }
    }

    /// Returns the error for the first ground key of a map that repeats an earlier one,
    /// labeled at the repetition, or `None` if no ground key repeats.
    ///
    /// Keys calling a nondeterministic function are not compared,
    /// since no two evaluations of them need agree.
    pub(super) fn repeated_key(
        &self,
        pairs: &[(Term, Term)],
        span: Option<Span>,
    ) -> Option<Diagnostic> {
        let known: Vec<(&Term, DataValue)> = pairs
            .iter()
            .filter(|(key, _)| key.is_ground() && !is_volatile(key))
            .filter_map(|(key, _)| Some((key, self.evaluate(key).ok()?)))
            .collect();
        let position = first_repeat(known.iter().map(|(_, value)| value))?;
        Some(duplicate_key(known[position].0, span))
    }
}

/// Returns the position of the first value that equals an earlier one.
fn first_repeat<'v>(values: impl IntoIterator<Item = &'v DataValue>) -> Option<usize> {
    let mut seen = BTreeSet::new();
    values.into_iter().position(|value| !seen.insert(value))
}

/// Returns the error for a map key written again, labeled at the repetition.
fn duplicate_key(key: &Term, span: Option<Span>) -> Diagnostic {
    TranslationError::DuplicateMapKey {
        key: key.to_string(),
    }
    .at(key.span.or(span))
}
