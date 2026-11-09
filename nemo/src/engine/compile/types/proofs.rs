//! The proofs that a rule can never fire,
//! and the hint for a type conflict in which a label is close to a builtin's name.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::Inferred,
    management::TableId,
    value::{DomainSet, LabelBound, LabelTypes, Shape, Type},
};

use crate::{
    diagnostics::TypeError,
    engine::compile::{
        compiled::{CompiledRule, FilterOrigin, RuleFilter, VariableId},
        import_plan::ImportPlan,
        types::{RuleSourceInfo, TableNames, written_column},
    },
    model::{AggregateKind, Span},
    values::TypeSyntax,
};

/// One reason why a rule can never fire,
/// with the position of what it is about, if that is more precise than the rule's.
struct Proof {
    /// The position of what the reason is about.
    span: Option<Span>,
    /// The reason.
    text: String,
}

impl Proof {
    /// Creates a reason about the given position.
    fn at(span: Option<Span>, text: impl Into<String>) -> Self {
        Self {
            span,
            text: text.into(),
        }
    }
}

/// What the proofs that a rule can never fire read besides the rule.
pub(super) struct ProofContext<'a> {
    /// What the reports need to know of the rule as it was written.
    pub(super) source: RuleSourceInfo<'a>,
    /// The inferred column types per table.
    pub(super) types: &'a HashMap<TableId, Vec<Type>>,
    /// The name and the arity of each table.
    pub(super) table_names: &'a TableNames<'a>,
    /// The type that each `@type` declares for its label.
    pub(super) labels: &'a LabelTypes,
    /// What the loads of imported predicates leave out.
    pub(super) plan: &'a ImportPlan,
}

/// Returns the warning for a rule that provably never fires, with its proofs as labels and
/// notes, or `None` if no proof exists.
///
/// The proofs are told in terms of the `columns` that the joins give,
/// before the conditions narrow them.
/// Only if those prove nothing does a variable whose `narrowed` type is empty prove it.
pub(super) fn never_fires_warning(
    rule: &CompiledRule,
    context: &ProofContext<'_>,
    columns: &[Type],
    narrowed: &[Type],
) -> Option<Diagnostic> {
    let source = context.source;
    let mut proofs = rule_proofs(rule, context, columns);
    if proofs.is_empty() {
        let narrowing = if rule.computed.is_empty() {
            "conditions"
        } else {
            "conditions and assignments"
        };
        proofs.extend(
            narrowed
                .iter()
                .take(rule.variables)
                .enumerate()
                .filter(|(_, column)| column.is_empty())
                .map(|(variable, _)| {
                    Proof::at(
                        None,
                        format!(
                            "no value of {} satisfies the rule's {narrowing}",
                            variable_name(source, variable)
                        ),
                    )
                }),
        );
    }
    let (first, rest) = proofs.split_first()?;
    let mut warning = TypeError::NeverFires.diagnostic();
    warning = match first.span.or(source.span) {
        Some(span) => warning.with_label(span, first.text.clone()),
        None => warning.with_note(first.text.clone()),
    };
    for proof in rest {
        warning = match proof.span {
            Some(span) => warning.with_label(span, proof.text.clone()),
            None => warning.with_note(proof.text.clone()),
        };
    }
    // A rule that never fires and has a tuple labeled close to a builtin's name
    // probably misspells the builtin, so the builtin is offered as a hint.
    if let Some(hint) = columns.iter().find_map(builtin_hint) {
        warning = warning.with_hint(hint);
    }
    Some(warning)
}

/// Returns the proofs that a rule can never fire, or none if there is no proof.
fn rule_proofs(rule: &CompiledRule, context: &ProofContext<'_>, columns: &[Type]) -> Vec<Proof> {
    let mut proofs = Vec::new();

    for (variable, column) in columns.iter().enumerate().take(rule.variables) {
        if column.is_empty() {
            proofs.push(variable_proof(rule, context, variable));
        }
    }
    // The columns that a pattern that never matches tests.
    // What is extracted from them follows from the pattern, and is not reported again.
    let mut dead_patterns: Vec<VariableId> = Vec::new();
    for filter in &rule.filters {
        if filter.inputs.iter().any(|&input| columns[input].is_empty()) {
            continue;
        }
        let inputs: Vec<Type> = filter
            .inputs
            .iter()
            .map(|&input| columns[input].clone())
            .collect();
        // An exact result decides the condition outright, which is how
        // a pattern's guard against a known shape is proven never to hold.
        let outcome = match filter.program.infer(&inputs, context.labels) {
            Inferred::Exact(exact) => match exact.as_ref().effective_boolean_value() {
                Ok(true) => continue,
                Ok(false) => Outcome::False,
                Err(_) => Outcome::Fails,
            },
            Inferred::Bound(result) => {
                if result.domains().intersects(DomainSet::TRUTHY) {
                    continue;
                }
                if result.is_empty() {
                    Outcome::Fails
                } else {
                    Outcome::Never(result)
                }
            }
        };
        if matches!(filter.origin, FilterOrigin::Pattern(_)) {
            dead_patterns.extend(&filter.inputs);
        }
        proofs.push(filter_proof(rule, context, filter, &inputs, &outcome));
    }
    for (index, computed) in rule.computed.iter().enumerate() {
        // A computed column is reported only if all its inputs can hold values,
        // since empty inputs are reported for their own variables.
        // Filters are reported first, since a pattern that never matches
        // explains more than the empty extractions that follow from it.
        let column = rule.variables + index;
        if columns[column].is_empty()
            && computed
                .inputs
                .iter()
                .all(|&input| !columns[input].is_empty() && !dead_patterns.contains(&input))
        {
            let text = match source_name(context.source, column) {
                Some(name) => format!("{name} can never be computed"),
                // Every unnamed computed column that can be empty is an
                // expression in the head or in its aggregate.
                None => String::from("an expression in the head can never be computed"),
            };
            proofs.push(Proof::at(None, text));
        }
    }
    if let Some(aggregate) = &rule.aggregate {
        for function in &aggregate.functions {
            if matches!(function.kind, AggregateKind::Sum | AggregateKind::Average)
                && let Some(input) = function.input
                && !columns[input].is_empty()
                && !columns[input]
                    .domains()
                    .intersects(DomainSet::NUMERIC | DomainSet::ORDERED_DURATION)
            {
                proofs.push(Proof::at(
                    None,
                    format!(
                        "the aggregated values have type {}, never numbers or durations",
                        TypeSyntax::of(&columns[input])
                    ),
                ));
            }
        }
    }
    proofs
}

/// How a filter that provably never holds fails every row.
enum Outcome {
    /// It is always false.
    False,
    /// It always fails to evaluate.
    Fails,
    /// It always evaluates to a value of this type, which is never true.
    Never(Type),
}

/// Returns why a filter fails every row,
/// told in terms of the part of the rule's text that the filter comes from.
///
/// `inputs` contains the types of the filter's inputs.
fn filter_proof(
    rule: &CompiledRule,
    context: &ProofContext<'_>,
    filter: &RuleFilter,
    inputs: &[Type],
    outcome: &Outcome,
) -> Proof {
    let fails = |what: &str| match outcome {
        Outcome::False => format!("{what} is always false"),
        Outcome::Fails => format!("{what} always fails to evaluate"),
        Outcome::Never(result) => format!(
            "{what} always evaluates to {}, which is never true",
            TypeSyntax::of(result)
        ),
    };
    let is_false = matches!(outcome, Outcome::False);
    let text = match filter.origin {
        // A shape test reads only the value it tests.
        FilterOrigin::Pattern(_) if is_false && inputs.len() == 1 => format!(
            "this pattern never matches a value of type {}",
            TypeSyntax::of(&inputs[0])
        ),
        FilterOrigin::Pattern(_) if is_false => String::from("this pattern never matches"),
        FilterOrigin::Pattern(_) => fails("this pattern"),
        FilterOrigin::Constant(_) if is_false && inputs.len() == 1 => {
            match atom_column(rule, context, filter.inputs[0]) {
                Some((predicate, column)) => format!(
                    "column {} of `{predicate}` holds {}, never this constant",
                    column + 1,
                    TypeSyntax::of(&inputs[0])
                ),
                None => format!(
                    "this column holds {}, never this constant",
                    TypeSyntax::of(&inputs[0])
                ),
            }
        }
        FilterOrigin::Constant(_) => fails("this constant"),
        // The filter compares the later occurrence with the earlier one.
        FilterOrigin::Repeated(_) if is_false && inputs.len() == 2 => format!(
            "{} would have to be {} and {} at once",
            variable_name(context.source, filter.inputs[1]),
            TypeSyntax::of(&inputs[1]),
            TypeSyntax::of(&inputs[0])
        ),
        FilterOrigin::Repeated(_) => fails("this repeated variable"),
        FilterOrigin::Expression(_) if is_false => {
            String::from("this argument never equals a value of its column")
        }
        FilterOrigin::Expression(_) => fails("this argument"),
        FilterOrigin::Condition(_) => fails("this condition"),
    };
    Proof::at(filter.origin.span(), text)
}

/// Returns the predicate and the kept column, counted from 0, that a body variable is read
/// from, or `None` if no positive atom binds it.
fn atom_column<'a>(
    rule: &CompiledRule,
    context: &ProofContext<'a>,
    variable: VariableId,
) -> Option<(&'a str, usize)> {
    rule.body.iter().find_map(|atom| {
        let position = atom.variables.iter().position(|&bound| bound == variable)?;
        let &(predicate, arity) = context.table_names.get(&atom.table)?;
        Some((
            predicate,
            written_column(context.plan, predicate, arity, position),
        ))
    })
}

/// Returns why a variable can have no value:
/// the first atom column at which the intersection of its types becomes empty.
///
/// If no atom column empties it, the expression of its binding can never be computed.
fn variable_proof(rule: &CompiledRule, context: &ProofContext<'_>, variable: usize) -> Proof {
    let source = context.source;
    let name = source_name(source, variable);

    let mut running = Type::ANY;
    for atom in &rule.body {
        for (position, &bound) in atom.variables.iter().enumerate() {
            if bound != variable {
                continue;
            }
            let column = &context.types[&atom.table][position];
            let (predicate, _) = context.table_names[&atom.table];
            if column.is_empty() {
                let text = match &name {
                    Some(name) => {
                        format!("{name} is read from `{predicate}`, which is always empty")
                    }
                    None => format!("`{predicate}` is always empty"),
                };
                return Proof::at(None, text);
            }
            let narrowed = running.intersect(column);
            if narrowed.is_empty() {
                return Proof::at(
                    None,
                    format!(
                        "{} would have to be {} and {} at once",
                        variable_name(source, variable),
                        TypeSyntax::of(&running),
                        TypeSyntax::of(column)
                    ),
                );
            }
            running = narrowed;
        }
    }
    // The atoms agree, so the expression bound to the column can have no value.
    match name {
        Some(name) => Proof::at(None, format!("{name} can never be computed")),
        None => {
            let span = rule
                .bindings
                .iter()
                .find(|binding| binding.target == variable)
                .and_then(|binding| binding.span);
            Proof::at(span, "this argument can never be computed")
        }
    }
}

/// Returns the written name of a rule column, formatted as a variable,
/// or `None` if it has none.
fn source_name(source: RuleSourceInfo<'_>, variable: usize) -> Option<String> {
    source
        .names
        .get(variable)
        .and_then(Option::as_deref)
        .map(|name| format!("`?{name}`"))
}

/// Returns how messages name a rule column:
/// by its written name, or as a joined value if it has none.
pub(super) fn variable_name(source: RuleSourceInfo<'_>, variable: usize) -> String {
    source_name(source, variable).unwrap_or_else(|| String::from("a joined value"))
}

/// Returns a hint if a type conflict involves a labeled tuple whose label is one edit away
/// from a builtin's name and has at least three characters.
///
/// `loh(?x)` where a number was expected is probably a misspelled `LOG(?x)`.
/// The hint is given only for a conflict,
/// since without type information a name close to a builtin's is just a name.
pub(crate) fn builtin_hint(written: &Type) -> Option<String> {
    let label = tuple_label(written)?;
    let builtin = crate::model::Operator::NAMES
        .iter()
        .find(|builtin| label.len() >= 3 && one_edit_apart(label, builtin))?;
    Some(format!(
        "write `{builtin}(…)` to call the builtin, since `{label}(…)` builds a labeled tuple"
    ))
}

/// Returns the first exactly known tuple label in a type, searching nested shapes too.
fn tuple_label(written: &Type) -> Option<&str> {
    written
        .tuple_shapes()?
        .iter()
        .find_map(|shape| match shape {
            Shape::Structural(shape) => match &shape.label {
                LabelBound::Exactly(Some(label)) => Some(&**label),
                _ => shape.elements.iter().find_map(tuple_label),
            },
            Shape::Declared(label) => Some(&**label),
        })
}

/// Returns whether two names are one edit apart (a substitution, insertion, or deletion),
/// compared case-insensitively.
fn one_edit_apart(written: &str, builtin: &str) -> bool {
    let written = written.to_ascii_uppercase();
    let written = written.as_bytes();
    let builtin = builtin.as_bytes();
    let (short, long) = if written.len() <= builtin.len() {
        (written, builtin)
    } else {
        (builtin, written)
    };
    if long.len() - short.len() > 1 {
        return false;
    }
    let prefix = short
        .iter()
        .zip(long)
        .take_while(|(left, right)| left == right)
        .count();
    if short.len() == long.len() {
        // One substitution: everything past the difference agrees.
        prefix < short.len() && short[prefix + 1..] == long[prefix + 1..]
    } else {
        // One insertion: the suffix realigns past the extra character.
        short[prefix..] == long[prefix + 1..]
    }
}
