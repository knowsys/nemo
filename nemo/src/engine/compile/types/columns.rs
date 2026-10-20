//! A rule's columns: what each can hold, narrowed by the rule's conditions,
//! and what the rule writes into the tables.

use std::collections::HashMap;

use nemo_physical::{
    function::Outcome,
    management::TableId,
    value::{DomainSet, LabelTypes, Type, ValueDomain},
};

use crate::{
    engine::compile::compiled::{CompiledAggregate, CompiledRule, ExistentialTerm},
    model::AggregateKind,
};

/// Returns the types each of a rule's columns can hold.
///
/// Variables intersect the table columns they join,
/// bound columns additionally what their expressions produce,
/// and computed columns hold what their expressions produce.
/// The rule's conditions, bindings and computed columns then narrow the columns they read:
/// a row is kept only if every condition holds, every binding yields its column's value,
/// and every computed column evaluates.
pub(super) fn rule_columns(
    rule: &CompiledRule,
    types: &HashMap<TableId, Vec<Type>>,
    labels: &LabelTypes,
) -> Vec<Type> {
    columns_of(rule, types, labels, true)
}

/// Returns the types each of a rule's columns can hold before its conditions narrow them:
/// the columns a proof that the rule cannot fire is told in.
pub(super) fn joined_columns(
    rule: &CompiledRule,
    types: &HashMap<TableId, Vec<Type>>,
    labels: &LabelTypes,
) -> Vec<Type> {
    columns_of(rule, types, labels, false)
}

/// Returns the types each of a rule's columns can hold,
/// narrowed by its conditions and bindings if `narrowing` is set; see [rule_columns].
fn columns_of(
    rule: &CompiledRule,
    types: &HashMap<TableId, Vec<Type>>,
    labels: &LabelTypes,
    narrowing: bool,
) -> Vec<Type> {
    let mut columns = vec![Type::ANY; rule.variables + rule.computed.len()];
    for atom in &rule.body {
        let table = &types[&atom.table];
        for (position, &variable) in atom.variables.iter().enumerate() {
            columns[variable] = columns[variable].intersect(&table[position]);
        }
    }
    // Expressions read columns that other expressions narrow. The loop
    // ends because the columns only shrink.
    loop {
        let mut changed = false;
        for binding in &rule.bindings {
            let inputs = read(&columns, &binding.inputs);
            let refined = columns[binding.target]
                .intersect(&binding.program.infer(&inputs, labels).into_bound());
            if refined != columns[binding.target] {
                columns[binding.target] = refined;
                changed = true;
            }
            if narrowing {
                let outcome = Outcome::Within(columns[binding.target].clone());
                let narrowed = binding.program.narrow(&inputs, labels, &outcome);
                changed |= narrow(&mut columns, &binding.inputs, narrowed);
            }
        }
        if narrowing {
            for filter in &rule.filters {
                let inputs = read(&columns, &filter.inputs);
                let narrowed = filter.program.narrow(&inputs, labels, &Outcome::Holds);
                changed |= narrow(&mut columns, &filter.inputs, narrowed);
            }
        }
        for (index, computed) in rule.computed.iter().enumerate() {
            let column = rule.variables + index;
            let inputs = read(&columns, &computed.inputs);
            let refined =
                columns[column].intersect(&computed.program.infer(&inputs, labels).into_bound());
            if refined != columns[column] {
                columns[column] = refined;
                changed = true;
            }
            // A row whose computed column errors is dropped.
            if narrowing {
                let outcome = Outcome::Within(columns[column].clone());
                let narrowed = computed.program.narrow(&inputs, labels, &outcome);
                changed |= narrow(&mut columns, &computed.inputs, narrowed);
            }
        }
        if !changed {
            break;
        }
    }
    columns
}

/// Returns the types of the columns an expression reads, in input order.
pub(super) fn read(columns: &[Type], inputs: &[usize]) -> Vec<Type> {
    inputs.iter().map(|&input| columns[input].clone()).collect()
}

/// Intersects the columns an expression reads with their narrowed types, in input order,
/// returning whether any column shrank.
fn narrow(columns: &mut [Type], inputs: &[usize], narrowed: Vec<Type>) -> bool {
    let mut changed = false;
    for (&input, narrowed) in inputs.iter().zip(narrowed) {
        let refined = columns[input].intersect(&narrowed);
        if refined != columns[input] {
            columns[input] = refined;
            changed = true;
        }
    }
    changed
}

/// Calls the action for every column a rule writes: its head atoms, its
/// aggregate head, and its existential inserts,
/// with the rule column the written values come from, if one does.
pub(super) fn each_write(
    rule: &CompiledRule,
    columns: &[Type],
    labels: &LabelTypes,
    mut action: impl FnMut(TableId, usize, &Type, Option<usize>),
) {
    for head in &rule.heads {
        for (position, &column) in head.columns.iter().enumerate() {
            action(head.table, position, &columns[column], Some(column));
        }
    }
    if let Some(aggregate) = &rule.aggregate {
        for (position, written) in aggregate_output(aggregate, columns, labels)
            .iter()
            .enumerate()
        {
            action(aggregate.table, position, written, None);
        }
    }
    if let Some(existential) = &rule.existential {
        for atom in &existential.inserts {
            for (position, term) in atom.columns.iter().enumerate() {
                let (written, origin) = match term {
                    ExistentialTerm::Frontier(index) => {
                        let column = existential.frontier[*index];
                        (columns[column].clone(), Some(column))
                    }
                    ExistentialTerm::Existential(_) => (Type::of(ValueDomain::Null), None),
                    ExistentialTerm::Constant(value) => (Type::of_value(value.as_ref()), None),
                };
                action(atom.table, position, &written, origin);
            }
        }
    }
}

/// Returns the types an aggregate head's columns can hold.
fn aggregate_output(
    aggregate: &CompiledAggregate,
    columns: &[Type],
    labels: &LabelTypes,
) -> Vec<Type> {
    use crate::engine::compile::compiled::AggregateColumn;

    let results: Vec<Type> = aggregate
        .functions
        .iter()
        .map(|function| {
            // `#count()` has no input; every other aggregate does.
            let input = || function.input.expect("only a match count has no input");
            match function.kind {
                AggregateKind::Count => Type::of(ValueDomain::Integer),
                // A sum adds its inputs, and an average divides the sum by the count,
                // so the numbers promote as they do under `+` and `/`:
                // an exact sum that is integral is an integer, and an exact average any exact
                // number. Durations sum and average to durations of one kind.
                // Extrema return one of their inputs, whatever its type.
                AggregateKind::Sum | AggregateKind::Average => {
                    let domains = columns[input()].domains();
                    let mut result = domains & DomainSet::NUMERIC;
                    if domains.intersects(DomainSet::EXACT) {
                        result |= match function.kind {
                            AggregateKind::Average => DomainSet::EXACT,
                            _ if domains.contains(ValueDomain::Decimal) => DomainSet::EXACT,
                            _ => DomainSet::INTEGER,
                        };
                    }
                    if domains.intersects(DomainSet::DURATION) {
                        result |= DomainSet::ORDERED_DURATION;
                    }
                    Type::of_domains(result)
                }
                AggregateKind::Min | AggregateKind::Max => columns[input()].clone(),
                // Arities and keys are per-group facts, not types.
                AggregateKind::Tuple => Type::of(ValueDomain::Tuple),
                AggregateKind::Map => Type::of(ValueDomain::Map),
            }
        })
        .collect();
    let space: Vec<Type> = aggregate
        .group
        .iter()
        .map(|&group| columns[group].clone())
        .chain(results.iter().cloned())
        .collect();
    let post: Vec<Type> = aggregate
        .post
        .iter()
        .map(|computed| {
            let inputs: Vec<Type> = computed
                .inputs
                .iter()
                .map(|&input| space[input].clone())
                .collect();
            computed.program.infer(&inputs, labels).into_bound()
        })
        .collect();
    aggregate
        .columns
        .iter()
        .map(|reference| match reference {
            AggregateColumn::Group(index) => columns[aggregate.group[*index]].clone(),
            AggregateColumn::Result(index) => results[*index].clone(),
            AggregateColumn::Post(index) => post[*index].clone(),
        })
        .collect()
}
