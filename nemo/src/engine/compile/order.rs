//! Variable orders: the order in which a rule's join binds its variables.
//!
//! The order decides which variables the join binds first, and so how early it can prune.
//! The order in which a rule is *written* is the starting point, and it is usually a good one,
//! since the author knows which atom is small and leads with it.
//! Only these reasons override it:
//! - A variable that shares no atom with a variable bound before it would open a cartesian
//!   product, so [connected] passes it over.
//! - A column bound to an expression must come after the variables that the expression reads,
//!   or the join cannot probe it, so [probing] delays it.
//! - A self-recursive rule may be led by a head variable (see [head_leader]).
//!
//! Orders are chosen before the tables contain any data,
//! so no ranking can distinguish a large relation from a small one,
//! and deviating from the written order without a structural reason would be guesswork.

use crate::engine::compile::compiled::{CompiledRule, NegatedAtom, NegatedColumn, VariableId};

/// Chooses the variable order of every rule, in place.
///
/// One order serves all of a rule's semi-naive variants.
pub(crate) fn choose(rules: &mut [CompiledRule]) {
    for rule in rules {
        rule.order = choose_for(rule);
    }
}

/// Chooses one rule's variable order.
fn choose_for(rule: &CompiledRule) -> Vec<VariableId> {
    let order = ordered(rule, &[]);
    match head_leader(rule, &order) {
        Some(leader) => ordered(rule, &[leader]),
        None => order,
    }
}

/// Builds an order from the written one, starting with the `seeds`.
///
/// The variables after the seeds are chosen as without seeds:
/// each next variable is the first remaining one that [connected] and [probing] keep.
fn ordered(rule: &CompiledRule, seeds: &[VariableId]) -> Vec<VariableId> {
    let mut order: Vec<VariableId> = Vec::with_capacity(rule.variables);
    let mut remaining: Vec<VariableId> = (0..rule.variables).collect();
    for &seed in seeds {
        order.push(seed);
        remaining.retain(|&variable| variable != seed);
    }

    while !remaining.is_empty() {
        let candidates = probing(connected(remaining.clone(), rule, &order), rule, &order);
        let next = candidates
            .first()
            .copied()
            .expect("every filter keeps at least one candidate");
        order.push(next);
        remaining.retain(|&variable| variable != next);
    }
    order
}

/// Returns a head variable to lead the order with,
/// if the chosen order of a self-recursive rule leads with a variable that is not in the head.
///
/// During the join, head rows are deduplicated within the groups of rows that share the
/// order's leading variables.
/// If the leading variable is not in the head, there are no such groups.
/// Then, when the join's output is processed in parallel chunks,
/// each chunk derives again the head rows that the other chunks also find.
/// Leading with the head variable that the body mentions first restores the grouping.
///
/// Only self-recursive rules qualify,
/// since their repeated applications produce the most duplicate head rows,
/// and for a rule that is not recursive, the selectivity of the written order matters more.
/// The rule must also have a single head atom, and no aggregate, existential variable,
/// nondeterministic function, or negated atom that shares a column with the body.
/// The new leading variable must occur in a body atom, and no binding may compute it.
fn head_leader(rule: &CompiledRule, order: &[VariableId]) -> Option<VariableId> {
    if rule.heads.len() != 1
        || rule.aggregate.is_some()
        || rule.existential.is_some()
        || rule.nondeterministic
        || rule.negated.iter().any(NegatedAtom::shares_columns)
        || !rule
            .body
            .iter()
            .any(|atom| rule.heads.iter().any(|head| head.table == atom.table))
    {
        return None;
    }
    let head = &rule.heads[0];
    let first = *order.first()?;
    if head.columns.contains(&first) {
        return None;
    }
    head.columns
        .iter()
        .copied()
        .filter(|&column| column < rule.variables)
        .min()
        .filter(|&leader| {
            rule.body
                .iter()
                .any(|atom| atom.variables.contains(&leader))
                && rule.bindings.iter().all(|binding| binding.target != leader)
        })
}

/// Returns the candidates that share an atom with an ordered variable,
/// since binding any other variable starts a cartesian product.
///
/// Negated atoms count, since their shared columns prune the join as positive atoms do.
/// If no candidate shares an atom with an ordered variable, the rule's body is disconnected,
/// every candidate is as good as the next, and all candidates are returned.
fn connected(
    candidates: Vec<VariableId>,
    rule: &CompiledRule,
    order: &[VariableId],
) -> Vec<VariableId> {
    if order.is_empty() {
        return candidates;
    }
    let kept: Vec<VariableId> = candidates
        .iter()
        .copied()
        .filter(|&variable| {
            atoms_of(rule, variable).any(|columns| {
                columns
                    .iter()
                    .any(|column| order.contains(column) && *column != variable)
            })
        })
        .collect();
    if kept.is_empty() { candidates } else { kept }
}

/// Returns the candidates that are ready, so that a column bound to an expression is
/// *probed* rather than enumerated.
///
/// A variable is ready unless a binding computes it from variables that are not yet ordered.
/// Such a column cannot be probed, so the join would enumerate the column and filter it
/// instead, which is what the binding exists to avoid.
///
/// If no candidate is ready, the function returns the inputs that the blocked bindings still
/// wait for, since an atom's column that binds them may be all that connects them,
/// and ordering an input first keeps the probe.
/// If there are no such inputs, because bindings read each other in a cycle,
/// it returns all candidates, and the rule does without the probe.
fn probing(
    candidates: Vec<VariableId>,
    rule: &CompiledRule,
    order: &[VariableId],
) -> Vec<VariableId> {
    let blocked = |variable: VariableId| {
        rule.bindings
            .iter()
            .filter(|binding| binding.target == variable)
            .any(|binding| binding.inputs.iter().any(|input| !order.contains(input)))
    };
    let kept: Vec<VariableId> = candidates
        .iter()
        .copied()
        .filter(|&variable| !blocked(variable))
        .collect();
    if !kept.is_empty() {
        return kept;
    }
    let mut inputs: Vec<VariableId> = Vec::new();
    for &variable in &candidates {
        for binding in rule.bindings.iter().filter(|b| b.target == variable) {
            for &input in &binding.inputs {
                if !order.contains(&input)
                    && !candidates.contains(&input)
                    && !inputs.contains(&input)
                {
                    inputs.push(input);
                }
            }
        }
    }
    inputs.sort_unstable();
    if inputs.is_empty() {
        candidates
    } else {
        inputs
    }
}

/// Returns the variables of each atom that a variable occurs in:
/// first those of the positive body atoms, then the shared columns of the negated atoms.
fn atoms_of(rule: &CompiledRule, variable: VariableId) -> impl Iterator<Item = Vec<VariableId>> {
    let positive = rule
        .body
        .iter()
        .map(|atom| atom.variables.clone())
        .filter(move |columns| columns.contains(&variable));
    let negated = rule
        .negated
        .iter()
        .map(|atom| {
            atom.columns
                .iter()
                .filter_map(|column| match column {
                    NegatedColumn::Shared(id) => Some(*id),
                    NegatedColumn::Private => None,
                })
                .collect::<Vec<_>>()
        })
        .filter(move |columns| columns.contains(&variable));
    positive.chain(negated)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{engine::Engine, testing::parse};

    /// Returns the order that the engine chooses for the first rule of a program,
    /// as variable ids.
    fn order_of(source: &str) -> Vec<VariableId> {
        let program = parse(source);
        let mut engine = Engine::new();
        engine.load(program).expect("the program loads");
        engine.join_order(0).expect("the program has a rule")
    }

    /// A variable that shares no atom with the variables ordered before it is passed over.
    #[test]
    fn disconnected_variables_are_passed_over() {
        // The written order is ?a, ?b, ?c, ?d. Binding ?c third would open a
        // cartesian product, since neither ?a nor ?b constrains it.
        // ?d shares r with ?b, so it comes first, and ?c follows it.
        let order = order_of(
            "
            p(1, 2) . q(3, 4) . r(2, 4) .
            link(?a, ?c) :- p(?a, ?b), q(?c, ?d), r(?b, ?d) .
        ",
        );
        assert_eq!(order, vec![0, 1, 3, 2], "the join stays connected");
    }

    /// A connected body keeps the order in which it was written.
    #[test]
    fn a_connected_body_keeps_the_order_it_was_written_in() {
        // Every variable already follows a variable that it shares an atom with,
        // so there is nothing to correct,
        // and the written order reads both atoms in their storage order.
        let order = order_of(
            "
            p(1, 2) . q(2, 3) .
            joined(?x, ?y, ?z) :- p(?x, ?y), q(?y, ?z) .
        ",
        );
        assert_eq!(order, vec![0, 1, 2], "the written order stands");
    }

    /// A column bound to an expression comes after the variables that the expression reads.
    #[test]
    fn bound_columns_follow_their_inputs() {
        // The first column of e is bound to ?x + 1. Ordering it before ?x would turn
        // the probe into a filter over an enumerated column.
        let order = order_of(
            "
            d(1) . d(2) . e(2, 5) .
            near(?x, ?v) :- d(?x), e(?x + 1, ?v) .
        ",
        );
        let position = |variable: VariableId| {
            order
                .iter()
                .position(|&other| other == variable)
                .expect("every variable is ordered")
        };
        // ?x has id 0, and the bound column of e has id 1.
        assert!(position(0) < position(1), "the probe's input comes first");
    }
}
