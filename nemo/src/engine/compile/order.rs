//! Variable orders: the sequence in which a rule's join binds its variables.
//!
//! The order decides the join's shape:
//! which variables the join binds first, and so how early it can prune.
//! A bad one costs orders of magnitude.
//! `link(?a, ?c) :- p(?a, ?b), q(?c, ?d), r(?b, ?d)` in its written order binds `?c` third,
//! when nothing bound so far constrains it, and enumerates every `?c` for every `(?a, ?b)`.
//!
//! The order a rule is *written* in is the starting point, and it is usually a good one,
//! since the author knows which atom is small and leads with it.
//! Two structural facts override it, and nothing else does.
//! A variable sharing no atom with a variable bound before it would open a cartesian product,
//! so [connected] passes it over.
//! A column bound to an expression must come after the variables the expression reads,
//! or the join cannot probe it, so [probing] holds it back.
//!
//! Orders are chosen before the tables hold any data,
//! so no ranking can tell a large relation from a small one,
//! and deviating from the written order without a structural reason is guesswork.

use crate::engine::compile::compiled::{CompiledRule, NegatedColumn, VariableId};

/// Chooses every rule's variable order, in place.
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

/// Builds an order from the written one, optionally seeding its first variables.
///
/// The structural filters repair everything downstream of the seeds exactly as they do
/// without them.
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
/// if a self-recursive rule's chosen order leads with a variable the head projects away.
///
/// During the join, head rows are deduplicated within groups of the order's leading variables.
/// If the leading variable is not in the head, there are no such groups,
/// and when the join's output is processed in parallel chunks,
/// each chunk derives again the head rows the other chunks also find.
/// Moving the earliest-written head variable to the front restores the grouping.
///
/// Only self-recursive rules qualify,
/// since their repeated applications produce the most duplicate head rows,
/// and for a non-recursive rule the selectivity of the written order matters more.
/// The rule must also be a plain single-head rule,
/// led by a variable an atom can enumerate and no binding computes.
fn head_leader(rule: &CompiledRule, order: &[VariableId]) -> Option<VariableId> {
    if rule.heads.len() != 1
        || rule.aggregate.is_some()
        || rule.existential.is_some()
        || rule.nondeterministic
        || rule.negated.iter().any(|atom| !atom.is_guard())
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

/// Keeps the variables that share an atom with an already-ordered one,
/// since binding anything else starts a cartesian product.
///
/// Negated atoms count, since their shared columns prune the join just as positive ones do.
/// An empty result means the rule's body is disconnected,
/// and every candidate is as good as the next.
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

/// Keeps the variables that are ready to be *probed* rather than enumerated.
///
/// A column bound to an expression that reads variables not yet ordered cannot be probed.
/// The plan then filters an enumerated column instead,
/// which is what the binding exists to avoid.
///
/// A variable no binding targets is always ready.
/// If nothing is, the inputs the blocked bindings still wait for come next,
/// since an atom's own column binding them may be all that connects them,
/// and ordering the input first is what keeps the probe.
/// Bindings that read each other cyclically leave nothing to wait for,
/// and the rule proceeds without the probe.
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

/// Returns the atoms a variable occurs in, as their column variables:
/// the positive body atoms, then the negated ones' shared columns.
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

    /// Returns the engine's chosen order for the first rule of a program, as
    /// variable ids.
    fn order_of(source: &str) -> Vec<VariableId> {
        let program = parse(source);
        let mut engine = Engine::new();
        engine.load(program).expect("the program loads");
        engine.join_order(0).expect("the program has a rule")
    }

    #[test]
    fn disconnected_variables_are_passed_over() {
        // Written order is ?a, ?b, ?c, ?d, and binding ?c third opens a
        // cartesian product, since neither ?a nor ?b constrains it. ?d
        // shares r with ?b, so it goes first and ?c follows bound.
        let order = order_of(
            "
            p(1, 2) . q(3, 4) . r(2, 4) .
            link(?a, ?c) :- p(?a, ?b), q(?c, ?d), r(?b, ?d) .
        ",
        );
        assert_eq!(order, vec![0, 1, 3, 2], "the join stays connected");
    }

    #[test]
    fn a_connected_body_keeps_the_order_it_was_written_in() {
        // Every variable here already follows one it shares an atom
        // with, so there is nothing to correct, and the author's order
        // reads both atoms exactly as they are stored.
        let order = order_of(
            "
            p(1, 2) . q(2, 3) .
            joined(?x, ?y, ?z) :- p(?x, ?y), q(?y, ?z) .
        ",
        );
        assert_eq!(order, vec![0, 1, 2], "the written order stands");
    }

    #[test]
    fn bound_columns_follow_their_inputs() {
        // ?y is bound to ?x + 1: ordering ?y first would demote the
        // probe to a filter over an enumerated column.
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
        // ?x = 0, the bound column of e = 1.
        assert!(position(0) < position(1), "the probe's input comes first");
    }
}
