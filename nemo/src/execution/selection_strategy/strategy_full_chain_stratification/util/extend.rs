use std::collections::HashMap;

use crate::execution::planning::normalization::rule::NormalizedRule;
use crate::rule_model::components::term::{
    Term, operation::operation_kind::OperationKind, primitive::Primitive,
};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::{
    atom::Atom,
    ordered_atoms::{GetRuleMem, SortedHeadAtoms, ReorderedAtoms},
    unify::unify
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::planning::normalization::operation::Operation;

/// maps indices of body/head atoms of the 2nd rule to indices of head atoms of the 1st rule
#[derive(Debug, Clone, Default)]
pub struct AtomMapping(HashMap<usize, usize>);

impl AtomMapping {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn mapped<'a, T>(&'a self, domain: &'a Vec<T>) -> impl Iterator<Item = &'a T> + 'a {
        self.0.keys().copied().map(|k| &domain[k])
    }

    pub fn maxidx(&self) -> usize {
        self.0.keys().copied().max().unwrap_or(0)
    }
}

#[derive(Debug)]
pub enum CheckResult {
    Accept,
    Extend,
    Reject,
}

type CheckFn = fn(&NormalizedRule, &NormalizedRule, &AtomMapping, &Substitution) -> CheckResult;

#[derive(Debug, Default)]
pub struct Reliance {
    mu: AtomMapping,
    idx_dom: usize,
    idx_ran: usize,
}

// Check whether subset of rule2_part unifies with rule1_head, s.t. the CheckFn accepts.
pub fn extend_init<'b, 'a: 'b, T: Atom + 'static>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    check: CheckFn,
    previous_opt: Option<&Reliance>,
    mut eta: Substitution,
) -> Option<Reliance>
where
    Vec<&'a T>: GetRuleMem<'a>,
{
    let (idx_dom, idx_ran) = match previous_opt {
        Some(Reliance {
            idx_dom, idx_ran, ..
        }) => (*idx_dom, *idx_ran + 1), // look for "next" one
        None => (0, 0),
    };
    let rule1 = mem.rules[rule1_index];
    let rule2 = mem.rules[rule2_index];

    // initialize the substitution with known constant replacements (possibly from normalization)
    for op in rule1.operations().iter().chain(rule2.operations().iter()) {
        if let Operation::Operation {
            kind: OperationKind::Equal,
            subterms,
        } = op
        {
            if let [
                Operation::Primitive(Primitive::Variable(var)),
                Operation::Primitive(prim),
            ] = subterms.as_slice()
            {
                eta.insert(
                    Primitive::Variable(var.clone()),
                    Term::Primitive(prim.clone()),
                );
            }
        }
    }

    extend::<T>(
        rule1,
        rule2,
        mem.sorted_head_atoms.get(mem.rules, rule1_index),
        T::reorder(&mut mem.reordered_atoms).get(mem.rules, rule2_index),
        check,
        &mut AtomMapping::new(),
        eta,
        idx_dom,
        idx_ran,
    )
}

fn extend<T: Atom>(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    rule1_head: &SortedHeadAtoms,
    rule2_part: &ReorderedAtoms<'_, T>,
    check: CheckFn,
    mu: &mut AtomMapping,
    eta: Substitution,
    idx_dom: usize,
    idx_ran: usize,
) -> Option<Reliance> {
    for i in idx_dom..rule2_part.len() {
        let atom_i = rule2_part[i];

        debug_assert!(
            !mu.0.contains_key(&i),
            "extend tried to change previous mapping"
        );

        let (ran_start, ran_end) = match rule1_head.ranges.get(&atom_i.pred()).copied() {
            Some((start, end)) => (
                if i == idx_dom && idx_ran > start {
                    idx_ran
                } else {
                    start
                },
                end,
            ),
            None => (0, 0), // pred does not occur in rule1_head --> nothing to map to
        };

        for j in ran_start..ran_end {
            let atom_j = rule1_head.sorted_atoms[j];

            // prefer mapping variables of rule1 to variables of rule2
            if let Some(eta) = unify(atom_j.terms(), atom_i.primitives(), eta.clone()) {
                mu.0.insert(i, j);
                match check(rule1, rule2, mu, &eta) {
                    CheckResult::Accept => {
                        return Some(Reliance {
                            mu: mu.clone(),
                            idx_dom: i,
                            idx_ran: j,
                        });
                    }
                    CheckResult::Extend => {
                        if let Some(Reliance { mu, .. }) = extend(
                            rule1,
                            rule2,
                            rule1_head,
                            rule2_part,
                            check,
                            mu,
                            eta,
                            i + 1,
                            0,
                        ) {
                            return Some(Reliance {
                                mu,
                                idx_dom: i,
                                idx_ran: j,
                            });
                        }
                    }
                    CheckResult::Reject => {}
                }
            }
        }
        mu.0.remove(&i);
    }

    None
}
