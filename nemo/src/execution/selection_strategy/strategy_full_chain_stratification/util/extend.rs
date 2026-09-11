use std::collections::HashMap;

use crate::rule_model::components::term::operation::operation_kind::OperationKind;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    combined_consts, shift_atom, Atom, Constant, Operation, Rule, Var,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::{
    atom::AtomsPart,
    ordered_atoms::SortedHeadAtoms,
    unify::unify,
};

/// maps indices of body/head atoms of the 2nd rule to indices of head atoms of the 1st rule
#[derive(Debug, Clone, Default)]
pub struct AtomMapping(HashMap<usize, usize>);

impl AtomMapping {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn mapped<'a, T>(&'a self, domain: &'a [T]) -> impl Iterator<Item = &'a T> + 'a {
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

type CheckFn = fn(&Rule, &Rule, &AtomMapping, &Substitution) -> CheckResult;

#[derive(Debug, Default)]
pub struct Reliance {
    mu: AtomMapping,
    idx_dom: usize,
    idx_ran: usize,
}

// Check whether subset of rule2_part unifies with rule1_head, s.t. the CheckFn accepts.
pub fn extend_init<'b, 'a: 'b, T: AtomsPart>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    check: CheckFn,
    previous_opt: Option<&Reliance>,
    mut eta: Substitution,
) -> Option<Reliance> {
    let (idx_dom, idx_ran) = match previous_opt {
        Some(Reliance {
            idx_dom, idx_ran, ..
        }) => (*idx_dom, *idx_ran + 1), // look for "next" one
        None => (0, 0),
    };

    let (rule1, rule2_native) = mem.rules.get_two(rule1_index, rule2_index);
    // Give rule2 a disjoint working copy of its variables (rule1 keeps ids `0..rule1.var_count()`,
    // rule2's copy gets `rule1.var_count()..`), so the two rules' variables can never collide --
    // this also correctly handles the self-restraint case where rule1_index == rule2_index.
    let offset = rule1.var_count();

    // Rule1 is never primed, so its cached sorted head atoms are used as-is; rule2's cached
    // reordered atoms (computed against its native, unprimed ids, and shared across every rule1
    // it's compared against) just get their ids shifted here -- a cheap pass over the small,
    // already-ordered list, avoiding rerunning the heuristic itself for every pair.
    let rule1_head: &SortedHeadAtoms = mem.sorted_head_atoms.get(rule1, rule1_index);
    let rule2_part_native = T::reordered_mem(&mut mem.reordered_atoms).get(rule2_native, rule2_index);
    let rule2_part: Vec<Atom> = T::atoms(rule2_part_native)
        .iter()
        .map(|a| shift_atom(a, offset))
        .collect();

    let rule2 = rule2_native.prime(offset);
    let rule2 = &rule2;

    // initialize the substitution with known constant replacements (possibly from normalization)
    for op in rule1.operations().iter().chain(rule2.operations().iter()) {
        if let Operation::Operation {
            kind: OperationKind::Equal,
            subterms,
        } = op
        {
            if let [Operation::Primitive(var), Operation::Primitive(val)] = subterms.as_ref() {
                eta.insert(*var, *val);
            }
        }
    }

    let consts = combined_consts(rule1, rule2);

    extend(
        rule1,
        rule2,
        rule1_head,
        &rule2_part,
        &consts,
        check,
        &mut AtomMapping::new(),
        eta,
        idx_dom,
        idx_ran,
    )
}

#[allow(clippy::too_many_arguments)]
fn extend(
    rule1: &Rule,
    rule2: &Rule,
    rule1_head: &SortedHeadAtoms,
    rule2_part: &[Atom],
    consts: &HashMap<Var, Constant>,
    check: CheckFn,
    mu: &mut AtomMapping,
    eta: Substitution,
    idx_dom: usize,
    idx_ran: usize,
) -> Option<Reliance> {
    for i in idx_dom..rule2_part.len() {
        let atom_i = &rule2_part[i];

        debug_assert!(
            !mu.0.contains_key(&i),
            "extend tried to change previous mapping"
        );

        let (ran_start, ran_end) = match rule1_head.ranges.get(&atom_i.predicate()).copied() {
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
            let atom_j = &rule1_head.sorted_atoms[j];

            // prefer mapping variables of rule2 onto variables of rule1
            if let Some(eta) = unify(
                atom_j.terms().iter().copied(),
                atom_i.terms().iter().copied(),
                consts,
                eta.clone(),
            ) {
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
                            consts,
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
