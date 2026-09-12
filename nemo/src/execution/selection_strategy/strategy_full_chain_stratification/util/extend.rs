use std::collections::HashMap;

use crate::rule_model::components::term::operation::operation_kind::OperationKind;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atoms, Constant, EdgeId, Operation, Rule, Var, combined_consts,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::{
    atom::AtomsPart, unify::unify,
};

/// Maps rule2's atoms (by [EdgeId], identifying an atom independent of any particular search
/// order) to the rule1 head atom each unified with.
#[derive(Debug, Clone, Default)]
pub struct AtomMapping(HashMap<EdgeId, EdgeId>);

impl AtomMapping {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    fn insert(&mut self, rule2_edge: EdgeId, rule1_edge: EdgeId) {
        self.0.insert(rule2_edge, rule1_edge);
    }

    fn remove(&mut self, rule2_edge: EdgeId) {
        self.0.remove(&rule2_edge);
    }

    pub fn contains(&self, rule2_edge: EdgeId) -> bool {
        self.0.contains_key(&rule2_edge)
    }

    /// The set of rule2 atoms currently mapped.
    pub fn domain(&self) -> impl Iterator<Item = EdgeId> + '_ {
        self.0.keys().copied()
    }
}

#[derive(Debug)]
pub enum CheckResult {
    Accept,
    Extend,
    Reject,
}

/// `rule1, rule2, rule2_part, i, mu, eta`: `rule2_part` is rule2's heuristically-ordered edge list
/// for whichever part is being searched over (positive/negative/head body, see `AtomsPart`), and
/// `i` is the current search frontier: `rule2_part[..i]` have already been visited by the search
/// (whether or not they ended up in `mu`), `rule2_part[i..]` have not.
type CheckFn = fn(&Rule, &Rule, &[EdgeId], usize, &AtomMapping, &Substitution) -> CheckResult;

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

    // Cached against rule2's native (unprimed) ids and shared across every rule1 it's compared
    // against -- priming only changes the *values* an edge's row holds, never which edges exist
    // or their order, so this same edge list stays valid against the primed `rule2` below.
    let rule2_part = T::edges(T::reordered_mem(&mut mem.reordered_atoms).get(rule2_native, rule2_index)).to_vec();

    let rule2 = rule2_native.prime(offset);
    let rule2 = &rule2;
    let atoms2 = T::atoms(rule2);

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
        atoms2,
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
    atoms2: &Atoms,
    rule2_part: &[EdgeId],
    consts: &HashMap<Var, Constant>,
    check: CheckFn,
    mu: &mut AtomMapping,
    eta: Substitution,
    idx_dom: usize,
    idx_ran: usize,
) -> Option<Reliance> {
    let rule1_head = rule1.head();

    for i in idx_dom..rule2_part.len() {
        let edge_i = rule2_part[i];
        let pred_i = atoms2.predicate_of(edge_i);
        let terms_i = atoms2.row(edge_i);

        debug_assert!(!mu.contains(edge_i), "extend tried to change previous mapping");

        let tuples1 = rule1_head.tuples_for(pred_i);
        let tuple_count = tuples1.map_or(0, |t| t.len());
        let j_start = if i == idx_dom { idx_ran } else { 0 };

        for j in j_start..tuple_count {
            let atom1_row = &tuples1.expect("tuple_count > 0 implies Some")[j];

            // prefer mapping variables of rule2 onto variables of rule1
            if let Some(eta) = unify(
                atom1_row.iter().copied(),
                terms_i.iter().copied(),
                consts,
                eta.clone(),
            ) {
                let color1 = rule1_head
                    .color_of(pred_i)
                    .expect("tuple_count > 0 implies this predicate has a color");
                let edge_j = EdgeId {
                    color: color1,
                    idx: j,
                };
                mu.insert(edge_i, edge_j);
                match check(rule1, rule2, rule2_part, i, mu, &eta) {
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
                            atoms2,
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
        mu.remove(edge_i);
    }

    None
}
