//! Defines the execution strategy for chain-stratified rulesets by which rule applications respect a precedence.

// Note: possibly performance issue: the Tag contains the actual string... should be interned

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash};

use crate::execution::planning::analysis::variable_order::{
    VariableOrder, build_preferable_variable_orders_for_rule,
};
use crate::execution::planning::normalization::{
    atom::{body::BodyAtom, head::HeadAtom},
    rule::NormalizedRule,
};
use crate::rule_model::{
    components::{
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    substitution::Substitution,
};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

use delegate::delegate;
use strum::EnumCount;
use strum_macros::EnumCount;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, EnumCount)]
#[repr(usize)]
enum EdgeLabel {
    Positive,
    Restraint,
    Negation,
    Aggregation,
}

impl crate::util::labeled_graph::SpecialEdgeLabel for EdgeLabel {
    fn is_special(&self) -> bool {
        *self != EdgeLabel::Positive
    }
}

type Graph = crate::util::labeled_graph::LabeledGraph<usize, EdgeLabel, petgraph::Directed>;

/// Defines a strategy where rule are divided into different strata which are executed in succession.
/// Entering a new statum implies that the table for every negated atom will not get any new elements.
#[derive(Debug)]
pub struct StrategyFullChainStratification<SubStrategy: RuleSelectionStrategy> {
    ordered_strata: Vec<Vec<usize>>, // the 0th stratum is "special" and contains all the Datalog rules --> Datalog-first chase
    substrategies: Vec<SubStrategy>,

    current_stratum: usize,
}

/// this is needed because BodyAtoms will have iterator of owned Primitives (converted from Variables) and HeadAtoms will have
impl AsRef<Primitive> for Primitive {
    fn as_ref(&self) -> &Primitive {
        self
    }
}

/// Extend MGU eta to also unify the given iterators of terms
/// terms_a and terms_b should correspond to atoms wih the same predicate symbol, i.e. the same arity
/// thus, unify(.) assumes that both iterators have the same number of values
///
/// in cas body atoms are to be unified, their terms: Vec<Variable> have to be converted
/// maybe look for euqlities in operations to get back at the constants in the body?
///
/// Try to preserve variables of atom_a by mapping variables of atom_b onto them
/// We will produce an MGU with $dom(\eta) \cap ran(\eta) = \varnothing$, i.e. A->B means that B is not replaced
/// maybe resuse Nemo's substitution for VariableMapping?
fn unify<'a, T, U>(
    terms_a: impl Iterator<Item = T>,
    terms_b: impl Iterator<Item = U>,
    mut eta: Substitution,
) -> Option<Substitution>
where
    T: AsRef<Primitive>,
    U: AsRef<Primitive>,
{
    for (term_a, term_b) in terms_a.zip(terms_b) {
        let (term_a, term_b) = (term_a.as_ref(), term_b.as_ref());
        if term_a != term_b {
            let mapped_term_a_opt = eta.get(term_a);
            let mapped_term_b_opt = eta.get(term_b);
            match mapped_term_a_opt {
                Some(mapped_term_a) => match mapped_term_a {
                    Primitive::Ground(_) => match mapped_term_b_opt {
                        Some(mapped_term_b) => match mapped_term_b {
                            Primitive::Ground(_) => {
                                if mapped_term_a != mapped_term_b {
                                    log::trace!(
                                        "cannot assign constant {mapped_term_a} to constant {mapped_term_b}"
                                    );
                                    return None;
                                }
                            }
                            Primitive::Variable(_) => {
                                log::trace!(
                                    "remap {term_b} from {mapped_term_b} to constant {mapped_term_a}"
                                );
                                eta.remap(mapped_term_b, mapped_term_a);
                            }
                        },
                        None => {
                            log::trace!("map {term_b} to constant {mapped_term_a}");
                            eta.remap(term_b.clone(), mapped_term_a);
                        }
                    },
                    Primitive::Variable(_) => {
                        match mapped_term_b_opt {
                            Some(mapped_term_b) => {
                                match mapped_term_b {
                                    Primitive::Ground(_) => {
                                        log::trace!(
                                            "remap {term_a} from {mapped_term_a} to constant {mapped_term_b}"
                                        );
                                        eta.remap(mapped_term_a, mapped_term_b);
                                    }
                                    Primitive::Variable(_) => {
                                        if mapped_term_a != mapped_term_b {
                                            log::trace!(
                                                "remap {term_b} from {mapped_term_b} to variable {mapped_term_a}"
                                            );
                                            // have choice, so leave mapping for term_a in place and change it for term_b!
                                            eta.remap(mapped_term_b, mapped_term_a);
                                        }
                                    }
                                }
                            }
                            None => {
                                if *term_b != mapped_term_a {
                                    log::trace!("map {term_b} to variable {mapped_term_a}");
                                    eta.remap(term_b.clone(), mapped_term_a);
                                }
                            }
                        }
                    }
                },
                None => {
                    match mapped_term_b_opt {
                        Some(mapped_term_b) => {
                            if *term_a != mapped_term_b {
                                log::trace!(
                                    "map {term_a} to {} {mapped_term_b}",
                                    if matches!(mapped_term_b, Primitive::Ground(_)) {
                                        "constant"
                                    } else {
                                        "variable"
                                    }
                                );
                                eta.remap(term_a.clone(), mapped_term_b);
                            }
                        }
                        None => {
                            if term_b != term_a {
                                log::trace!("map {term_b} to variable {term_a}");
                                // have choice, so map term_b to term_a!
                                eta.remap(term_b.clone(), term_a.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    Some(eta)
}

pub enum CheckResult {
    Accept,
    Extend,
    Reject,
}

type CheckFn = fn(&NormalizedRule, &NormalizedRule, &AtomMapping, &Substitution) -> CheckResult;

/// maps indices of body/head atoms of the 2nd rule to indices of head atoms of the 1st rule
type AtomMapping = HashMap<usize, usize>;

struct Reliance {
    mu: AtomMapping,
    idx_dom: usize,
    idx_ran: usize,
}

type Predicate = String;

trait Atom {
    type Item<'a>: AsRef<Primitive>
    where
        Self: 'a;

    fn pred(&self) -> Predicate;
    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>>;
    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable>;

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, Self>>;
}

impl Atom for HeadAtom {
    type Item<'a> = &'a Primitive;

    fn pred(&self) -> Predicate {
        self.predicate().name().to_owned()
    }

    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>> {
        self.terms()
    }

    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable> {
        self.variables()
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, HeadAtom>> {
        &mut reordered_atoms.reordered_head_atoms
    }
}

impl Atom for BodyAtom {
    type Item<'a> = Primitive;

    fn pred(&self) -> Predicate {
        self.predicate().name().to_owned()
    }

    /// Return an iterator over all terms contained in this atom as primitives.
    fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>> {
        self.terms().map(|var| Primitive::Variable(var.clone()))
    }

    fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable> {
        self.terms()
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, BodyAtom>> {
        &mut reordered_atoms.reordered_body_atoms
    }
}

#[repr(transparent)]
struct NegBodyAtom(BodyAtom);

impl NegBodyAtom {
    /// Cast a reference to a [BodyAtom] to a reference to a [NegBodyAtom].
    /// This is safe, since the layout of the transparent newtype is compatible.
    /// This is analogous to deriving `bytemuck::TransparentWrapper` or `ref_cast::RefCast`.
    fn wrap_ref(r: &BodyAtom) -> &Self {
        //let tmp: *const BodyAtom = r;
        //unsafe { &*(tmp as *const NegBodyAtom) }
        unsafe { &*std::ptr::from_ref(r).cast::<NegBodyAtom>() }
    }
}

impl Atom for NegBodyAtom {
    type Item<'a>
        = <BodyAtom as Atom>::Item<'a>
    where
        Self: 'a;
    delegate! {
        to self.0 {
            fn pred(&self) -> Predicate;
            fn primitives<'a>(&'a self) -> impl Iterator<Item = Self::Item<'a>>;
            fn variables<'a>(&'a self) -> impl Iterator<Item = &'a Variable>;
        }
    }

    fn reorder<'b, 'a: 'b>(
        reordered_atoms: &'b mut ReorderAtoms<'a>,
    ) -> &'b mut Mem<ReorderedAtoms<'a, NegBodyAtom>> {
        &mut reordered_atoms.reordered_negative_body_atoms
    }
}

// Check whether subset of rule2_part unifies with rule1_head, s.t. the CheckFn accepts.
fn extend_init<'b, 'a: 'b, T: Atom + 'static>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    check: CheckFn,
    previous_opt: Option<&Reliance>,
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
    extend::<T>(
        &mem.rules[rule1_index],
        &mem.rules[rule2_index],
        mem.sorted_head_atoms.get(mem.rules, rule1_index),
        T::reorder(&mut mem.reordered_atoms).get(mem.rules, rule2_index),
        check,
        &mut AtomMapping::new(),
        Substitution::default(),
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
            !mu.contains_key(&i),
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
                mu.insert(i, j);
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
        mu.remove(&i);
    }

    None
}

fn check_posr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

fn is_positive_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<BodyAtom>(mem, rule1_index, rule2_index, check_posr, previous_opt)
}

fn check_self_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

fn is_self_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(mem, rule_index, rule_index, check_self_restr, previous_opt)
}

fn check_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

fn is_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(mem, rule1_index, rule2_index, check_restr, previous_opt)
}

fn check_negr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

fn is_negation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<NegBodyAtom>(mem, rule1_index, rule2_index, check_negr, previous_opt)
}

fn check_aggr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

fn is_aggregation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<BodyAtom>(mem, rule1_index, rule2_index, check_aggr, previous_opt)
}

type ReorderedAtoms<'a, T> = Vec<&'a T>;
type ReorderedBodyAtoms<'a> = ReorderedAtoms<'a, BodyAtom>;
type ReorderedNegBodyAtoms<'a> = ReorderedAtoms<'a, NegBodyAtom>;
type ReorderedHeadAtoms<'a> = ReorderedAtoms<'a, HeadAtom>;

struct Mem<T>(Vec<Option<T>>);

impl<T: Clone> Mem<T> {
    fn new(len: usize) -> Self {
        Self(vec![None; len])
    }
}

trait GetRuleMem<'a> {
    fn compute(rule: &'a NormalizedRule) -> Self;
}

impl<'a, T: GetRuleMem<'a>> Mem<T> {
    fn get(&mut self, rules: &'a Vec<&'a NormalizedRule>, rule_index: usize) -> &T {
        self.0[rule_index].get_or_insert_with(|| T::compute(&rules[rule_index]))
    }
}

fn reorder_atoms<'a, T: Atom>(
    atoms: impl IntoIterator<Item = &'a T>,
    order: &VariableOrder,
) -> ReorderedAtoms<'a, T> {
    let mut atoms: Vec<_> = atoms.into_iter().collect();
    let mut reordered = Vec::with_capacity(atoms.len());
    let mut vars_so_far = HashSet::new();
    for v in order.iter() {
        vars_so_far.insert(v);
        // find all atoms which only use variables from vars_so_far
        let (only_active_vars, remainder): (Vec<_>, Vec<_>) = atoms
            .into_iter()
            .partition(|atom| atom.variables().all(|var| vars_so_far.contains(var)));
        reordered.extend_from_slice(&only_active_vars);
        atoms = remainder;
    }
    reordered
}

impl<'a> GetRuleMem<'a> for ReorderedBodyAtoms<'a> {
    /// Reorder body atoms.
    /// to be applied to body / head of rule2 before calling extend
    /// maybe reuse Nemo's heuristic for join order?
    /// use NormalizeRule::variable_order --> should be Some
    fn compute(rule: &'a NormalizedRule) -> ReorderedBodyAtoms<'a> {
        reorder_atoms(rule.positive(), rule.body_variable_order())
    }
}

impl<'a> GetRuleMem<'a> for ReorderedNegBodyAtoms<'a> {
    /// Reorder negative body atoms.
    /// Such a variable order has not been computed anwhere else, as negative atoms don't partake in joins, so we just apply the heuristic manually.
    fn compute(rule: &'a NormalizedRule) -> ReorderedNegBodyAtoms<'a> {
        fn construct_auxiliary_negation_rule(rule: &NormalizedRule) -> NormalizedRule {
            let mut universal_variables = rule
                .negative()
                .iter()
                .flat_map(|atom| atom.terms())
                .collect::<Vec<_>>();
            universal_variables.dedup();

            let head = HeadAtom::new(
                Tag::new(String::from("__AUX")),
                universal_variables
                    .into_iter()
                    .cloned()
                    .map(Primitive::from),
            );

            NormalizedRule::positive_rule(vec![head], rule.negative().clone(), vec![])
        }

        let auxiliary_rule = construct_auxiliary_negation_rule(rule);
        let auxiliary_order = build_preferable_variable_orders_for_rule(&auxiliary_rule, None)
            .restrict_to(&rule.variables().cloned().collect::<HashSet<_>>());

        reorder_atoms(
            rule.negative()
                .iter()
                .map(|atom| NegBodyAtom::wrap_ref(atom)),
            &auxiliary_order,
        )
    }
}

impl<'a> GetRuleMem<'a> for ReorderedHeadAtoms<'a> {
    /// Reorder head atoms.
    fn compute(rule: &'a NormalizedRule) -> ReorderedHeadAtoms<'a> {
        reorder_atoms(rule.head(), rule.head_variable_order())
    }
}

struct ReorderAtoms<'a> {
    reordered_body_atoms: Mem<ReorderedBodyAtoms<'a>>,
    reordered_negative_body_atoms: Mem<ReorderedNegBodyAtoms<'a>>,
    reordered_head_atoms: Mem<ReorderedHeadAtoms<'a>>,
}

#[derive(Clone)]
struct SortedHeadAtoms<'a> {
    sorted_atoms: ReorderedHeadAtoms<'a>,
    ranges: HashMap<Predicate, (usize, usize)>,
}

impl<'a> GetRuleMem<'a> for SortedHeadAtoms<'a> {
    /// to be applied to head of rule1 before calling extend
    fn compute(rule: &'a NormalizedRule) -> SortedHeadAtoms<'a> {
        let mut sorted_atoms: Vec<_> = rule.head().iter().collect();
        sorted_atoms.sort_unstable_by_key(|atom| atom.predicate());
        let mut ranges = HashMap::new();
        let mut last = 0;
        for i in 1..sorted_atoms.len() {
            let p = sorted_atoms[i - 1].predicate();
            if &p != &sorted_atoms[i].predicate() {
                ranges.insert(p.name().to_string(), (last, i));
                last = i;
            }
        }
        SortedHeadAtoms {
            sorted_atoms,
            ranges,
        }
    }
}

#[derive(Clone)]
struct RuleEncoding {
    pred_stream: Vec<Predicate>,
    arg_stream: Vec<usize>,
}

/// will be indexed by EdgeLabel variants via their usize repr
/// meaning of values:
///   None --> not computed yet
///   Some(vec![]) --> no reliance of this type
///   Some(vec![mu]) --> found reliance of this type with AtomMapping mu
///   Some(vec![mu1,mu2]) --> found reliance of this type with multiple possible AtomMappings (relevant for chain computations)
type Reliances = [Option<Vec<Reliance>>; EdgeLabel::COUNT];

struct RuleMemoization<'a> {
    rules: &'a Vec<&'a NormalizedRule>,

    // these fields memoize auxiliary per-rule data
    // uses `Vec<Option<X>>` (initialized to `vec![None; rules.len()]`) instead of `HashMap<usize,X>`
    reordered_atoms: ReorderAtoms<'a>,
    sorted_head_atoms: Mem<SortedHeadAtoms<'a>>,
    encoded_rules: Vec<Option<RuleEncoding>>,
}

struct RelianceMemoization<'a> {
    data: RuleMemoization<'a>,

    // the following 3 fields are instead of 2 fields of types `HashMap<(usize, usize), Vec<usize>>` and `HashMap<Vec<usize>, Reliances>`, which spares the need to store the keys twice
    reliances: Vec<Reliances>,
    reliances_key_map: HashMap<Vec<usize>, usize>, // maps canonized rule-pair keys into `reliances` Vec
    reliances_edge_map: HashMap<(usize, usize), usize>, // maps pairs of rule indices into `reliances` Vec
}

impl<'a> RelianceMemoization<'a> {
    fn new(rules: &'a Vec<&'a NormalizedRule>) -> Self {
        Self {
            data: RuleMemoization {
                rules,

                reordered_atoms: ReorderAtoms {
                    reordered_body_atoms: Mem::new(rules.len()),
                    reordered_negative_body_atoms: Mem::new(rules.len()),
                    reordered_head_atoms: Mem::new(rules.len()),
                },
                sorted_head_atoms: Mem(vec![None; rules.len()]),
                encoded_rules: vec![None; rules.len()],
            },

            reliances: Vec::new(),
            reliances_key_map: HashMap::new(),
            reliances_edge_map: HashMap::new(),
        }
    }

    fn encode_rule(rule: &NormalizedRule) -> RuleEncoding {
        use crate::execution::planning::normalization::{
            aggregate::Aggregation,
            atom::{body::BodyAtom, head::HeadAtom, import::ImportAtom},
            operation::Operation,
        };
        use crate::rule_model::components::term::primitive::{Primitive, variable::Variable};

        struct EncodeState {
            pred_stream: Vec<Predicate>,
            arg_stream: Vec<usize>,
            v: usize,
            var_map: HashMap<String, usize>,
        }

        impl EncodeState {
            fn encode_iter<'a, T: 'a>(
                &mut self,
                len: usize,
                it: impl Iterator<Item = &'a T>,
                f: fn(&mut EncodeState, &T),
            ) {
                self.arg_stream.push(len);
                for elem in it {
                    f(self, elem);
                }
            }
            fn encode_vec<T>(&mut self, vec: &Vec<T>, f: fn(&mut EncodeState, &T)) {
                self.encode_iter(vec.len(), vec.iter(), f);
            }
            fn encode_variable(&mut self, var: &Variable) {
                let name = match var {
                    Variable::Universal(u_var) => u_var.name(),
                    Variable::Existential(e_var) => Some(e_var.name()),
                    Variable::Global(g_var) => Some(g_var.name()),
                };
                match name {
                    Some(name) => {
                        self.arg_stream
                            .push(*self.var_map.entry(name.to_string()).or_insert_with(|| {
                                let v = self.v;
                                self.v += 1;
                                v
                            }));
                    }
                    None => {
                        // anonymous variable
                        self.arg_stream.push(self.v);
                        self.v += 1;
                    }
                }
            }
            fn encode_primitive(&mut self, prim: &Primitive) {
                match prim {
                    Primitive::Variable(var) => self.encode_variable(var),
                    Primitive::Ground(g) => {
                        use std::hash::Hasher;
                        let mut hasher = DefaultHasher::new();
                        g.value().hash(&mut hasher);
                        let hash = hasher.finish();
                        self.arg_stream.push(hash as usize); // may truncate high bits of hash
                    }
                }
            }
            fn encode_body_atom(&mut self, body_atom: &BodyAtom) {
                self.pred_stream
                    .push(body_atom.predicate().name().to_owned());
                self.encode_iter(body_atom.arity(), body_atom.terms(), Self::encode_variable);
            }
            fn encode_import_atom(&mut self, import_atom: &ImportAtom) {
                self.pred_stream
                    .push(import_atom.predicate().name().to_owned());
                self.encode_iter(
                    import_atom.arity(),
                    import_atom.variables(),
                    Self::encode_variable,
                );
            }
            fn encode_operation(&mut self, operation: &Operation) {
                match operation {
                    Operation::Primitive(prim) => self.encode_primitive(prim),
                    Operation::Operation { kind, subterms } => {
                        use crate::rule_model::components::term::operation::operation_kind::OperationKind;
                        use strum::IntoEnumIterator;

                        let kind_index = OperationKind::iter().position(|k| k == *kind).unwrap();
                        self.arg_stream.push(kind_index);
                        self.encode_vec(subterms, Self::encode_operation);
                    }
                }
            }
            fn encode_head_atom(&mut self, head_atom: &HeadAtom) {
                self.pred_stream
                    .push(head_atom.predicate().name().to_owned());
                self.encode_iter(head_atom.arity(), head_atom.terms(), Self::encode_primitive);
            }
            fn encode_aggregation(&mut self, aggregation: &Aggregation, aggregate_index: usize) {
                use crate::rule_model::components::term::aggregate::AggregateKind;
                use strum::IntoEnumIterator;

                let kind = aggregation.aggregate_kind();
                let kind_index = AggregateKind::iter().position(|k| k == kind).unwrap();
                self.arg_stream.push(kind_index);
                self.encode_variable(aggregation.input_variable());
                self.encode_variable(aggregation.output_variable());
                self.encode_vec(aggregation.distinct_variables(), Self::encode_variable);
                self.encode_vec(aggregation.group_by_variables(), Self::encode_variable);

                self.arg_stream.push(aggregate_index);
            }
        }

        let mut state = EncodeState {
            pred_stream: vec![],
            arg_stream: vec![],
            v: 0,
            var_map: HashMap::new(),
        };

        state.encode_vec(rule.positive(), EncodeState::encode_body_atom);
        state.encode_vec(rule.negative(), EncodeState::encode_body_atom);
        state.encode_vec(rule.positive_imports(), EncodeState::encode_import_atom);
        state.encode_vec(rule.negative_imports(), EncodeState::encode_import_atom);
        state.encode_vec(rule.operations(), EncodeState::encode_operation);
        state.encode_vec(rule.head(), EncodeState::encode_head_atom);
        if let Some(aggregate) = rule.aggregate() {
            state.encode_aggregation(
                aggregate,
                rule.aggregate_index()
                    .expect("rule with aggregate should have aggregation_index"),
            );
        }

        RuleEncoding {
            pred_stream: state.pred_stream,
            arg_stream: state.arg_stream,
        }
    }

    fn encode_rule_pair(enc1: &RuleEncoding, enc2: &RuleEncoding) -> Vec<usize> {
        let mut key = Vec::with_capacity(
            enc1.pred_stream.len()
                + enc1.arg_stream.len()
                + enc2.pred_stream.len()
                + enc2.arg_stream.len(),
        );
        let mut pred_map = HashMap::new();
        let mut p = 0;
        for pred in enc1.pred_stream.iter().chain(enc2.pred_stream.iter()) {
            key.push(*pred_map.entry(pred).or_insert_with(|| {
                let prev_p = p;
                p += 1;
                prev_p
            }));
        }
        key
    }

    // only get one reliance
    // todo: implement something like get_all()
    fn get<'b>(
        &'b mut self,
        rule1_index: usize,
        rule2_index: usize,
        label: EdgeLabel,
    ) -> Option<&'b Reliance>
    where
        'a: 'b,
    {
        let rule1 = *&self.data.rules[rule1_index];
        let rule2 = *&self.data.rules[rule2_index];
        let reliances_index = *self
            .reliances_edge_map
            .entry((rule1_index, rule2_index))
            .or_insert_with(|| {
                let _ = self.data.encoded_rules[rule1_index]
                    .get_or_insert_with(|| Self::encode_rule(rule1));
                let _ = self.data.encoded_rules[rule2_index]
                    .get_or_insert_with(|| Self::encode_rule(rule2));
                let enc1 = self.data.encoded_rules[rule1_index].as_ref().unwrap();
                let enc2 = self.data.encoded_rules[rule2_index].as_ref().unwrap();
                let enc = Self::encode_rule_pair(enc1, enc2);
                *self.reliances_key_map.entry(enc).or_insert_with(|| {
                    let new_index = self.reliances.len();
                    self.reliances.push([const { None }; EdgeLabel::COUNT]);
                    new_index
                })
            });
        let atom_mappings_vec =
            self.reliances[reliances_index][label as usize].get_or_insert_with(|| {
                if let Some(mu) = match label {
                    EdgeLabel::Positive => {
                        is_positive_reliance(&mut self.data, rule1_index, rule2_index, None)
                    }
                    EdgeLabel::Restraint => {
                        if rule1_index == rule2_index {
                            match is_self_restraint_reliance(&mut self.data, rule1_index, None) {
                                None => is_restraint_reliance(
                                    &mut self.data,
                                    rule1_index,
                                    rule2_index,
                                    None,
                                ),
                                x => x,
                            }
                        } else {
                            is_restraint_reliance(&mut self.data, rule1_index, rule2_index, None)
                        }
                    }
                    EdgeLabel::Negation => {
                        is_negation_reliance(&mut self.data, rule1_index, rule2_index, None)
                    }
                    EdgeLabel::Aggregation => {
                        is_aggregation_reliance(&mut self.data, rule1_index, rule2_index, None)
                    }
                } {
                    vec![mu]
                } else {
                    vec![]
                }
            });
        atom_mappings_vec.last()
    }
}

impl<SubStrategy: RuleSelectionStrategy> StrategyFullChainStratification<SubStrategy> {
    /// Compute predicate-based dependencies in the ruleset
    fn build_dependency_graph(rules: &Vec<&NormalizedRule>) -> Graph {
        let mut predicate_to_rules_body_positive = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_aggregation = HashMap::<Tag, Vec<usize>>::new();

        let rule_count = rules.len();

        for (rule_index, rule) in rules.iter().enumerate() {
            for (body_predicate, _) in rule.predicates_positive() {
                let indices = predicate_to_rules_body_positive
                    .entry(body_predicate)
                    .or_default();

                indices.push(rule_index);
            }

            if let Some(head_index) = rule.aggregate_index() {
                let indices = predicate_to_rules_aggregation
                    .entry(rule.head()[head_index].predicate())
                    .or_default();

                indices.push(rule_index);
            }

            for (body_predicate, _) in rule.predicates_negative() {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate)
                    .or_default();

                indices.push(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head() {
                let indices = predicate_to_rules_head.entry(head_predicate).or_default();

                indices.push(rule_index);
            }
        }

        let mut dependency_graph = Graph::default();

        for rule_index in 0..rule_count {
            dependency_graph.add_node(rule_index);
        }

        for (head_predicate, head_rules) in predicate_to_rules_head {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Positive);
                    }
                }
            }

            for &existential_head_index in head_rules.iter().filter(|&&i| rules[i].is_existential())
            {
                for &head_index in &head_rules {
                    dependency_graph.add_edge(
                        head_index,
                        existential_head_index,
                        EdgeLabel::Restraint,
                    );
                }
            }

            if let Some(body_rules) = predicate_to_rules_body_negative.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Negation);
                    }
                }
            }
        }

        for (head_predicate, head_rules) in predicate_to_rules_aggregation {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Aggregation);
                    }
                }
            }
        }

        dependency_graph
    }

    /// Compute reliances within the given SCC of the dependency graph
    /// fails if the special edges are cyclic
    fn build_reliance_graph<'b, 'a: 'b>(
        mem: &'b mut RelianceMemoization<'a>,
        dependency_graph: &Graph,
        stratum: &Vec<usize>,
    ) -> Result<Graph, SelectionStrategyError> {
        let mut reliance_graph = Graph::default();

        for &rule_index in stratum {
            reliance_graph.add_node(rule_index);
        }

        let stratum_set = stratum.iter().copied().collect::<HashSet<_>>();

        for &rule1_index in stratum {
            for (&label, &rule2_index) in dependency_graph
                .edges_outgoing(&rule1_index)
                .filter(|(_, rule2_index)| stratum_set.contains(&rule2_index))
            {
                if let Some(_) = mem.get(rule1_index, rule2_index, label) {
                    if reliance_graph.has_path_via_special(
                        reliance_graph.node_unchecked(&rule1_index),
                        reliance_graph.node_unchecked(&rule1_index),
                    ) {
                        return Err(SelectionStrategyError::NonStratifiedProgram);
                    }
                    reliance_graph.add_edge(rule1_index, rule1_index, label);
                }
            }
        }

        Ok(reliance_graph)
    }

    /// Compute chains within the given SCC of the reliance graph
    /// chain graph will not use EdgeLabel::Positive
    fn build_chain_graph(
        mem: &mut RelianceMemoization,
        reliance_graph: &Graph,
        stratum: &Vec<usize>,
    ) -> Result<Graph, SelectionStrategyError> {
        todo!()
    }
}

impl<SubStrategy: RuleSelectionStrategy> RuleSelectionStrategy
    for StrategyFullChainStratification<SubStrategy>
{
    /// Create new [StrategyFullChainStratification].
    fn new(rules: Vec<&NormalizedRule>) -> Result<Self, SelectionStrategyError> {
        // prepare empty vec for strata and add placeholder for 0th Datalog-stratum (will be filled if ruleset is determined to be chain-stratifiable below)
        // since TarjanSCC traverses SCCs in reverse topological order, this vec will be reversed before usage
        let mut strata = vec![vec![]];

        // prepare maps to memoize encoded / canonized rules and rule pairs
        let mut mem = RelianceMemoization::new(&rules);

        // reusable state for inner Tarjan's algorithm
        let mut tarjan = petgraph::algo::TarjanScc::new();

        // compute the dependency graph and prepare hash map of nodes to SCC identifiers
        let dependency_graph = Self::build_dependency_graph(&rules);

        dependency_graph.decompose_and_refine(
            &mut strata,
            &mut petgraph::algo::TarjanScc::new(),
            |depg_stratum, strata| {
                let reliance_graph =
                    Self::build_reliance_graph(&mut mem, &dependency_graph, &depg_stratum)?;

                reliance_graph.decompose_and_refine(strata, &mut tarjan, |relg_stratum, strata| {
                    let chain_graph =
                        Self::build_chain_graph(&mut mem, &reliance_graph, &relg_stratum)?;

                    let chaing_strata = chain_graph.layer().expect(
                        "chain graph should be acyclic if no SelectionStrategyError was raised",
                    );

                    for chaing_stratum in chaing_strata {
                        strata.push(chaing_stratum);
                    }
                    Ok(())
                })
            },
        )?;

        let mut substrategies = Vec::with_capacity(strata.len());

        // extract 0th separate Datalog stratum to be applied exhaustively after each non-Datlog rule application, since we need to ensure Datalog-first chase
        let datalog_rules;
        // fill the placeholder in the strata list
        (strata[0], datalog_rules) = rules
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, rule)| rule.is_datalog())
            .unzip();
        substrategies.push(SubStrategy::new(datalog_rules)?);

        // reverse the strata, as they were pushed in reverse topological order above
        strata[1..].reverse();

        for stratum in &mut strata[1..] {
            stratum.sort();

            let mut sub_rules = Vec::with_capacity(stratum.len());

            // remove all Datalog rules from higher strata (they are redundant there, as they already live in the "special" 0th stratum)
            stratum.retain(|&i| {
                let rule = rules[i];
                if rule.is_datalog() {
                    false
                } else {
                    sub_rules.push(rule);
                    true
                }
            });

            sub_rules.shrink_to_fit();

            substrategies.push(SubStrategy::new(sub_rules)?);
        }

        if strata.len() > 1 {
            log::info!("Stratified program: {strata:?}")
        }

        Ok(Self {
            ordered_strata: strata,
            substrategies,
            current_stratum: 0,
        })
    }

    fn next_rule(&mut self, mut new_derivations: Option<bool>) -> Option<usize> {
        while self.current_stratum < self.ordered_strata.len() {
            if self.current_stratum != 0 && new_derivations == Some(true) {
                // prefer Datalog rules: after each rule application from a higher strate, exhaustively apply rules from the special 0th stratum
                if let Some(substrategy_next_datalog_rule) = self.substrategies[0].next_rule(None) {
                    return Some(self.ordered_strata[0][substrategy_next_datalog_rule]);
                }
            }
            if let Some(substrategy_next_rule) =
                self.substrategies[self.current_stratum].next_rule(new_derivations)
            {
                return Some(self.ordered_strata[self.current_stratum][substrategy_next_rule]);
            } else {
                self.current_stratum += 1;
                new_derivations = None;
            }
        }

        None
    }
}
