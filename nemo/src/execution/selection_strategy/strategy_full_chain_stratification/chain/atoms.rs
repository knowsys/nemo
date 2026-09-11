use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::{
    execution::planning::{
        analysis::variable_order::build_preferable_variable_orders_for_rule,
        normalization::{self, atom::head::HeadAtom, rule::NormalizedRule},
    },
    rule_model::components::{
        tag::Tag,
        term::{operation::operation_kind::OperationKind, primitive::Primitive},
    },
};
use nemo_physical::datavalues::AnyDataValue;

use crate::rule_model::components::term::primitive::variable::Variable as OrigVariable;

/// A variable is just a dense, per-rule id in `0..rule.var_count()`.
/// A term slot holding a ground value uses a reserved id too (see [`Rule::consts`]) so that
/// atoms/operations can stay uniformly `usize`-based.
pub(crate) type Var = usize;

/// An interned predicate symbol.
#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub(crate) struct Predicate(pub(crate) usize);

/// An interned constant (ground value).
#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub(crate) struct Constant(pub(crate) usize);

/// Interns values of type `T` into dense `usize` keys, assigned in first-seen order.
#[derive(Debug, Default)]
pub(crate) struct ComponentMap<T>(HashMap<T, usize>);

impl<T: Eq + Hash + Clone> ComponentMap<T> {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn get(&mut self, t: &T) -> usize {
        let len = self.0.len();
        *self.0.entry(t.clone()).or_insert(len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Atom {
    pub(crate) predicate: Predicate,
    pub(crate) terms: Box<[Var]>,
}

impl Atom {
    pub(crate) fn predicate(&self) -> Predicate {
        self.predicate
    }

    pub(crate) fn terms(&self) -> &[Var] {
        &self.terms
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = Var> + '_ {
        self.terms.iter().copied()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Operation {
    /// A single term slot.
    Primitive(Var),
    /// An operation applied to sub-terms.
    Operation {
        kind: OperationKind,
        subterms: Box<[Operation]>,
    },
}

impl Operation {
    fn from_normalized_operation(
        var_ids: &mut HashMap<OrigVariable, Var>,
        next_id: &mut Var,
        consts: &mut HashMap<Var, Constant>,
        const_map: &mut ComponentMap<AnyDataValue>,
        operation: &normalization::operation::Operation,
    ) -> Self {
        match operation {
            normalization::operation::Operation::Primitive(primitive) => Operation::Primitive(
                convert_primitive(var_ids, next_id, consts, const_map, primitive),
            ),
            normalization::operation::Operation::Operation { kind, subterms } => {
                Operation::Operation {
                    kind: *kind,
                    subterms: subterms
                        .iter()
                        .map(|operation| {
                            Operation::from_normalized_operation(
                                var_ids, next_id, consts, const_map, operation,
                            )
                        })
                        .collect(),
                }
            }
        }
    }
}

fn convert_variable(
    var_ids: &mut HashMap<OrigVariable, Var>,
    next_id: &mut Var,
    variable: &OrigVariable,
) -> Var {
    *var_ids.entry(variable.clone()).or_insert_with(|| {
        let id = *next_id;
        *next_id += 1;
        id
    })
}

fn convert_primitive(
    var_ids: &mut HashMap<OrigVariable, Var>,
    next_id: &mut Var,
    consts: &mut HashMap<Var, Constant>,
    const_map: &mut ComponentMap<AnyDataValue>,
    primitive: &Primitive,
) -> Var {
    match primitive {
        Primitive::Variable(variable) => convert_variable(var_ids, next_id, variable),
        Primitive::Ground(ground_term) => {
            let id = *next_id;
            *next_id += 1;
            consts.insert(id, Constant(const_map.get(&ground_term.value())));
            id
        }
    }
}

/// A rule, in a simplified, self-contained representation independent of [NormalizedRule].
///
/// Variables are dense `usize` ids starting at `0` (needed so that rules can be fed to the
/// `chain` solver, which indexes its domains/bitsets by dense vertex id). To compare two rules
/// (or a rule with itself, for self-restraint), obtain a disjoint working copy via [`Rule::prime`]
/// rather than relying on global uniqueness.
#[derive(Debug, Clone)]
pub(crate) struct Rule {
    head: Box<[Atom]>,
    body_pos: Box<[Atom]>,
    body_neg: Box<[Atom]>,
    operations: Box<[Operation]>,
    /// Which variable ids are actually bound to a ground value.
    consts: HashMap<Var, Constant>,
    var_count: usize,
    body_variable_order: Box<[Var]>,
    head_variable_order: Box<[Var]>,
    /// Heuristic order in which to visit negative-body atoms (there is no join to guide this,
    /// so it is computed the same way as [Self::body_variable_order], against a synthetic
    /// auxiliary rule whose body is this rule's negative atoms).
    negative_variable_order: Box<[Var]>,
}

impl Rule {
    pub(crate) fn head(&self) -> &[Atom] {
        &self.head
    }

    pub(crate) fn positive(&self) -> &[Atom] {
        &self.body_pos
    }

    pub(crate) fn negative_atoms(&self) -> &[Atom] {
        &self.body_neg
    }

    pub(crate) fn negative(&self) -> impl Iterator<Item = &Atom> {
        self.body_neg.iter()
    }

    pub(crate) fn operations(&self) -> &[Operation] {
        &self.operations
    }

    pub(crate) fn consts(&self) -> &HashMap<Var, Constant> {
        &self.consts
    }

    pub(crate) fn var_count(&self) -> usize {
        self.var_count
    }

    pub(crate) fn body_variable_order(&self) -> &[Var] {
        &self.body_variable_order
    }

    pub(crate) fn head_variable_order(&self) -> &[Var] {
        &self.head_variable_order
    }

    pub(crate) fn negative_variable_order(&self) -> &[Var] {
        &self.negative_variable_order
    }

    /// Return the set of universal (body) variables in this rule.
    pub(crate) fn universals(&self) -> HashSet<Var> {
        self.body_pos
            .iter()
            .flat_map(|atom| atom.variables())
            .collect()
    }

    fn head_variables(&self) -> HashSet<Var> {
        self.head.iter().flat_map(|atom| atom.variables()).collect()
    }

    /// Return the set of existential (head-only) variables in this rule.
    pub(crate) fn existentials(&self) -> HashSet<Var> {
        let universals = self.universals();
        self.head_variables()
            .into_iter()
            .filter(|v| !universals.contains(v))
            .collect()
    }

    /// Return the set of all variables occurring in this rule.
    pub(crate) fn variables(&self) -> HashSet<Var> {
        let mut vars = self.head_variables();
        vars.extend(self.universals());
        vars.extend(self.body_neg.iter().flat_map(|atom| atom.variables()));
        vars
    }

    /// Return a copy of this rule with every variable id shifted by `offset`, so that it becomes
    /// disjoint from any rule using ids below `offset`.
    pub(crate) fn prime(&self, offset: usize) -> Self {
        fn shift_atom(atom: &Atom, offset: usize) -> Atom {
            Atom {
                predicate: atom.predicate,
                terms: atom.terms.iter().map(|v| v + offset).collect(),
            }
        }
        fn shift_operation(operation: &Operation, offset: usize) -> Operation {
            match operation {
                Operation::Primitive(v) => Operation::Primitive(v + offset),
                Operation::Operation { kind, subterms } => Operation::Operation {
                    kind: *kind,
                    subterms: subterms
                        .iter()
                        .map(|op| shift_operation(op, offset))
                        .collect(),
                },
            }
        }

        Self {
            head: self.head.iter().map(|a| shift_atom(a, offset)).collect(),
            body_pos: self.body_pos.iter().map(|a| shift_atom(a, offset)).collect(),
            body_neg: self.body_neg.iter().map(|a| shift_atom(a, offset)).collect(),
            operations: self
                .operations
                .iter()
                .map(|op| shift_operation(op, offset))
                .collect(),
            consts: self.consts.iter().map(|(&v, &c)| (v + offset, c)).collect(),
            var_count: self.var_count,
            body_variable_order: self.body_variable_order.iter().map(|v| v + offset).collect(),
            head_variable_order: self.head_variable_order.iter().map(|v| v + offset).collect(),
            negative_variable_order: self
                .negative_variable_order
                .iter()
                .map(|v| v + offset)
                .collect(),
        }
    }

    /// Convert a [NormalizedRule] into this representation.
    ///
    /// # Panics
    /// Panics if `normalized_rule` does not have a body/head variable order computed
    /// (see [NormalizedRule::body_variable_order]/[NormalizedRule::head_variable_order]).
    /// This holds for any rule obtained via [normalization::program::NormalizedProgram::normalize_program].
    pub(crate) fn from_normalized_rule(
        pred_map: &mut ComponentMap<Tag>,
        const_map: &mut ComponentMap<AnyDataValue>,
        normalized_rule: &NormalizedRule,
    ) -> Self {
        let mut var_ids: HashMap<OrigVariable, Var> = HashMap::new();
        let mut next_id: Var = 0;
        let mut consts: HashMap<Var, Constant> = HashMap::new();

        let head: Box<[Atom]> = normalized_rule
            .head()
            .iter()
            .map(|head_atom| Atom {
                predicate: Predicate(pred_map.get(&head_atom.predicate())),
                terms: head_atom
                    .terms()
                    .map(|primitive| {
                        convert_primitive(&mut var_ids, &mut next_id, &mut consts, const_map, primitive)
                    })
                    .collect(),
            })
            .collect();

        let body_pos: Box<[Atom]> = normalized_rule
            .positive()
            .iter()
            .map(|body_atom| Atom {
                predicate: Predicate(pred_map.get(&body_atom.predicate())),
                terms: body_atom
                    .terms()
                    .map(|variable| convert_variable(&mut var_ids, &mut next_id, variable))
                    .collect(),
            })
            .collect();

        let body_neg: Box<[Atom]> = normalized_rule
            .negative()
            .map(|body_atom| Atom {
                predicate: Predicate(pred_map.get(&body_atom.predicate())),
                terms: body_atom
                    .terms()
                    .map(|variable| convert_variable(&mut var_ids, &mut next_id, variable))
                    .collect(),
            })
            .collect();

        let operations: Box<[Operation]> = normalized_rule
            .operations()
            .iter()
            .map(|operation| {
                Operation::from_normalized_operation(
                    &mut var_ids,
                    &mut next_id,
                    &mut consts,
                    const_map,
                    operation,
                )
            })
            .collect();

        let body_variable_order: Box<[Var]> = normalized_rule
            .body_variable_order()
            .iter()
            .filter_map(|v| var_ids.get(v).copied())
            .collect();
        let head_variable_order: Box<[Var]> = normalized_rule
            .head_variable_order()
            .iter()
            .filter_map(|v| var_ids.get(v).copied())
            .collect();

        // There is no join to guide an order for the negative-body atoms (they don't partake in
        // joins), so compute one against a synthetic auxiliary rule the same way the heuristic
        // is otherwise applied, matching `build_preferable_variable_orders_for_rule`'s use above.
        let negative_variable_order: Box<[Var]> = {
            let mut universal_variables: Vec<OrigVariable> = normalized_rule
                .negative()
                .flat_map(|atom| atom.terms().cloned())
                .collect();
            universal_variables.dedup();

            let aux_head = HeadAtom::new(
                Tag::new(String::from("__AUX")),
                universal_variables.iter().cloned().map(Primitive::from),
            );
            let aux_rule = NormalizedRule::positive_rule(
                vec![aux_head],
                normalized_rule.negative().cloned().collect(),
                vec![],
            );
            let all_vars: HashSet<OrigVariable> = normalized_rule.variables().cloned().collect();
            let order =
                build_preferable_variable_orders_for_rule(&aux_rule, None).restrict_to(&all_vars);

            order.iter().filter_map(|v| var_ids.get(v).copied()).collect()
        };

        Self {
            head,
            body_pos,
            body_neg,
            operations,
            consts,
            var_count: next_id,
            body_variable_order,
            head_variable_order,
            negative_variable_order,
        }
    }
}

/// Merge two rules' `consts` maps (their variable ids are assumed disjoint, e.g. because one of
/// them has been [`Rule::prime`]d against the other).
pub(crate) fn combined_consts(rule1: &Rule, rule2: &Rule) -> HashMap<Var, Constant> {
    let mut consts = rule1.consts.clone();
    consts.extend(&rule2.consts);
    consts
}

/// A collection of atoms grouped by predicate ("color"), as flat tuples.
/// This is the compact representation used by the `chain` solver machinery; it is
/// derived from a [Rule] rather than being the primary representation reliance-checking
/// works against (see [Rule] for that).
pub(crate) struct Atoms {
    pub(crate) tuples: Box<[super::tuples::Tuples]>,
    pub(crate) preds: HashMap<Predicate, usize>,
}
