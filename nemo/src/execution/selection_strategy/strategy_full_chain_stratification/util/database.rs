use std::collections::{HashMap, HashSet};

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, ComponentMap, Constant, Predicate, Var,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::hypergraph::Hypergraph;
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::solver::Solver;
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::tuples::Tuples;

/// The resolved value of a term slot: either it turned out to be bound to a constant, or it is
/// still a (possibly still-free) variable.
#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub(crate) enum Value {
    Constant(Constant),
    Variable(Var),
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub(crate) struct RepresentativeAtom {
    predicate: Predicate,
    values: Vec<Value>,
}

impl RepresentativeAtom {
    pub(crate) fn predicate(&self) -> Predicate {
        self.predicate
    }

    pub(crate) fn values(&self) -> &[Value] {
        &self.values
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = Var> + '_ {
        self.values.iter().filter_map(|v| match v {
            Value::Variable(x) => Some(*x),
            Value::Constant(_) => None,
        })
    }

    /// Resolve `atom`'s terms w.r.t. `eta`, and classify each resolved id as constant or variable
    /// via `consts` (which must cover every rule whose variable ids `atom` or `eta`'s range may
    /// reference, e.g. via [`super::super::chain::atoms::combined_consts`]).
    pub(crate) fn from_atom_with_substitution(
        eta: &Substitution,
        consts: &HashMap<Var, Constant>,
        atom: &Atom,
    ) -> Self {
        Self {
            predicate: atom.predicate(),
            values: atom
                .terms()
                .iter()
                .map(|&t| {
                    let resolved = eta.resolve(t);
                    match consts.get(&resolved) {
                        Some(&c) => Value::Constant(c),
                        None => Value::Variable(resolved),
                    }
                })
                .collect(),
        }
    }

    pub(crate) fn substitute_atoms<'a>(
        eta: &'a Substitution,
        consts: &'a HashMap<Var, Constant>,
        atoms: impl IntoIterator<Item = Atom> + 'a,
    ) -> impl Iterator<Item = Self> + 'a {
        atoms
            .into_iter()
            .map(move |atom| Self::from_atom_with_substitution(eta, consts, &atom))
    }
}

#[derive(Debug)]
pub(crate) struct RepresentativeDatabase(HashMap<Predicate, HashSet<Vec<Value>>>);

impl RepresentativeDatabase {
    pub(crate) fn new<'a>(facts: impl IntoIterator<Item = &'a RepresentativeAtom>) -> Self {
        Self(HashMap::new()).add_facts(facts)
    }

    pub(crate) fn add_facts<'a>(
        mut self,
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> Self {
        // here, variables are allowed in the database with the understanding that they are replaced with fresh variables injectively
        for fact in facts.into_iter() {
            self.0
                .entry(fact.predicate())
                .or_default()
                .insert(fact.values().to_vec());
        }
        self
    }

    /// Is there an assignment of `existentials` such that all of `atoms` (with every other value
    /// treated as fixed) hold in this database? This is a homomorphism-existence query: build the
    /// atoms as a pattern hypergraph (existentials as free source vertices, everything else
    /// pre-assigned to its known target vertex) and the matching facts as the target hypergraph,
    /// then ask the `chain` solver whether a homomorphism exists.
    pub(crate) fn entails<'a>(
        &self,
        existentials: &HashSet<Var>,
        atoms: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> bool {
        let atoms: Vec<&RepresentativeAtom> = atoms.into_iter().collect();
        if atoms.is_empty() {
            return true;
        }

        // Distinct predicates referenced, in first-appearance order, with their arity and the
        // database's facts -- bail out immediately if any has no facts at all (nothing could
        // possibly match), exactly as the direct check used to.
        let mut pred_index: HashMap<Predicate, usize> = HashMap::new();
        let mut arities: Vec<usize> = Vec::new();
        let mut facts_per_color: Vec<&HashSet<Vec<Value>>> = Vec::new();
        for atom in &atoms {
            let pred = atom.predicate();
            if !pred_index.contains_key(&pred) {
                let Some(facts) = self.0.get(&pred) else {
                    return false;
                };
                pred_index.insert(pred, arities.len());
                arities.push(atom.values().len());
                facts_per_color.push(facts);
            }
        }
        let colors = arities.len();

        // Intern query values into source (pattern) vertices; every non-existential slot's value
        // is already fixed, so it's additionally interned into a target vertex right away.
        let mut source_vertex_of: ComponentMap<Value> = ComponentMap::new();
        let mut target_vertex_of: ComponentMap<Value> = ComponentMap::new();
        let mut pre_assigned: Vec<Option<usize>> = Vec::new();
        let mut source_rows: Vec<Vec<usize>> = vec![Vec::new(); colors];

        for atom in &atoms {
            let color = pred_index[&atom.predicate()];
            for &value in atom.values() {
                let before = source_vertex_of.len();
                let source_vertex = source_vertex_of.get(&value);
                if source_vertex == before {
                    let is_existential = matches!(value, Value::Variable(v) if existentials.contains(&v));
                    pre_assigned.push(if is_existential {
                        None
                    } else {
                        Some(target_vertex_of.get(&value))
                    });
                }
                source_rows[color].push(source_vertex);
            }
        }

        // Intern every relevant fact's values into target vertices too, aligned with the
        // pattern's colors.
        let mut target_rows: Vec<Vec<usize>> = vec![Vec::new(); colors];
        for (color, facts) in facts_per_color.iter().enumerate() {
            for fact in facts.iter() {
                for &v in fact {
                    target_rows[color].push(target_vertex_of.get(&v));
                }
            }
        }

        let source_tuples: Vec<Tuples> = source_rows
            .into_iter()
            .zip(&arities)
            .map(|(data, &arity)| Tuples::from_rows(arity, data.chunks(arity).map(|c| c.to_vec())))
            .collect();
        let target_tuples: Vec<Tuples> = target_rows
            .into_iter()
            .zip(&arities)
            .map(|(data, &arity)| Tuples::from_rows(arity, data.chunks(arity).map(|c| c.to_vec())))
            .collect();

        let source = Hypergraph::from_tuples(source_tuples, source_vertex_of.len());
        let target = Hypergraph::from_tuples(target_tuples, target_vertex_of.len());

        let mut solver = Solver::from_hypergraphs_partial(&source, &target, &pre_assigned);
        solver.solve()
    }

    pub(crate) fn contains<'a>(
        &self,
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> bool {
        facts.into_iter().all(|fact| {
            self.0
                .get(&fact.predicate())
                .is_some_and(|extent| extent.contains(fact.values()))
        })
    }

    /// Debug-format a set of facts (for `log::trace!` call sites); `db`, if given, restricts the
    /// printed facts to those already contained in it.
    pub(crate) fn display<'a>(
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
        db: Option<&Self>,
    ) -> String {
        let parts: Vec<String> = facts
            .into_iter()
            .filter(|fact| db.map(|db| db.contains([*fact])).unwrap_or(true))
            .map(|fact| format!("{:?}({:?})", fact.predicate(), fact.values()))
            .collect();
        format!("{{ {} }}", parts.join(", "))
    }
}
