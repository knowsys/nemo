use std::{
    collections::{HashMap, HashSet},
    num::NonZero,
};
use std::hash::Hash;

use crate::{
    execution::planning::normalization::{self, rule::NormalizedRule},
    rule_model::components::{
        tag::Tag,
        term::{operation::operation_kind::OperationKind, primitive::Primitive},
    },
};
use nemo_physical::datavalues::AnyDataValue;

use crate::rule_model::components::term::primitive::variable::Variable as OrigVariable;

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub struct Predicate {
    key: usize,
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub struct Variable {
    key: usize,
    prime: Option<NonZero<u16>>,
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub struct Constant {
    key: usize,
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub enum Term {
    Variable(Variable),
    Constant(Constant),
}

impl Term {
    fn as_variable(&self) -> Option<Variable> {
        match self {
            Term::Variable(variable) => Some(*variable),
            _ => None,
        }
    }
}

trait Prime {
    fn prime(&self) -> Self;
}

impl Prime for Variable {
    fn prime(&self) -> Self {
        Self {
            key: self.key,
            prime: Some(
                self.prime
                    .map(|p| p.checked_add(1).expect("prime should not overflow"))
                    .unwrap_or(NonZero::new(1).expect("one is not zero")),
            ),
        }
    }
}

pub struct Substitution {
    map: HashMap<Variable, Term>,
}

impl Substitution {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, from: Variable, to: Term) {
        self.map.insert(from, to);
    }

    /// Check is this is the identity substitution.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Add a new mapping from `old_y` to `new_y` and adjust existing mappings onto `old_y`.
    pub fn remap(&mut self, old_y: Variable, new_y: Term) {
        let term_old_y = Term::Variable(old_y);
        debug_assert!(term_old_y != new_y, "trying to add identity transmutation");
        debug_assert!(
            !self.map.contains_key(&old_y),
            "domain and range of unifier are not disjoint"
        );
        // modify all entries pointing to old_y to now point to new_y instead
        self.map
            .iter_mut()
            .filter(|(_k, v)| **v == term_old_y)
            .for_each(|(_k, v)| {
                *v = new_y.clone();
            });
        self.map.insert(old_y, new_y);
    }

    /// Resolve a primitive w.r.t. the substitution.
    pub fn get_term<'a>(&'a self, primitive: &'a Term) -> Option<&'a Term> {
        match primitive {
            Term::Variable(_) => self.map.get(&primitive.as_variable()?),
            Term::Constant(_) => Some(primitive),
        }
    }

    /// Resolve a variable w.r.t. the substitution.
    pub fn get_variable<'a>(&'a self, variable: &'a Variable) -> Option<Variable> {
        self.map
            .get(variable)?
            .as_variable()
    }

    /// Resolve all variables in the iterator w.r.t. the substitution.
    pub fn substitute_variables<'a>(
        &'a self,
        vars: impl IntoIterator<Item = &'a Variable>,
    ) -> impl Iterator<Item = Variable> {
        vars.into_iter().filter_map(|v| self.get_variable(v))
    }

    /// Check if the gien variable is mapped by this substitution.
    pub fn contains_variable(&self, k: &Variable) -> bool {
        self.map.contains_key(k)
    }

    /// Restrict this substitution to the given domain of variables.
    pub fn restriction(&self, domain: &HashSet<&Variable>) -> Self {
        Self {
            map: self
                .map
                .clone()
                .into_iter()
                .filter(|(from, _to)| {
                    domain.contains(from)
                })
                .collect(),
        }
    }

    /// f.compose(g) means (f ∘︎ g)(x) = f(g(x)
    pub fn compose(&self, other: &Self) -> Self {
        Self {
            map: other
                .map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        match v {
                            Term::Variable(primitive) => self.map.get(primitive).cloned(),
                            _ => None,
                        }
                        .unwrap_or(v.clone()),
                    )
                })
                .chain(
                    self.map
                        .iter()
                        .filter(|(k, _v)| !other.map.contains_key(k))
                        .map(|(k, v)| (k.clone(), v.clone())),
                )
                .filter(|(k, v)| Term::Variable(k.clone()) != *v)
                .collect(),
        }
    }
}

trait Apply {
    fn apply(&self, eta: &Substitution) -> Self;
}

//impl Apply for HashSet<Variable> {
//    fn apply(&self, eta: &Substitution) -> Self {
//        self.iter()
//            .filter_map(|var| self.map.get(var)?.as_variable())
//            .collect()
//    }
//}

impl Prime for Term {
    fn prime(&self) -> Self {
        match self {
            Self::Variable(variable) => Self::Variable(variable.prime()),
            _ => *self,
        }
    }
}

impl Apply for Term {
    fn apply(&self, eta: &Substitution) -> Self {
        match self {
            Term::Variable(variable) => *eta.map.get(variable).unwrap_or_else(|| self),
            _ => *self,
        }
    }
}

#[derive(Debug)]
struct Atom {
    predicate: Predicate,
    terms: Box<[Term]>,
}

impl Prime for Atom {
    fn prime(&self) -> Self {
        Self {
            predicate: self.predicate,
            terms: self.terms.iter().map(|term| term.prime()).collect(),
        }
    }
}

impl Apply for Atom {
    fn apply(&self, eta: &Substitution) -> Self {
        Self {
            predicate: self.predicate,
            terms: self.terms.apply(eta),
        }
    }
}

#[derive(Debug)]
pub enum Operation {
    /// Primitive term
    Primitive(Term),
    /// Operation
    Operation {
        /// Type of operation
        kind: OperationKind,
        /// Input to the opreation
        subterms: Box<[Operation]>,
    },
}

impl Prime for Operation {
    fn prime(&self) -> Self {
        match self {
            Operation::Primitive(term) => Operation::Primitive(term.prime()),
            Operation::Operation { kind, subterms } => Operation::Operation {
                kind: *kind,
                subterms: subterms.prime(),
            },
        }
    }
}

impl Apply for Operation {
    fn apply(&self, eta: &Substitution) -> Self {
        match self {
            Operation::Primitive(term) => Operation::Primitive(term.apply(eta)),
            Operation::Operation { kind, subterms } => Operation::Operation {
                kind: *kind,
                subterms: subterms.apply(eta),
            },
        }
    }
}

impl Operation {
    fn from_normalized_operation(
        var_map: &mut ComponentMap<OrigVariable>,
        const_map: &mut ComponentMap<AnyDataValue>,
        operation: &normalization::operation::Operation,
    ) -> Self {
        match operation {
            normalization::operation::Operation::Primitive(primitive) => {
                Operation::Primitive(match primitive {
                    Primitive::Variable(variable) => Term::Variable(Variable {
                        key: var_map.get(variable),
                        prime: None,
                    }),
                    Primitive::Ground(ground_term) => Term::Constant(Constant {
                        key: const_map.get(&ground_term.value()),
                    }),
                })
            }
            normalization::operation::Operation::Operation { kind, subterms } => {
                Operation::Operation {
                    kind: *kind,
                    subterms: subterms
                        .iter()
                        .map(|operation| {
                            Operation::from_normalized_operation(var_map, const_map, operation)
                        })
                        .collect(),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Rule {
    head: Box<[Atom]>,
    body_pos: Box<[Atom]>,
    body_neg: Box<[Atom]>,
    operations: Box<[Operation]>,
}

impl<T: Prime, I: IntoIterator<Item = T>> Prime for I {
    fn prime(&self) -> Self {
        self.into_iter().map(|t| t.prime()).collect()
    }
}

impl<'a, T: Apply + 'a, I: IntoIterator<Item = &'a T> + FromIterator<T>> Apply for I {
    fn apply(&self, eta: &Substitution) -> Self {
        self.into_iter().map(|t| t.apply(eta)).collect()
    }
}

impl Prime for Rule {
    fn prime(&self) -> Self {
        Self {
            head: self.head.prime(),
            body_pos: self.body_pos.prime(),
            body_neg: self.body_neg.prime(),
            operations: self.operations.prime(),
        }
    }
}

impl Apply for Rule {
    fn apply(&self, eta: &Substitution) -> Self {
        Self {
            head: self.head.apply(eta),
            body_pos: self.body_pos.apply(eta),
            body_neg: self.body_neg.apply(eta),
            operations: self.operations.apply(eta),
        }
    }
}

#[derive(Debug)]
pub struct ComponentMap<T>(HashMap<T, usize>);

impl<T :Eq + Hash + Clone> ComponentMap<T> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    fn get(&mut self, t: &T) -> usize {
        let l = self.0.len();
        *self.0.entry(t.clone()).or_insert_with(|| l)
    }
}

impl Rule {
    pub fn operations(&self) -> &Box<[Operation]> {
        &self.operations
    }

    pub fn from_normalized_rule(
        pred_map: &mut ComponentMap<Tag>,
        const_map: &mut ComponentMap<AnyDataValue>,
        normalized_rule: &NormalizedRule,
    ) -> (Self, ComponentMap<OrigVariable>) {
        let mut var_map = ComponentMap::new();
        (
            Self {
                head: normalized_rule
                    .head()
                    .iter()
                    .map(|head_atom| Atom {
                        predicate: Predicate {
                            key: pred_map.get(&head_atom.predicate()),
                        },
                        terms: head_atom
                            .terms()
                            .map(|primitive| match primitive {
                                Primitive::Variable(variable) => Term::Variable(Variable {
                                    key: var_map.get(variable),
                                    prime: None,
                                }),
                                Primitive::Ground(ground_term) => Term::Constant(Constant {
                                    key: const_map.get(&ground_term.value()),
                                }),
                            })
                            .collect(),
                    })
                    .collect(),
                body_pos: normalized_rule
                    .positive()
                    .iter()
                    .map(|body_atom| Atom {
                        predicate: Predicate {
                            key: pred_map.get(&body_atom.predicate()),
                        },
                        terms: body_atom
                            .terms()
                            .map(|variable| {
                                Term::Variable(Variable {
                                    key: var_map.get(variable),
                                    prime: None,
                                })
                            })
                            .collect(),
                    })
                    .collect(),
                body_neg: normalized_rule
                    .negative()
                    .map(|body_atom| Atom {
                        predicate: Predicate {
                            key: pred_map.get(&body_atom.predicate()),
                        },
                        terms: body_atom
                            .terms()
                            .map(|variable| {
                                Term::Variable(Variable {
                                    key: var_map.get(variable),
                                    prime: None,
                                })
                            })
                            .collect(),
                    })
                    .collect(),
                operations: normalized_rule
                    .operations()
                    .iter()
                    .map(|operation| {
                        Operation::from_normalized_operation(&mut var_map, const_map, operation)
                    })
                    .collect(),
            },
            var_map,
        )
    }
}
