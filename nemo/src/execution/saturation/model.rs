//! Model of rules supported by the saturation algorithm

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nemo_physical::{datatypes::StorageValueT, management::database::Dict};

use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    term::{
        primitive::{ground::GroundTerm, variable::Variable, Primitive},
        Term,
    },
    IterableVariables,
};

pub(crate) type VariableIdx = u16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// Terms supported in the body of [`SaturationRule`]s
pub(crate) enum BodyTerm {
    Constant(StorageValueT),
    Variable(VariableIdx),
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// Atoms whose arguments are represented by [`BodyTerm`]
pub(crate) struct SaturationAtom {
    pub(super) predicate: Arc<str>,
    pub(super) terms: Box<[BodyTerm]>,
    pub(super) equality: Option<(VariableIdx, VariableIdx)>,
}

impl SaturationAtom {
    /// Iterate over the variables in a [`SaturationAtom`]
    pub(super) fn variables(&self) -> impl Iterator<Item = VariableIdx> + use<'_> {
        self.terms.iter().flat_map(|term| match term {
            BodyTerm::Variable(var) => Some(*var),
            _ => None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Head {
    Datalog(Box<[SaturationAtom]>),
}

pub(crate) type JoinOrder = Arc<[JoinOp]>;

#[derive(Debug, Clone)]
pub(crate) enum JoinOp {
    Join(SaturationAtom),
    Filter(SaturationAtom),
}

#[derive(Debug, Clone)]
pub(crate) struct SaturationRule {
    pub(super) body_atoms: Arc<[SaturationAtom]>,
    pub(super) join_orders: Box<[Option<JoinOrder>]>,
    pub(super) head: Head,
}

impl SaturationRule {
    pub(super) fn join_order(&mut self, index: usize) -> JoinOrder {
        if let Some(order) = &self.join_orders[index] {
            order.clone()
        } else {
            let atom = &self.body_atoms[index];
            let variables: HashSet<_> = atom.variables().collect();
            let mut mask = vec![true; self.body_atoms.len()];
            mask[index] = false;

            let order = compute_join_order(variables, &self.body_atoms, &mut mask);

            self.join_orders[index] = Some(order.clone());
            order
        }
    }

    #[allow(unused)]
    pub(crate) fn input_predicates(&self) -> impl Iterator<Item = Arc<str>> + use<'_> {
        self.body_atoms.iter().map(|a| a.predicate.clone())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SaturationFact {
    pub(super) predicate: Arc<str>,
    pub(super) values: Arc<[StorageValueT]>,
}

#[derive(Default)]
struct Variables(HashMap<String, u16>, u16);

impl Variables {
    fn get(&mut self, var: Cow<str>) -> u16 {
        match self.0.get(var.as_ref()) {
            Some(index) => *index,
            None => {
                let index = self.add_fresh();
                self.0.insert(var.to_string(), index);
                index
            }
        }
    }

    fn add_fresh(&mut self) -> u16 {
        let index = self.1;
        self.1 += 1;
        index
    }
}

impl SaturationRuleTranslation<'_> {
    fn convert_term(&mut self, term: &Term) -> Result<BodyTerm, &'static str> {
        match GroundTerm::try_from(term.clone()) {
            Ok(ground) => {
                let value = ground.value().to_storage_value_t_dict(self.dict);
                Ok(BodyTerm::Constant(value))
            }
            Err(term) => {
                let Term::Primitive(Primitive::Variable(var)) = term else {
                    return Err("not a ground-term or variable");
                };

                let Variable::Universal(var) = var else {
                    return Err("existential");
                };

                match var.name() {
                    Some(name) => Ok(BodyTerm::Variable(self.variables.get(Cow::Borrowed(name)))),
                    None => Ok(BodyTerm::Ignore),
                }
            }
        }
    }

    fn convert_body_atom(&mut self, atom: &Atom) -> Result<SaturationAtom, &'static str> {
        let predicate = self.interner.create(atom.predicate().name());

        let mut terms: Box<[BodyTerm]> = atom
            .terms()
            .map(|term| self.convert_term(term))
            .collect::<Result<_, _>>()?;

        let mut equality = None;

        for i in 0..terms.len() {
            let (left, right) = terms.split_at_mut(i + 1);
            if let BodyTerm::Variable(v) = &mut left[i]
                && right.iter().any(|other| other == &BodyTerm::Variable(*v)) {
                    if equality.is_some() {
                        return Err("only supports a single equality");
                    }

                    let orig = *v;
                    *v = self.variables.add_fresh();

                    equality = Some((orig, *v));
                }
        }

        Ok(SaturationAtom {
            predicate,
            terms,
            equality,
        })
    }

    fn convert_head_atom(&mut self, atom: &Atom) -> Result<SaturationAtom, &'static str> {
        let predicate = self.interner.create(atom.predicate().name());

        let terms: Box<[BodyTerm]> = atom
            .terms()
            .map(|term| self.convert_term(term))
            .collect::<Result<_, _>>()?;

        Ok(SaturationAtom {
            predicate,
            terms,
            equality: Default::default(),
        })
    }

    fn convert_literal(&mut self, lit: &Literal) -> Result<SaturationAtom, &'static str> {
        match lit {
            Literal::Positive(atom) => self.convert_body_atom(atom),
            Literal::Negative(_) => Err("negation"),
            Literal::Operation(_) => Err("unsupported operation"),
        }
    }

    pub(crate) fn convert(&mut self, rule: &Rule) -> Result<SaturationRule, &'static str> {
        let body: Arc<[SaturationAtom]> = rule
            .body()
            .iter()
            .map(|lit| self.convert_literal(lit))
            .collect::<Result<_, _>>()?;

        let join_orders: Box<[_]> = std::iter::repeat_n(None, body.len()).collect();

        let head = if rule.variables().any(Variable::is_existential) {
            // existential variable are not supported yet
            return Err("existential");
        } else {
            Head::Datalog(
                rule.head()
                    .iter()
                    .map(|atom| self.convert_head_atom(atom))
                    .collect::<Result<_, _>>()?,
            )
        };

        Ok(SaturationRule {
            body_atoms: body,
            join_orders,
            head,
        })
    }
}

pub(crate) struct SaturationRuleTranslation<'a> {
    variables: Variables,
    interner: Interner,
    dict: &'a mut Dict,
}

impl<'a> SaturationRuleTranslation<'a> {
    /// Create at [`SaturationRuleTranslation`] referring to a [`Dict`]
    pub(crate) fn new(dict: &'a mut Dict) -> Self {
        Self {
            variables: Variables::default(),
            interner: Interner(HashSet::new()),
            dict,
        }
    }
}

struct Interner(HashSet<Arc<str>>);

impl Interner {
    fn create(&mut self, input: &str) -> Arc<str> {
        if let Some(res) = self.0.get(input) {
            res.clone()
        } else {
            self.0.insert(Arc::from(input));
            self.0.get(input).unwrap().clone()
        }
    }
}

fn filter_index(variables: &HashSet<VariableIdx>, atom: &SaturationAtom) -> (i32, i32) {
    let mut other_variables = 0;
    let mut overlapping_variables = 0;

    for var in atom.variables() {
        if variables.contains(&var) {
            overlapping_variables += 1;
        } else {
            other_variables += 1;
        }
    }

    (other_variables, overlapping_variables)
}

fn compute_join_order(
    mut variables: HashSet<VariableIdx>,
    body: &[SaturationAtom],
    mask: &mut [bool],
) -> JoinOrder {
    let mut operations = Vec::new();

    loop {
        let mut index = None;
        let mut min_new_variables = i32::MAX;
        let mut max_overlapping = 0;

        for (current_index, atom) in body
            .iter()
            .enumerate()
            .zip(&mut *mask)
            .filter_map(|(atom, flag)| flag.then_some(atom))
        {
            let (other, overlap) = filter_index(&variables, atom);

            if other < min_new_variables
                || (other == min_new_variables && max_overlapping < overlap)
            {
                min_new_variables = other;
                max_overlapping = overlap;
                index = Some(current_index);
            }
        }

        let Some(index) = index else {
            break JoinOrder::from(operations);
        };

        mask[index] = false;
        if min_new_variables == 0 {
            operations.push(JoinOp::Filter(body[index].clone()));
        } else {
            operations.push(JoinOp::Join(body[index].clone()));
            variables.extend(body[index].variables());
        }
    }
}

#[cfg(test)]
pub(super) fn bench_rules(n: usize) -> (Vec<SaturationRule>, Arc<str>) {
    use std::iter::repeat_n;

    let one = BodyTerm::Constant(StorageValueT::Int64(1));
    let zero = BodyTerm::Constant(StorageValueT::Int64(0));
    let predicate: Arc<str> = Arc::from("p");

    let rules: Vec<_> = (0..n)
        .map(|i| {
            let head = (0..VariableIdx::try_from(i).unwrap())
                .map(BodyTerm::Variable)
                .chain(Some(one))
                .chain(repeat_n(zero, n - i - 1));

            let head = SaturationAtom {
                predicate: predicate.clone(),
                terms: head.collect(),
                equality: Default::default(),
            };

            let body = (0..VariableIdx::try_from(i).unwrap())
                .map(BodyTerm::Variable)
                .chain(Some(zero))
                .chain(repeat_n(one, n - i - 1));

            let body = SaturationAtom {
                predicate: predicate.clone(),
                terms: body.collect(),
                equality: Default::default(),
            };

            SaturationRule {
                body_atoms: Arc::from([body]),
                join_orders: Box::from([None]),
                head: Head::Datalog(Box::from([head])),
            }
        })
        .collect();

    (rules, predicate)
}
