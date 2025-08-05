//! Model of rules supported by the saturation algorithm

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nemo_physical::{datatypes::StorageValueT, dictionary::DvDict, management::database::Dict};

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

type VariableIdx = u16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BodyTerm {
    Constant(StorageValueT),
    Variable(VariableIdx),
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SaturationAtom {
    predicate: Arc<str>,
    terms: Box<[BodyTerm]>,
}

impl SaturationAtom {
    fn variables(&self) -> impl Iterator<Item = VariableIdx> + use<'_> {
        self.terms.iter().flat_map(|term| match term {
            BodyTerm::Variable(var) => Some(*var),
            _ => None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HeadTerm {
    Existential(VariableIdx),
    Universal(VariableIdx),
    Constant(StorageValueT),
}

struct HeadAtom {
    predicate: Arc<str>,
    terms: Box<[HeadTerm]>,
}

enum Head {
    Datalog(Box<[SaturationAtom]>),
    Existential(Box<[HeadTerm]>),
}

type JoinOrder = Arc<[JoinOp]>;

enum JoinOp {
    Join(SaturationAtom),
    Filter(SaturationAtom),
}

struct SaturationRule {
    body_atoms: Box<[SaturationAtom]>,
    join_orders: Box<[JoinOrder]>,
    head: Head,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SaturationFact {
    predicate: Arc<str>,
    values: Box<[StorageValueT]>,
}

struct Variables(HashMap<String, u16>);

impl Variables {
    fn get(&mut self, var: Cow<str>) -> u16 {
        match self.0.get(var.as_ref()) {
            Some(index) => *index,
            None => {
                let index = self
                    .0
                    .len()
                    .try_into()
                    .expect("number of variables must be smaller than u16::MAX");

                self.0.insert(var.to_string(), index);
                index
            }
        }
    }
}

fn convert_term(term: Term, dict: &mut Dict, vars: &mut Variables) -> Result<BodyTerm, ()> {
    match GroundTerm::try_from(term) {
        Ok(ground) => {
            let value = ground.value().to_storage_value_t_dict(dict);
            Ok(BodyTerm::Constant(value))
        }
        Err(term) => {
            let Term::Primitive(Primitive::Variable(var)) = term else {
                return Err(());
            };

            let Variable::Universal(var) = var else {
                return Err(());
            };

            match var.name() {
                Some(name) => Ok(BodyTerm::Variable(vars.get(Cow::Borrowed(name)))),
                None => Ok(BodyTerm::Ignore),
            }
        }
    }
}

struct Interner(HashSet<Arc<str>>);

impl Interner {
    fn create(&mut self, input: &str) -> Arc<str> {
        if let Some(res) = self.0.get(input) {
            return res.clone();
        } else {
            self.0.insert(Arc::from(input));
            self.0.get(input).unwrap().clone()
        }
    }
}

fn convert_atom(atom: &Atom, dict: &mut Dict, vars: &mut Variables) -> Result<SaturationAtom, ()> {
    let predicate = Arc::from(atom.predicate().name());

    let terms: Box<[BodyTerm]> = atom
        .terms()
        .map(|term| convert_term(term.clone(), dict, vars))
        .collect::<Result<_, ()>>()?;

    Ok(SaturationAtom { predicate, terms })
}

fn convert_literal(
    lit: &Literal,
    dict: &mut Dict,
    vars: &mut Variables,
) -> Result<SaturationAtom, ()> {
    match lit {
        Literal::Positive(atom) => convert_atom(atom, dict, vars),
        Literal::Negative(_) => Err(()),
        Literal::Operation(_) => Err(()),
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

fn convert(rule: Rule, dict: &mut Dict) -> Result<SaturationRule, ()> {
    let mut vars = Variables(HashMap::new());

    let body: Box<[SaturationAtom]> = rule
        .body()
        .iter()
        .map(|lit| convert_literal(lit, dict, &mut vars))
        .collect::<Result<_, ()>>()?;

    let join_orders: Box<[JoinOrder]> = body
        .iter()
        .enumerate()
        .map(|(idx, atom)| {
            let variables: HashSet<_> = atom.variables().collect();
            let mut mask = vec![true; body.len()];
            mask[idx] = false;

            compute_join_order(variables, &body, &mut mask)
        })
        .collect();

    let head = if rule.variables().any(Variable::is_existential) {
        todo!()
    } else {
        Head::Datalog(
            rule.head()
                .iter()
                .map(|atom| convert_atom(atom, dict, &mut vars))
                .collect::<Result<_, ()>>()?,
        )
    };

    Ok(SaturationRule {
        body_atoms: body,
        join_orders,
        head,
    })
}
