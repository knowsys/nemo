//! Functionality 
use nemo::rule_model::{
    components::{
        atom::Atom,
        rule::Rule,
        fact::Fact,
        tag::Tag,
        term::{
            Term,
            function::FunctionTerm,
            primitive::{Primitive, variable::Variable},
        },
    },
};
use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_set::{RuleRefs};

use std::collections::{HashMap, HashSet};


pub mod cyclicity;
pub mod acyclicity;

pub fn union<'a>(
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    other_facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    other_facts_by_pred
        .into_iter()
        .fold(facts_by_pred, |mut ret_val, (pred, facts)| {
            ret_val
                .entry(pred)
                .and_modify(|cur_facts| cur_facts.insert_all_take(facts.clone()))
                .or_insert(facts);
            ret_val
        })
}

fn facts_for_assignment<'a>(
    atoms: Vec<&'a Atom>,
    ass: &Assignment<'_>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    atoms.iter().fold(
        HashMap::<&Tag, HashSet<Fact>>::new(),
        |mut ret_val, atom| {
            let fact: Fact = assign(atom, ass);
            let pred: &'a Tag = atom.predicate_ref();
            ret_val
                .entry(pred)
                .and_modify(|facts| {
                    facts.insert(fact.clone());
                })
                .or_insert(HashSet::from([fact]));
            ret_val
        },
    )
}


pub fn body_for_assignment<'a>(
    rule: &'a Rule,
    ass: &Assignment,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let body: Vec<&Atom> = rule.body_positive_refs();
    facts_for_assignment(body, ass)
}

pub fn head_for_assignment<'a>(
    rule: &'a Rule,
    ass: &Assignment,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let head: Vec<&Atom> = rule.head_refs();
    facts_for_assignment(head, ass)
}

fn assign_rec(sk_func: &FunctionTerm, ass: &Assignment) -> Term {
    let subterms: Vec<Term> = sk_func
        .terms()
        .map(|term| match term {
            Term::Primitive(Primitive::Variable(var)) => ass.get(var).unwrap().clone(),
            Term::FunctionTerm(sk_func) => assign_rec(sk_func, ass),
            _ => panic!(),
        })
        .collect();
    let pred: &Tag = sk_func.tag();
    Term::FunctionTerm(FunctionTerm::from((pred, subterms)))
}

fn assign(atom: &Atom, ass: &Assignment) -> Fact {
    let subterms: Vec<Term> = atom
        .terms()
        .map(|term| match term {
            Term::Primitive(Primitive::Variable(var)) => ass.get(var).unwrap().clone(),
            Term::FunctionTerm(sk_func) => assign_rec(sk_func, ass),
            _ => panic!(),
        })
        .collect();
    let pred: &Tag = atom.predicate_ref();
    Fact::from((pred, subterms))
}

#[derive(Clone, Copy)]
pub enum StrategySelector {
    MFA,
    RMFA,
}

pub trait CyclicityStrategy {
    fn is_blocked(&self, rule: &Rule, ass: &Assignment) -> bool;
}

#[derive(Default)]
pub struct NoBlockStrategy;

impl CyclicityStrategy for NoBlockStrategy {
    fn is_blocked(&self, _rule: &Rule, _ass: &Assignment) -> bool {
        false
    }
}



pub type Assignment<'a> = HashMap<&'a Variable, Term>;
pub type VarPerAtomIdxPosIdx<'a> = HashMap<(usize, usize, Option<usize>), &'a Variable>;
pub type VarPerAtomIdxPosIdxPerRule<'a> = HashMap<&'a Rule, VarPerAtomIdxPosIdx<'a>>;
