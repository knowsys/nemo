//! This module defines [TransformationProjectionPushing].

use std::{collections::{HashMap, HashSet}};

use crate::rule_model::{
    components::{atom::Atom, literal::Literal, rule::Rule, statement::Statement, tag::Tag, term::{primitive::Primitive, Term}, IterablePrimitives},
    error::ValidationReport,
    programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Removes any predicate position that is not needed to compute the output.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationProjectionPushing {}

impl TransformationProjectionPushing {
    fn collect_marked_postions(program: &ProgramHandle) -> HashMap<Tag,HashSet<usize>> {
        let mut marked_positions = HashMap::new();

        // collect positions of output predicates as marked positions 
        for export in program.exports() {
            let tag = export.predicate();
            let arity = match export.expected_arity() {
                Some(arity) => arity,
                None => *program.arities().get(export.predicate()).expect("Output predicate should have implicit or explicit arity"),
            };
            let positions: HashSet<usize> = (0..arity).collect();
            marked_positions.insert(tag.clone(), positions);
        }

        let mut done = false;
        while !done {
            done = true;
            for rule in program.rules() {
                done = done && !Self::update_markings_for_rule(rule, &mut marked_positions);
            }
        }

        for (pred, positions) in marked_positions.iter() {
            let positions: Vec<String> = positions.iter().map(|pos| pos.to_string()).collect();
            log::info!("{}: {}", pred, positions.join(" "));
        }

        marked_positions
    }

    // update markings for given rule
    // returns whether an update was made
    fn update_markings_for_rule(rule: &Rule, marked_positions: &mut HashMap<Tag, HashSet<usize>>) -> bool {
        match Self::collect_marked_terms(rule, marked_positions) {
            None => false,
            Some(marked_terms) => {
                let mut changed = false;
                for atom in rule.body() {
                    let markings = Self::collect_markings_for_atom(atom, &marked_terms);
                    if let Some(predicate) = atom.predicate() {
                        match marked_positions.get_mut(&predicate) {
                            None => {
                                marked_positions.insert(predicate, markings);
                                changed = true;
                            },
                            Some(x) => {
                                if !markings.is_subset(x) {
                                    changed = true;
                                    x.extend(markings);
                                }
                            }
                        }
                    }
                }
                changed
            }
        }
    }

    // collect marked terms of given rule
    // returns None if no head predicate was marked initially
    fn collect_marked_terms<'a>(rule: &'a Rule, marked_positions: &HashMap<Tag, HashSet<usize>>) -> Option<HashSet<&'a Primitive>> {
        let mut relevant = false;
        let mut marked_terms: HashSet<&Primitive> = HashSet::new();

        for atom in rule.head() {
            // collect positions of all head atoms with a marked predicate
            if let Some(markings) = marked_positions.get(&atom.predicate()) {
                relevant = true;
                for (i, term) in atom.terms().enumerate() {
                    if let Term::Primitive(prim) = term {
                        if markings.contains(&i) {
                            marked_terms.insert(prim);
                        }
                    }
                }
            }
        }

        if relevant {
            let prims: Vec<&Primitive> = rule.body().iter().flat_map(|atom|atom.primitive_terms()).collect();
            for prim in prims.iter() {
                match prim {
                    // add all ground terms, aka. constants
                    Primitive::Ground(_) => { marked_terms.insert(prim); }
                    // add variables that occur at least twice
                    Primitive::Variable(_) => { 
                        if prims.iter().filter(|p|p.eq(&prim)).count() > 1 {
                            marked_terms.insert(prim);
                        }
                    }
                }
            }

            Some(marked_terms)
        } else {
            None
        }
    }

    // collect marked positions for atom based on given marked terms
    fn collect_markings_for_atom(atom: &Literal, marked_terms: &HashSet<&Primitive>) -> HashSet<usize> {
        let mut markings = HashSet::new();
        for (i, term) in atom.terms().enumerate() {
            if let Term::Primitive(prim) = term {
                if marked_terms.contains(&prim) {
                    markings.insert(i);
                }
            } 
        }

        markings
    }

    // project the literal to the positions given as markers
    fn project_literal(literal: &Literal, markers: &HashSet<usize>) -> Literal {
        match literal {
            Literal::Operation(_) => literal.clone(),
            Literal::Positive(atom) => Literal::Positive(Self::project_atom(atom, markers)),
            Literal::Negative(atom) => Literal::Negative(Self::project_atom(atom, markers)),
        }
    }

    // project the atom to the positions given as markers
    fn project_atom(atom: &Atom, markers: &HashSet<usize>) -> Atom {
        let terms: Vec<Term>  = atom.terms().enumerate()
            .filter(|(i,_term)|markers.contains(i))
            .map(|(_i,term)|term.clone())
            .collect();
        Atom::new(atom.predicate(), terms)
    }
}

impl ProgramTransformation for TransformationProjectionPushing {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();
        let markers = Self::collect_marked_postions(program);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let head: Vec<Atom> = rule.head().iter().filter_map(|atom|{
                        match markers.get(&atom.predicate()) {
                            Some(markers) => Some(Self::project_atom(atom, markers)),
                            None => None
                        }
                    }).collect();
                    let body: Vec<Literal> = rule.body().iter().filter_map(|literal|{
                        match literal.predicate() {
                            None => Some(literal.clone()),
                            Some(predicate) => match markers.get(&predicate) {
                                Some(markers) => Some(Self::project_literal(literal, markers)),
                                None => None
                            }
                        }
                    }).collect();
                    commit.add_rule(Rule::new(head, body));
                }
                Statement::Fact(fact) => {
                    // TODO: update fact
                    commit.keep(fact);
                }
                Statement::Import(import) => {
                    commit.keep(import);
                }
                Statement::Export(_) | Statement::Output(_) | Statement::Parameter(_) => {
                    commit.keep(statement)
                }
            }
        }

        commit.submit()
    }
}
