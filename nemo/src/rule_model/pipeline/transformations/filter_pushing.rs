
//! This module defines [TransformationFilterPushing].

use std::{collections::{HashMap, HashSet}, fmt::Display};

use crate::rule_model::{
    components::{atom::Atom, literal::Literal, rule::Rule, statement::Statement, tag::Tag, term::{operation::Operation, primitive::{variable::{positional::PositionalMarker, Variable}, Primitive}, Term}},
    error::ValidationReport,
    programs::{handle::ProgramHandle, ProgramRead},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Pushes filters such that they are applied earlier.
/// Removes filters that has become obsolete.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationFilterPushing {}

#[derive(Debug)]
enum FilterExpression {
    Top,
    Bot,
    Conjunction(HashSet<Literal>)
}

impl TransformationFilterPushing {
    fn init_filters(program: &ProgramHandle) -> HashMap<Tag,FilterExpression> {
        let mut filter_expressions: HashMap<Tag, FilterExpression> = HashMap::new();
        let output_predicates: HashSet<&Tag> = program.exports().map(|export|export.predicate()).collect();

        for predicate in program.derived_predicates() {
            if output_predicates.contains(&predicate) {
                filter_expressions.insert(predicate, FilterExpression::Top);
            } else {
                filter_expressions.insert(predicate, FilterExpression::Bot);
            }
        }

        filter_expressions
    }

    fn compute_filters(program: &ProgramHandle) -> HashMap<Tag,FilterExpression> {
        let mut filter_expressions = Self::init_filters(program);
        let mut queue: Vec<Tag> = program.exports().map(|export|export.predicate().clone()).collect();

        while let Some(predicate) = queue.pop() {
            for rule in program.rules() {
                for head in rule.head() {
                    if head.predicate().eq(&predicate) {
                        for changed_predicate in Self::update_filters(rule, head, &mut filter_expressions) {
                            if !queue.contains(&changed_predicate) {
                                queue.push(changed_predicate);
                            }
                        }
                    }
                }
            }
        }

        filter_expressions
    }

    // push the head filter expression to each body atom with a derived predicate
    // returns set of predicates for which the filter expression has changed
    fn update_filters(rule: &Rule, head: &Atom, filter_expressions: &mut HashMap<Tag,FilterExpression>) -> HashSet<Tag> {
        let mut updated_predicates: HashSet<Tag> = HashSet::new();

        for atom in rule.body_positive() {
            if filter_expressions.contains_key(&atom.predicate()) && Self::push_filter(rule, head, atom, filter_expressions) {
                updated_predicates.insert(atom.predicate());
            }
        }

        // TODO: handle negative body atoms

        updated_predicates
    }

    fn push_filter(rule: &Rule, from: &Atom, to: &Atom, filter_expressions: &mut HashMap<Tag,FilterExpression>) -> bool {
        // get the filter expression to be pushed
        if let Some(from_filter) = filter_expressions.get(&from.predicate()) {
            // construct the mapping used to instantiate the filter expression for the from atom
            // according to the terms occuring in the from atom
            let mut unmarkings: HashMap<PositionalMarker,Variable> = HashMap::new();
            for (i,term) in from.terms().enumerate() {
                if let Term::Primitive(Primitive::Variable(var)) = term {
                    let position = PositionalMarker::new(i);
                    unmarkings.insert(position, var.clone());
                }
            }

            // construct filter of the rule body filter and the instantiated from filter expression
            // initialize filter with instantiated filter expression
            let mut rule_filter: HashSet<Literal> = match from_filter {
                FilterExpression::Top => HashSet::new(),
                FilterExpression::Bot => return false,
                FilterExpression::Conjunction(flt) => flt.iter().filter_map(|literal|Self::unmark(literal, &unmarkings)).collect(),
            };
            // add filter atom in the rule body
            for literal in rule.body() {
                match literal {
                    Literal::Operation(_) => {rule_filter.insert(literal.clone());},
                    Literal::Positive(atom) => {
                        if !filter_expressions.contains_key(&atom.predicate()) {
                            rule_filter.insert(literal.clone());
                        }
                    }
                    Literal::Negative(_) => {},
                };
            }


            // get new filter expression, using positional markers
            let new_flt = match filter_expressions.get(&to.predicate()) {
                // if old filter is top, nothing to be done
                Some(FilterExpression::Top) => {
                    None
                },

                // if old filter is bottom, use the complete closure
                Some(FilterExpression::Bot) => {
                    // construct the mapping used to map variables to positional markersaccoring
                    // according to to atom
                    let mut markings: HashMap<Variable,PositionalMarker> = HashMap::new();
                    for (i,term) in to.terms().enumerate() {
                        if let Term::Primitive(Primitive::Variable(var)) = term {
                            let position = PositionalMarker::new(i);
                            markings.insert(var.clone(), position.clone());
                        }
                    }

                    let mut to_filter: HashSet<Literal> = HashSet::new();
                    for literal in Self::closure(rule_filter) {
                        if let Some(literal) = Self::mark(&literal, &markings) {
                            to_filter.insert(literal);
                        }
                    }
                    if to_filter.is_empty() {
                        Some(FilterExpression::Top)
                    } else {
                        Some(FilterExpression::Conjunction(to_filter))
                    }
                },

                // if old filter is conjunction, keep those literals that are entailed by closure
                Some(FilterExpression::Conjunction(old_flt)) => {
                    let closure = Self::closure(rule_filter);
                    let mut unmarkings: HashMap<PositionalMarker,Variable> = HashMap::new();
                    for (i,term) in to.terms().enumerate() {
                        if let Term::Primitive(Primitive::Variable(var)) = term {
                            let position = PositionalMarker::new(i);
                            unmarkings.insert(position, var.clone());
                        }
                    }

                    let mut new_flt: HashSet<Literal> = HashSet::new();
                    for flt_literal in old_flt {
                        if let Some(literal) = Self::unmark(flt_literal, &unmarkings) {
                            if closure.contains(&literal) {
                                new_flt.insert(flt_literal.clone());
                            }
                        }
                    }

                    // the empty set is defined as top
                    if new_flt.is_empty() {
                        Some(FilterExpression::Top)
                    // check if the filter expression has become more general, i.e., smaller
                    } else if old_flt.is_subset(&new_flt) {
                        None
                    } else {
                        Some(FilterExpression::Conjunction(new_flt))
                    }
                },

                // if to predicate is not in filter expression, it is EDB and nothing to be done
                None => None,
            };

            if let Some(new_flt) = new_flt {
                filter_expressions.insert(to.predicate(), new_flt);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn closure(atoms: HashSet<Literal>) -> HashSet<Literal> {
        atoms
    }

    // replace all variables with positional markers according to map
    // returns None if a term is encountered that cannot be mapped
    fn mark(literal: &Literal, map: &HashMap<Variable,PositionalMarker>) -> Option<Literal> {
        let mut terms: Vec<Term> = Vec::new();
        for term in literal.terms() {
            match term {
                Term::Primitive(Primitive::Ground(_)) => terms.push(term.clone()),
                Term::Primitive(Primitive::Variable(var)) => {
                    if let Some(pos) = map.get(var) {
                        terms.push(pos.clone().into());
                    } else {
                        return None
                    }
                }
                _ => return None,
            }
        }

        match literal {
            Literal::Operation(op) => Some(Literal::Operation(Operation::new(op.operation_kind(), terms))),
            Literal::Positive(atom) => Some(Literal::Positive(Atom::new(atom.predicate(), terms))),
            _ => None,
        }
    }

    // replace all positional marker with variables according to map
    // returns None if a term is encountered that cannot be mapped
    fn unmark(literal: &Literal, map: &HashMap<PositionalMarker,Variable>) -> Option<Literal> {
        let mut terms: Vec<Term> = Vec::new();
        for term in literal.terms() {
            match term {
                Term::Primitive(Primitive::Ground(_)) => terms.push(term.clone()),
                Term::Primitive(Primitive::Variable(Variable::Positional(marker))) => {
                    if let Some(var) = map.get(marker) {
                        terms.push(var.clone().into());
                    } else {
                        return None
                    }
                }
                _ => return None,
            }
        }

        match literal {
            Literal::Operation(op) => Some(Literal::Operation(Operation::new(op.operation_kind(), terms))),
            Literal::Positive(atom) => Some(Literal::Positive(Atom::new(atom.predicate(), terms))),
            _ => None,
        }
    }
}

impl ProgramTransformation for TransformationFilterPushing {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        // apply normalization: no repeated variables in body

        let mut commit = program.fork();
        let _filter_expressions = Self::compute_filters(program);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    // TODO: update rule
                    commit.keep(rule);
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

impl Display for FilterExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Top => {
                f.write_str("top")
            },
            Self::Bot => {
                f.write_str("bot")
            },
            Self::Conjunction(conj) => {
                for (i,literal) in conj.iter().enumerate() {
                    write!(f, "{literal}")?;

                    if i < conj.len() - 1 {
                        f.write_str(", ")?;
                    }
                }

                f.write_str("")
            }
        }
    }
}

