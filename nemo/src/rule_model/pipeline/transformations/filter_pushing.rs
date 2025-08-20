
//! This module defines [TransformationFilterPushing].

use std::{collections::{HashMap, HashSet, VecDeque}, fmt::Display};

use nemo_physical::datavalues::DataValue;

use crate::rule_model::{
    components::{atom::Atom, literal::Literal, rule::Rule, statement::Statement, tag::Tag, term::{operation::{operation_kind::OperationKind, Operation}, primitive::{ground::GroundTerm, variable::{positional::PositionalMarker, Variable}, Primitive}, Term}},
    error::ValidationReport,
    programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
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

    // initialize filters, i.e., set filters for output predicates to true and for all remaining
    // idb predicates to false
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

    // compute the filters iteratively, until nothing changes any more
    fn compute_filters(program: &ProgramHandle, ground_terms: &HashSet<GroundTerm>) -> HashMap<Tag,FilterExpression> {
        let mut filter_expressions = Self::init_filters(program);
        let mut queue: Vec<Tag> = program.exports().map(|export|export.predicate().clone()).collect();

        while let Some(predicate) = queue.pop() {
            for (tag, flt) in filter_expressions.iter() {
                log::info!("{tag}: {flt}");
            }

            for rule in program.rules() {
                for head in rule.head() {
                    if head.predicate().eq(&predicate) {
                        for changed_predicate in Self::update_filters(rule, head, &mut filter_expressions, ground_terms) {
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
    fn update_filters(
        rule: &Rule,
        head: &Atom,
        filter_expressions: &mut HashMap<Tag,FilterExpression>,
        ground_terms: &HashSet<GroundTerm>
    ) -> HashSet<Tag> {
        let mut updated_predicates: HashSet<Tag> = HashSet::new();

        for literal in rule.body() {
            match literal {
                Literal::Positive(atom) | Literal::Negative(atom) => {
                    if filter_expressions.contains_key(&atom.predicate()) &&
                    Self::push_filter_for_atom(rule, head, atom, filter_expressions, ground_terms) {
                        updated_predicates.insert(atom.predicate());
                    }
                },
                _ => {},
            }
        }

        updated_predicates
    }

    // push the filter atoms to the atom
    fn push_filter(
        filter_atoms: &HashSet<Literal>,
        to: &Atom,
        filter_expressions: &mut HashMap<Tag,FilterExpression>,
        ground_terms: &HashSet<GroundTerm>
    ) -> bool {
        // get new filter expression, using positional markers
        let new_flt = match filter_expressions.get(&to.predicate()) {
            // if old filter is top, nothing to be done
            Some(FilterExpression::Top) => {
                None
            },

            // if old filter is bottom, use the complete closure
            Some(FilterExpression::Bot) => {
                if let Some(closure) = Self::closure(filter_atoms, ground_terms) {
                    let markings = Self::get_markings(to);
                    let mut to_filter: HashSet<Literal> = HashSet::new();
                    for literal in closure {
                        if let Some(literal) = Self::mark(&literal, &markings) {
                            to_filter.insert(literal);
                        }
                    }
                    if to_filter.is_empty() {
                        Some(FilterExpression::Top)
                    } else {
                        Some(FilterExpression::Conjunction(to_filter))
                    }
                } else {
                    None
                }
            },

            // if old filter is conjunction, keep those literals that are entailed by closure
            Some(FilterExpression::Conjunction(old_flt)) => {
                if let Some(closure) = Self::closure(filter_atoms, ground_terms) {
                    let unmarkings = Self::get_unmarkings(to);
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
                } else {
                    None
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
    }

    // push filter expression for an atom
    fn push_filter_for_atom(
        rule: &Rule,
        from: &Atom,
        to: &Atom,
        filter_expressions: &mut HashMap<Tag,FilterExpression>,
        ground_terms: &HashSet<GroundTerm>
    ) -> bool {
        // get the filter expression to be pushed
        if let Some(from_filter) = filter_expressions.get(&from.predicate()) {
            // construct filter of the rule body filter and the instantiated from filter expression
            // initialize filter with instantiated filter expression
            let unmarkings = Self::get_unmarkings(from);
            let mut rule_filter: HashSet<Literal> = match from_filter {
                FilterExpression::Top => HashSet::new(),
                FilterExpression::Bot => return false,
                FilterExpression::Conjunction(flt) => flt.iter().filter_map(|literal|Self::unmark(literal, &unmarkings)).collect(),
            };

            // add filter atom in the rule body
            for literal in Self::get_filter_literals(rule, filter_expressions) {
                rule_filter.insert(literal.clone());
            }

            // push the constructed filter atoms
            Self::push_filter(&rule_filter, to, filter_expressions, ground_terms)
        } else {
            false
        }
    }

    // add the terms to hash map entry for key, if not present yet
    fn insert(kind: &OperationKind, terms: Vec<Term>, map: &mut HashMap<OperationKind,Vec<Vec<Term>>>) {
        if let Some(set) = map.get_mut(kind) {
            if !set.contains(&terms) {
                set.push(terms);
            }
        } else {
            map.insert(*kind, vec![terms]);
        }
    }

    // compute the logical closure of the given literals
    fn closure(literals: &HashSet<Literal>, ground_terms: &HashSet<GroundTerm>) -> Option<Vec<Literal>> {
        let mut queue: VecDeque<Literal> = VecDeque::new();
        let mut ops: HashMap<OperationKind,Vec<Vec<Term>>> = HashMap::new();
        let mut out: Vec<Literal> = Vec::new();

        for literal in literals {
            if let Literal::Operation(op) = literal {
                match op.operation_kind() {
                    OperationKind::NumericLessthaneq => {
                        let terms: Vec<Term> = literal.terms().cloned().collect();
                        Self::insert(&OperationKind::NumericLessthaneq, terms, &mut ops);
                        if !queue.contains(literal) {
                            queue.push_back(literal.clone());
                        }
                    },
                    OperationKind::NumericLessthan => {
                        let terms: Vec<Term> = literal.terms().cloned().collect();
                        Self::insert(&OperationKind::NumericLessthan, terms, &mut ops);
                        if !queue.contains(literal) {
                            queue.push_back(literal.clone());
                        }
                    },
                    OperationKind::Equal => {
                        queue.push_back(literal.clone());
                        let terms: Vec<Term> = literal.terms().cloned().collect();
                        if let Term::Operation(op) = &terms[1] {
                            if op.operation_kind().eq(&OperationKind::NumericSum) {
                                let op_terms: Vec<Term> = op.terms().cloned().collect();
                                if let Term::Primitive(Primitive::Ground(val)) = &op_terms[0] {
                                    if val.value().is_positive_number() {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, vec![op_terms[1].clone(), terms[0].clone()])));
                                    }
                                }
                                if let Term::Primitive(Primitive::Ground(val)) = &op_terms[1] {
                                    if val.value().is_positive_number() {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, vec![op_terms[0].clone(), terms[0].clone()])));
                                    }
                                }
                            }
                        }

                    }
                    _ => {
                        out.push(literal.clone());
                    },
                }
            } else {
                out.push(literal.clone());
            }
        }

        while let Some(literal) = queue.pop_front() {
            if !out.contains(&literal) {
                if let Literal::Operation(op) = literal {
                    if op.is_resolvable() {
                        if let Some(Term::Primitive(Primitive::Ground(ground))) = op.reduce() {
                            if let Some(false) = ground.value().to_boolean() {
                                return None;
                            }
                        }
                    } else {
                        out.push(Literal::Operation(op.clone()));

                        let terms: Vec<Term> = op.terms().cloned().collect();
                        Self::insert(&op.operation_kind(), terms.clone(), &mut ops);
                        match op.operation_kind() {
                            OperationKind::NumericLessthan => {
                                queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthaneq, terms)));

                                if let Some(qs) = ops.get(&OperationKind::NumericLessthaneq) {
                                    for res in Self::transitivity_left(op.terms().cloned().collect(), qs) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, res)));
                                    }
                                }
                                if let Some(ps) = ops.get(&OperationKind::NumericLessthan) {
                                    for res in Self::transitivity_right(op.terms().cloned().collect(), ps) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, res)));
                                    }
                                }
                                if let Some(ps) = ops.get(&OperationKind::NumericLessthaneq) {
                                    for res in Self::transitivity_right(op.terms().cloned().collect(), ps) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, res)));
                                    }
                                }
                                if let Some(ps) = ops.get(&OperationKind::NumericLessthan) {
                                    for res in Self::transitivity_right(op.terms().cloned().collect(), ps) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthan, res)));
                                    }
                                }
                            },
                            OperationKind::NumericLessthaneq => {
                                if let Some(qs) = ops.get(&OperationKind::NumericLessthaneq) {
                                    for res in Self::symmetry(terms.clone(), qs) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::Equal, res)));
                                    }
                                }
                                if let Some(qs) = ops.get(&OperationKind::NumericLessthaneq) {
                                    for res in Self::transitivity_left(terms.clone(), qs) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthaneq, res)));
                                    }
                                }
                                if let Some(ps) = ops.get(&OperationKind::NumericLessthaneq) {
                                    for res in Self::transitivity_right(op.terms().cloned().collect(), ps) {
                                        queue.push_back(Literal::Operation(Operation::new(OperationKind::NumericLessthaneq, res)));
                                    }
                                }
                            },
                            OperationKind::Equal => {
                                if let Term::Primitive(Primitive::Ground(first)) = terms[0].clone() {
                                    for res in Self::apply_equality_for_ground_terms(&first, &terms[1], &ground_terms) {
                                        queue.push_back(res);
                                    }
                                }
                                if let Term::Primitive(Primitive::Ground(second)) = terms[1].clone() {
                                    for res in Self::apply_equality_for_ground_terms(&second, &terms[0], &ground_terms) {
                                        queue.push_back(res);
                                    }
                                }
                            }
                            _ => {},
                        }
                    }
                } else {
                    out.push(literal.clone());
                }
            }
        }

        Some(out)
    }

    // collect literals that follow from ground = term and the natural inequalities for ground
    // w.r.t. ground_terms
    fn apply_equality_for_ground_terms(ground: &GroundTerm, term: &Term, ground_terms: &HashSet<GroundTerm>) -> Vec<Literal> {
        let mut literals: Vec<Literal> = Vec::new();
        for con in ground_terms {
            if con.value().lt(&ground.value()) {
                literals.push(Literal::Operation(Operation::new(OperationKind::NumericLessthan,
                    vec![con.clone().into(), term.clone()])));
            }
            if ground.value().lt(&con.value()) {
                literals.push(Literal::Operation(Operation::new(OperationKind::NumericLessthan,
                    vec![term.clone(), con.clone().into()])));
            }
        }
        literals
    }

    // get filter literal for a rule
    fn get_filter_literals<'a>(rule: &'a Rule, filter_expressions: &'a HashMap<Tag,FilterExpression>) -> Vec<&'a Literal>  {
        rule.body().iter().filter(|literal|Self::is_filter_literal(literal, filter_expressions)).collect()
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
    fn unmark(literal: &Literal, map: &HashMap<PositionalMarker,Term>) -> Option<Literal> {
        let mut terms: Vec<Term> = Vec::new();
        for term in literal.terms() {
            if let Term::Primitive(Primitive::Variable(Variable::Positional(marker))) = term {
                if let Some(var) = map.get(marker) {
                    terms.push(var.clone());
                } else {
                    return None
                }
            } else {
                terms.push(term.clone());
            }
        }

        match literal {
            Literal::Operation(op) => Some(Literal::Operation(Operation::new(op.operation_kind(), terms))),
            Literal::Positive(atom) => Some(Literal::Positive(Atom::new(atom.predicate(), terms))),
            _ => None,
        }
    }

    // get a mapping that maps the variables in atom to their positional markers
    fn get_markings(atom: &Atom) -> HashMap<Variable,PositionalMarker> {
        let mut markings: HashMap<Variable,PositionalMarker> = HashMap::new();
        for (i,term) in atom.terms().enumerate() {
            if let Term::Primitive(Primitive::Variable(var)) = term {
                let position = PositionalMarker::new(i);
                markings.insert(var.clone(), position.clone());
            }
        }
        markings
    }

    // get a mapping that maps the positional markers to their corresponding terms in atom
    fn get_unmarkings(atom: &Atom) -> HashMap<PositionalMarker,Term> {
        let mut unmarkings: HashMap<PositionalMarker,Term> = HashMap::new();
        for (i,term) in atom.terms().enumerate() {
            if let Term::Primitive(_) = term {
                let position = PositionalMarker::new(i);
                unmarkings.insert(position, term.clone());
            }
        }
        unmarkings
    }

    // get instantiated filter expression for the head atom and the filter literals in rule body
    fn get_f_plus(rule: &Rule, atom: &Atom, filter_expressions: &HashMap<Tag,FilterExpression>) -> Option<Vec<Literal>> {
        let mut f: Vec<Literal> = Vec::new();

        let unmarkings = Self::get_unmarkings(atom);
        match filter_expressions.get(&atom.predicate()) {
            None | Some(FilterExpression::Bot) => return None,
            Some(FilterExpression::Top) => {},
            Some(FilterExpression::Conjunction(conj)) => {
                for flt in conj {
                    if let Some(flt_lit) = Self::unmark(flt, &unmarkings) {
                        f.push(flt_lit);
                    }
                }
            }
        }

        for literal in Self::get_filter_literals(rule, filter_expressions) {
            f.push(literal.clone());
        }

        Some(f)
    }

    // get conjunction of instantiated filter expression for idb body atoms
    fn get_f_minus(rule: &Rule, filter_expressions: &HashMap<Tag,FilterExpression>) -> Vec<Literal> {
        let mut f: Vec<Literal> = Vec::new();

        for literal in rule.body() {
            if let Literal::Positive(atom) = literal {
                if let Some(FilterExpression::Conjunction(conj)) = filter_expressions.get(&atom.predicate()) {
                    let unmarkings = Self::get_unmarkings(atom);
                    for flt in conj {
                        f.push(Self::unmark(flt, &unmarkings).expect("All positional markers should be mapped to a term by construction."));
                    }
                }
            }
        }

        f
    }

    // check whether a literal is a filter literal, i.e., it is an operation or a positive atom
    // over a filter predicate
    fn is_filter_literal(literal: &Literal, filter_expressions: &HashMap<Tag,FilterExpression>) -> bool {
        match literal {
            Literal::Operation(_) => true,
            Literal::Positive(atom) => !filter_expressions.contains_key(&atom.predicate()),
            Literal::Negative(_) => false,
        }
    }

    // simplify filter expression f_plus by iteratively replacing atoms with false and check if it
    // is still entailed by remainder and f_minus
    fn simplify(f_plus: Vec<Literal>, f_minus: Vec<Literal>, ground_terms: &HashSet<GroundTerm>) -> Option<Vec<Literal>> {
        let mut simpl: Vec<Literal> = Vec::new();
        let mut f_plus: Vec<Literal> = f_plus.clone();

        while let Some(literal) = f_plus.pop() {
            let mut comb = HashSet::new();
            comb.extend(f_minus.clone());
            comb.extend(simpl.clone());
            comb.extend(f_plus.clone());
            if let Some(closure) = Self::closure(&comb, ground_terms) {
                if !closure.contains(&literal) {
                    simpl.push(literal.clone());
                }
            } else {
                return None;
            }
        }

        Some(simpl)
    }

    // p(x,y), q(y,z) => _(x,z)
    fn transitivity_right(q: Vec<Term>, ps: &[Vec<Term>]) -> Vec<Vec<Term>> {
        let mut result: Vec<Vec<Term>> = Vec::new();
        for p in ps.iter() {
            if p[1].eq(&q[0]) {
                result.push(vec![p[0].clone(), q[1].clone()]);
            }
        }
        result
    }

    // p(x,y), q(y,z) => _(x,z)
    fn transitivity_left(p: Vec<Term>, qs: &[Vec<Term>]) -> Vec<Vec<Term>> {
        let mut result: Vec<Vec<Term>> = Vec::new();
        for q in qs.iter() {
            if p[1].eq(&q[0]) {
                result.push(vec![p[0].clone(), q[1].clone()]);
            }
        }
        result
    }


    // p(x,y), p(y,x) => x = y
    fn symmetry(p: Vec<Term>, qs: &[Vec<Term>]) -> Vec<Vec<Term>> {
        let mut result: Vec<Vec<Term>> = Vec::new();
        for q in qs.iter() {
            if p[1].eq(&q[0]) && p[0].eq(&q[1]) {
                result.push(vec![p[0].clone(), p[1].clone()]);
            }
        }
        result
    }

    // collect ground terms in program
    fn get_ground_terms(program: &ProgramHandle) -> HashSet<GroundTerm> {
        program.rules().flat_map(|rule|{
            let mut terms: HashSet<GroundTerm> = HashSet::new();
            for atom in rule.head() {
                for term in atom.terms() {
                    if let Term::Primitive(Primitive::Ground(ground)) = term {
                        terms.insert(ground.clone());
                    }
                }
            }
            for literal in rule.body() {
                for term in literal.terms() {
                    if let Term::Primitive(Primitive::Ground(ground)) = term {
                        terms.insert(ground.clone());
                    }
                }
            }
            terms
        }).collect()
    }
}

impl ProgramTransformation for TransformationFilterPushing {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        // apply normalization: no repeated variables in body

        let mut commit = program.fork();
        let ground_terms = Self::get_ground_terms(program);
        let filter_expressions = Self::compute_filters(program, &ground_terms);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    for atom in rule.head() {
                        if let Some(f_plus) = Self::get_f_plus(rule, atom, &filter_expressions) {
                            let head = vec![atom.clone()];
                            let mut body: Vec<Literal> = Vec::new();
                            for literal in rule.body() {
                                if !Self::is_filter_literal(literal, &filter_expressions){
                                    body.push(literal.clone());
                                }
                            }

                            let f_minus = Self::get_f_minus(rule, &filter_expressions);
                            if let Some(simplification) = Self::simplify(f_plus, f_minus, &ground_terms) {
                                for literal in simplification {
                                    body.push(literal);
                                }

                                commit.add_rule(Rule::new(head, body));
                            }
                        }
                    }
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

