//! The correspondence of a declared rewrite: the path of each literal of an input rule
//! in the rule that replaces it, and what each of its variables became.
//!
//! A stage gives the path of each literal as a [LiteralPath]. Following
//! a path *unfolds* the predicates the commit defined along it: a
//! literal over a defined predicate gives way to the body of its
//! definition, whose head variables take the literal's terms and whose
//! other variables are renamed apart as `name~path`, the path saying
//! where the definition was unfolded. A negated literal unfolds only
//! if its definition is one atom.
//!
//! *Matching* each input literal against the literal at its path, and
//! the input head against the new one, binds the input rule's variables:
//! the assignment the [Correspondence] records. A literal of the new rule
//! that no path leads to is a guard, which only restricts the rule's matches.

mod mismatch;

use std::collections::HashMap;

use crate::{
    model::{
        Aggregate, Atom, Identifier, Literal, Rule, Substitution, Term, TermKind, Variable,
        VariableKind, Walk,
    },
    pipeline::{NameScope, matching::mismatch::CorrespondenceMismatch},
};

/// The location of an input rule's literal in the rule that replaces it.
///
/// The path is the literal's index in the new rule's body, followed,
/// for each predicate defined by the commit that the literal was moved into,
/// by its index in the body of that predicate's defining rule.
/// `[1, 0]` is the first body literal of the definition of the predicate
/// that the new rule's second body literal uses.
pub type LiteralPath = Vec<usize>;

/// How an input rule reappears in the rule that replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Correspondence {
    /// Per literal of the input rule, its path.
    pub(crate) literals: Vec<LiteralPath>,
    /// The term each variable of the input rule became, over the
    /// unfolded rule's variables.
    pub(crate) assignment: Vec<(Variable, Term)>,
}

/// The variables of the input rule bound so far.
type Bindings = HashMap<Variable, Term>;

/// The definitions instantiated so far, by the path of the literal
/// they were instantiated at.
type Unfolded = HashMap<LiteralPath, Vec<Literal>>;

/// Computes the correspondences of one commit's replacing rules,
/// unfolding the predicates the commit defined.
pub(crate) struct CorrespondenceMatcher<'a> {
    /// The rules defining the predicates the commit introduced, by the resolved name
    /// of the predicate defined: a defining rule, or an import's view.
    /// Each head is one atom of distinct universal variables.
    pub(crate) definitions: &'a HashMap<String, Rule>,
    /// The scope names resolve in.
    pub(crate) scope: &'a NameScope,
}

impl CorrespondenceMatcher<'_> {
    /// Returns how the input rule reappears in the new one, given the path
    /// of each of its literals, or why it does not.
    pub(crate) fn correspond(
        &self,
        input: &Rule,
        new: &Rule,
        literals: &[LiteralPath],
    ) -> Result<Correspondence, Box<CorrespondenceMismatch>> {
        let mut bindings = Bindings::new();
        if input.head.len() != new.head.len() {
            return Err(Box::new(CorrespondenceMismatch::HeadCount {
                input: input.head.len(),
                new: new.head.len(),
            }));
        }
        for (pattern, atom) in input.head.iter().zip(&new.head) {
            if !self.atom_matches(pattern, atom, &mut bindings) {
                return Err(Box::new(CorrespondenceMismatch::HeadAtom {
                    pattern: pattern.clone(),
                    atom: atom.clone(),
                }));
            }
        }
        if literals.len() != input.body.len() {
            return Err(Box::new(CorrespondenceMismatch::PathCount {
                input: input.body.len(),
                paths: literals.len(),
            }));
        }
        let mut unfolded = Unfolded::new();
        for (pattern, path) in input.body.iter().zip(literals) {
            let literal = self.literal_at(new, path, &mut unfolded)?;
            if !self.literal_matches(pattern, &literal, &mut bindings) {
                return Err(Box::new(CorrespondenceMismatch::PathLiteral {
                    pattern: pattern.clone(),
                    literal,
                    path: path.clone(),
                }));
            }
        }
        let mut assignment: Vec<(Variable, Term)> = bindings.into_iter().collect();
        assignment.sort_by(|(left, _), (right, _)| left.name.cmp(&right.name));
        Ok(Correspondence {
            literals: literals.to_vec(),
            assignment,
        })
    }

    /// Returns the literal a path leads to in a rule.
    ///
    /// The first step picks a literal of the rule's body.
    /// Every further step picks a literal of the body of the definition
    /// of the predicate the literal so far uses, instantiated at that literal.
    /// Each definition is instantiated once per path prefix,
    /// however many input literals were moved into it.
    fn literal_at(
        &self,
        rule: &Rule,
        path: &[usize],
        unfolded: &mut Unfolded,
    ) -> Result<Literal, Box<CorrespondenceMismatch>> {
        let Some((&first, rest)) = path.split_first() else {
            return Err(Box::new(CorrespondenceMismatch::EmptyPath));
        };
        let mut literal =
            rule.body.get(first).cloned().ok_or_else(|| {
                Box::new(CorrespondenceMismatch::NoBodyLiteral { position: first })
            })?;
        for (depth, &step) in rest.iter().enumerate() {
            let (atom, negated) = match &literal {
                Literal::Positive(atom) => (atom, false),
                Literal::Negative(atom) => (atom, true),
                Literal::Operation(_) => {
                    return Err(Box::new(CorrespondenceMismatch::NotUnfoldable {
                        literal: literal.clone(),
                    }));
                }
            };
            let prefix = &path[..=depth];
            if !unfolded.contains_key(prefix) {
                let definition = self
                    .scope
                    .resolve(&atom.predicate, None)
                    .ok()
                    .and_then(|name| self.definitions.get(&name))
                    .ok_or_else(|| {
                        Box::new(CorrespondenceMismatch::NotDefined { atom: atom.clone() })
                    })?;
                let body = instantiate(definition, atom, prefix)?;
                unfolded.insert(prefix.to_vec(), body);
            }
            let body = &unfolded[prefix];
            literal = match (negated, body.as_slice()) {
                (true, [Literal::Positive(inner)]) if step == 0 => Literal::Negative(inner.clone()),
                (true, _) => {
                    return Err(Box::new(CorrespondenceMismatch::NegatedDefinition {
                        literal: literal.clone(),
                    }));
                }
                (false, body) => body.get(step).cloned().ok_or_else(|| {
                    Box::new(CorrespondenceMismatch::NoDefinitionLiteral {
                        atom: atom.clone(),
                        position: step,
                    })
                })?,
            };
        }
        Ok(literal)
    }

    /// Returns whether a literal of the input rule matches an unfolded
    /// one, extending the bindings.
    fn literal_matches(
        &self,
        pattern: &Literal,
        literal: &Literal,
        bindings: &mut Bindings,
    ) -> bool {
        match (pattern, literal) {
            (Literal::Positive(pattern), Literal::Positive(atom))
            | (Literal::Negative(pattern), Literal::Negative(atom)) => {
                self.atom_matches(pattern, atom, bindings)
            }
            (Literal::Operation(pattern), Literal::Operation(operation)) => {
                pattern.operator == operation.operator
                    && self.all_match(&pattern.arguments, &operation.arguments, bindings)
            }
            _ => false,
        }
    }

    /// Returns whether an atom of the input rule matches one of the new
    /// rule, extending the bindings.
    fn atom_matches(&self, pattern: &Atom, atom: &Atom, bindings: &mut Bindings) -> bool {
        self.same_name(&pattern.predicate, &atom.predicate)
            && pattern.labels == atom.labels
            && self.all_match(&pattern.terms, &atom.terms, bindings)
    }

    /// Returns whether two lists of terms match position by position.
    fn all_match(&self, patterns: &[Term], terms: &[Term], bindings: &mut Bindings) -> bool {
        patterns.len() == terms.len()
            && patterns
                .iter()
                .zip(terms)
                .all(|(pattern, term)| self.term_matches(pattern, term, bindings))
    }

    /// Returns whether a term of the input rule matches one of the new
    /// rule: its universal and existential variables bind, an anonymous
    /// one matches anything, and everything else must be the same.
    fn term_matches(&self, pattern: &Term, term: &Term, bindings: &mut Bindings) -> bool {
        match (&pattern.kind, &term.kind) {
            (TermKind::Variable(variable), _) => match variable.kind {
                VariableKind::Anonymous => true,
                VariableKind::Global => pattern == term,
                VariableKind::Universal | VariableKind::Existential => {
                    match bindings.get(variable) {
                        Some(bound) => bound == term,
                        None => {
                            bindings.insert(variable.clone(), term.clone());
                            true
                        }
                    }
                }
            },
            (TermKind::Constant(left), TermKind::Constant(right)) => self.same_name(left, right),
            (TermKind::Operation(left), TermKind::Operation(right)) => {
                left.operator == right.operator && self.subterms_match(pattern, term, bindings)
            }
            (
                TermKind::Function {
                    name: left,
                    labels: left_labels,
                    ..
                },
                TermKind::Function {
                    name: right,
                    labels: right_labels,
                    ..
                },
            ) => {
                self.same_name(left, right)
                    && left_labels == right_labels
                    && self.subterms_match(pattern, term, bindings)
            }
            (TermKind::Tuple(_), TermKind::Tuple(_)) => {
                self.subterms_match(pattern, term, bindings)
            }
            (TermKind::Map { label: left, .. }, TermKind::Map { label: right, .. }) => {
                let labels = match (left, right) {
                    (None, None) => true,
                    (Some(left), Some(right)) => self.same_name(left, right),
                    _ => false,
                };
                labels && self.subterms_match(pattern, term, bindings)
            }
            (TermKind::Aggregate(left), TermKind::Aggregate(right)) => {
                left.kind == right.kind
                    && left.input.is_some() == right.input.is_some()
                    && left.value.is_some() == right.value.is_some()
                    && self.scopes_match(left, right, bindings)
                    && self.subterms_match(pattern, term, bindings)
            }
            _ => pattern == term,
        }
    }

    /// Returns whether the subterms of two terms of one shape match.
    fn subterms_match(&self, pattern: &Term, term: &Term, bindings: &mut Bindings) -> bool {
        let patterns: Vec<&Term> = pattern.subterms().collect();
        let terms: Vec<&Term> = term.subterms().collect();
        patterns.len() == terms.len()
            && patterns
                .into_iter()
                .zip(terms)
                .all(|(pattern, term)| self.term_matches(pattern, term, bindings))
    }

    /// Returns whether two aggregates range over matching scopes.
    fn scopes_match(
        &self,
        pattern: &Aggregate,
        aggregate: &Aggregate,
        bindings: &mut Bindings,
    ) -> bool {
        let terms = |aggregate: &Aggregate| -> Vec<Term> {
            aggregate
                .scope
                .iter()
                .cloned()
                .map(Term::variable)
                .collect()
        };
        self.all_match(&terms(pattern), &terms(aggregate), bindings)
    }

    /// Returns whether two names stand for the same thing: spelled
    /// alike, or resolving alike.
    fn same_name(&self, left: &Identifier, right: &Identifier) -> bool {
        left == right
            || matches!(
                (self.scope.resolve(left, None), self.scope.resolve(right, None)),
                (Ok(left), Ok(right)) if left == right
            )
    }
}

/// Returns the path of each literal of a body of `len` literals when each
/// group of positions, ascending, is replaced by one literal at the
/// position of the group's first.
///
/// A literal outside the groups gets its new index.
/// A literal inside a group gets the index of the replacing literal,
/// then its index within the group.
pub(crate) fn grouped_paths(len: usize, groups: &[&[usize]]) -> Vec<LiteralPath> {
    let mut paths = vec![LiteralPath::new(); len];
    let mut next = 0;
    for position in 0..len {
        if let Some(group) = groups.iter().find(|group| group.first() == Some(&position)) {
            for (offset, &member) in group.iter().enumerate() {
                paths[member] = vec![next, offset];
            }
            next += 1;
        } else if !groups.iter().any(|group| group.contains(&position)) {
            paths[position] = vec![next];
            next += 1;
        }
    }
    paths
}

/// Returns the body of a definition instantiated at an atom: its head
/// variables replaced by the atom's terms, its other variables renamed
/// apart for the path.
fn instantiate(
    definition: &Rule,
    atom: &Atom,
    path: &[usize],
) -> Result<Vec<Literal>, Box<CorrespondenceMismatch>> {
    let [head] = definition.head.as_slice() else {
        unreachable!("a definition's head is one atom")
    };
    if head.terms.len() != atom.terms.len() {
        return Err(Box::new(CorrespondenceMismatch::DefinitionArity {
            atom: atom.clone(),
            head: head.terms.len(),
        }));
    }
    let mut substitution = Substitution::new();
    for (variable, term) in head.terms.iter().zip(&atom.terms) {
        let TermKind::Variable(variable) = &variable.kind else {
            unreachable!("a definition's head is made of variables")
        };
        substitution.insert(variable.clone(), term.clone());
    }
    let suffix: Vec<String> = path.iter().map(ToString::to_string).collect();
    let suffix = suffix.join(".");
    for literal in &definition.body {
        for variable in literal.variables() {
            if matches!(
                variable.kind,
                VariableKind::Universal | VariableKind::Existential
            ) && substitution.get(variable).is_none()
            {
                let renamed = Variable {
                    kind: variable.kind,
                    name: format!("{}~{suffix}", variable.name),
                };
                substitution.insert(variable.clone(), Term::variable(renamed));
            }
        }
    }
    let mut body = definition.body.clone();
    for literal in &mut body {
        substitution.apply(literal);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::rule;

    /// Computes the correspondence of a new rule replacing an input rule
    /// under the definitions, with the given literal paths.
    fn check(
        input: &str,
        new: &str,
        definitions: &[&str],
        literals: &[&[usize]],
    ) -> Result<Correspondence, Box<CorrespondenceMismatch>> {
        let literals: Vec<LiteralPath> = literals.iter().map(|path| path.to_vec()).collect();
        let scope = NameScope::default();
        let by_name: HashMap<String, Rule> = definitions
            .iter()
            .map(|text| {
                let definition = rule(text);
                let name = scope
                    .resolve(&definition.head[0].predicate, None)
                    .expect("the name resolves");
                (name, definition)
            })
            .collect();
        CorrespondenceMatcher {
            definitions: &by_name,
            scope: &scope,
        }
        .correspond(&rule(input), &rule(new), &literals)
    }

    #[test]
    fn an_unchanged_rule_replaces_itself() {
        let found = check(
            "p(?x) :- q(?x), r(?x) .",
            "p(?x) :- q(?x), r(?x) .",
            &[],
            &[&[0], &[1]],
        )
        .expect("the rule replaces itself");
        assert_eq!(found.literals, [vec![0], vec![1]]);
    }

    #[test]
    fn a_fold_unfolds_into_its_definition() {
        let found = check(
            "p(?x) :- q(?x), r(?x, ?y), s(?y) .",
            "p(?x) :- q(?x), part(?x) .",
            &["part(?x) :- r(?x, ?y), s(?y) ."],
            &[&[0], &[1, 0], &[1, 1]],
        )
        .expect("the fold replaces the rule");
        assert_eq!(found.literals, [vec![0], vec![1, 0], vec![1, 1]]);
        assert!(
            found
                .assignment
                .iter()
                .any(|(variable, term)| variable.name == "y" && term.to_string() == "?y~1"),
            "{found:?}"
        );
    }

    #[test]
    fn a_dropped_literal_is_found_out() {
        let problem = check(
            "p(?x) :- q(?x), r(?x, ?y), s(?y) .",
            "p(?x) :- q(?x), part(?x) .",
            &["part(?x) :- r(?x, ?y) ."],
            &[&[0], &[1, 0], &[1, 1]],
        )
        .expect_err("a literal went missing");
        assert!(
            matches!(
                *problem,
                CorrespondenceMismatch::NoDefinitionLiteral { position: 1, .. }
            ),
            "{problem}"
        );
    }

    #[test]
    fn extra_literals_act_as_filters() {
        check(
            "p(?x) :- q(?x) .",
            "p(?x) :- q(?x), magic(?x), ?x > 3 .",
            &[],
            &[&[0]],
        )
        .expect("guards are allowed");
    }

    #[test]
    fn a_substituted_variable_matches_its_term() {
        let found = check("p(?x) :- q(?x, ?y) .", "p(?x) :- q(?x, 5) .", &[], &[&[0]])
            .expect("a specialization replaces the rule");
        assert!(
            found
                .assignment
                .iter()
                .any(|(variable, term)| variable.name == "y" && *term == Term::integer(5))
        );
    }

    #[test]
    fn a_negated_definition_of_one_atom_unfolds_negated() {
        let found = check(
            "p(?x) :- q(?x), ~t(?x, ?y) .",
            "p(?x) :- q(?x), ~absent(?x) .",
            &["absent(?x) :- t(?x, ?y) ."],
            &[&[0], &[1, 0]],
        )
        .expect("the negated definition replaces the negated atom");
        assert_eq!(found.literals, [vec![0], vec![1, 0]]);
    }

    #[test]
    fn a_changed_head_is_found_out() {
        let problem = check("p(?x) :- q(?x) .", "r(?x) :- q(?x) .", &[], &[&[0]])
            .expect_err("the head changed");
        assert!(
            matches!(*problem, CorrespondenceMismatch::HeadAtom { .. }),
            "{problem}"
        );
    }

    #[test]
    fn a_literal_at_the_wrong_path_is_found_out() {
        let problem = check(
            "p(?x) :- q(?x), r(?x) .",
            "p(?x) :- q(?x), r(?x) .",
            &[],
            &[&[1], &[0]],
        )
        .expect_err("the paths are swapped");
        assert!(
            matches!(&*problem, CorrespondenceMismatch::PathLiteral { path, .. } if *path == [1]),
            "{problem}"
        );
    }

    #[test]
    fn grouped_paths_follow_the_replaced_groups() {
        assert_eq!(
            grouped_paths(5, &[&[1, 3], &[4]]),
            [vec![0], vec![1, 0], vec![2], vec![1, 1], vec![3, 0]]
        );
        assert_eq!(grouped_paths(2, &[]), [vec![0], vec![1]]);
    }
}
