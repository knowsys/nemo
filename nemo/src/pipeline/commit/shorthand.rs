//! Common declared rewrites, each made of a few rewrite declarations.

use std::collections::HashSet;

use crate::{
    model::{
        Atom, Identifier, ImportDirective, Literal, Rule, Statement, Substitution, Term, TermKind,
        Variable, VariableKind, Walk,
    },
    pipeline::{Commit, LiteralPath, StatementId, grouped_paths, provenance::SubstitutionRecord},
};

impl Commit<'_> {
    /// Folds literals of a rule into a rule of their own.
    ///
    /// `name(x̄) :- literals .` defines the new predicate,
    /// with `x̄` the literals' universal variables that the rest of the rule uses.
    /// The rule uses `name(x̄)` at the position of the first literal.
    /// Returns the defining rule and the rule that replaces the input.
    ///
    /// # Panics
    ///
    /// Panics if `rule` is not a live rule the stage may rewrite, if a
    /// position names no literal of it, or if the defining rule would
    /// read a variable none of its positive literals binds.
    pub fn fold(
        &mut self,
        rule: StatementId,
        literals: &[usize],
        name: Identifier,
    ) -> (StatementId, StatementId) {
        let (input, folded) = self.literals_of(rule, literals);
        let kept: Vec<&Literal> = input
            .body
            .iter()
            .enumerate()
            .filter(|(position, _)| !folded.contains(position))
            .map(|(_, literal)| literal)
            .collect();
        let mut elsewhere: HashSet<&Variable> =
            input.head.iter().flat_map(|atom| universal(atom)).collect();
        for literal in &kept {
            elsewhere.extend(universal(*literal));
        }
        let mut frontier: Vec<Variable> = Vec::new();
        for &position in &folded {
            for variable in universal(&input.body[position]) {
                if elsewhere.contains(variable) && !frontier.contains(variable) {
                    frontier.push(variable.clone());
                }
            }
        }
        let atom = Atom::new(name, frontier.into_iter().map(Term::variable));
        let definition = Rule::new(
            vec![atom.clone()],
            folded
                .iter()
                .map(|&position| input.body[position].clone())
                .collect(),
        );
        self.expect_safe(&definition);
        let (rule_body, literals) = self.body_with(&input, &folded, Literal::Positive(atom));
        let rewritten = Rule {
            body: rule_body,
            ..input.clone()
        };
        let defining = self.define(definition, &[rule]);
        let replacing = self.replace_rule(rule, rewritten, literals);
        (defining, replacing)
    }

    /// Replaces literals of a rule by an atom over a new import that computes them.
    ///
    /// The import's view is the rule `atom :- literals .`,
    /// and the rule uses the atom at the position of the first literal.
    /// A single negated literal `~a` is extracted as an import of `a`, used negated.
    /// `sources` are further statements the import was composed from.
    /// Returns the import and the rule that replaces the input.
    ///
    /// # Panics
    ///
    /// Panics if `rule` is not a live rule the stage may rewrite, if a
    /// position names no literal of it, if several literals are all
    /// negated, or for the reasons [define_import](Self::define_import) lists.
    pub fn extract(
        &mut self,
        rule: StatementId,
        literals: &[usize],
        import: ImportDirective,
        sources: &[StatementId],
    ) -> (StatementId, StatementId) {
        let (input, extracted) = self.literals_of(rule, literals);
        let atom = Atom::new(
            import.predicate.clone(),
            import
                .variables
                .iter()
                .map(|name| Term::variable(Variable::universal(name))),
        );
        let negated = extracted
            .iter()
            .all(|&position| matches!(input.body[position], Literal::Negative(_)));
        let (view_body, read) = if negated {
            let [position] = extracted.as_slice() else {
                panic!(
                    "the `{}` stage extracted several negated literals of `{input}`, but only \
                     one negated literal can be extracted",
                    self.stage
                );
            };
            let Literal::Negative(negated) = &input.body[*position] else {
                unreachable!("the literal is negated")
            };
            (
                vec![Literal::Positive(negated.clone())],
                Literal::Negative(atom.clone()),
            )
        } else {
            (
                extracted
                    .iter()
                    .map(|&position| input.body[position].clone())
                    .collect(),
                Literal::Positive(atom.clone()),
            )
        };
        let view = Rule::new(vec![atom], view_body);
        let (body, literals) = self.body_with(&input, &extracted, read);
        let rewritten = Rule {
            body,
            ..input.clone()
        };
        let mut all = vec![rule];
        all.extend(sources.iter().copied().filter(|&source| source != rule));
        let defining = self.define_import(import, view, &all);
        let replacing = self.replace_rule(rule, rewritten, literals);
        (defining, replacing)
    }

    /// Replaces a rule, in place, by an import that computes the rule's head from its whole body.
    ///
    /// The import's predicate is the head's,
    /// and its view is the rule (see [define_import](Self::define_import)).
    /// `sources` are further statements the import was composed from.
    ///
    /// # Panics
    ///
    /// Panics if `rule` is not a live rule the stage may rewrite, or if
    /// its head is not the import's atom of distinct universal
    /// variables.
    pub fn extract_rule(
        &mut self,
        rule: StatementId,
        import: ImportDirective,
        sources: &[StatementId],
    ) -> StatementId {
        self.replace_by_import(rule, import, sources)
    }

    /// Adds a filter rule that copies the given literals of `rule` (the rule keeps them)
    /// and returns its head atom, for an import's `where` clause.
    ///
    /// The filter rule is `name(variables) :- literals .`.
    ///
    /// # Panics
    ///
    /// Panics if `rule` is not a live rule the stage may rewrite, if a
    /// position names no literal of it, or for the reasons [define_filter](Self::define_filter)
    /// lists.
    pub fn define_filter_from(
        &mut self,
        rule: StatementId,
        literals: &[usize],
        variables: &[String],
        name: Identifier,
    ) -> Atom {
        let (input, copied) = self.literals_of(rule, literals);
        let atom = Atom::new(
            name,
            variables
                .iter()
                .map(|name| Term::variable(Variable::universal(name))),
        );
        let filter = Rule::new(
            vec![atom.clone()],
            copied
                .iter()
                .map(|&position| input.body[position].clone())
                .collect(),
        );
        self.define_filter(filter, &[rule]);
        atom
    }

    /// Replaces a rule by its instance under a substitution, recording
    /// each replaced occurrence for diagnostics.
    ///
    /// # Panics
    ///
    /// Panics if `rule` is not a live rule the stage may rewrite.
    pub fn substitute(&mut self, rule: StatementId, substitution: &Substitution) -> StatementId {
        self.expect_rule(rule);
        let Statement::Rule(mut rewritten) = self.statement(rule).clone() else {
            unreachable!("the statement is a rule")
        };
        let mut substituted = Vec::new();
        rewritten.walk(&mut |term| {
            if let TermKind::Variable(variable) = &term.kind
                && let Some(replacement) = substitution.get(variable)
            {
                substituted.push(SubstitutionRecord {
                    variable: variable.clone(),
                    occurrence: term.span,
                    definition: replacement.span,
                });
            }
        });
        substitution.apply(&mut rewritten);
        let literals = (0..rewritten.body.len())
            .map(|position| vec![position])
            .collect();
        self.replace_rule_substituted(rule, rewritten, literals, substituted)
    }

    /// Returns a rule the stage may rewrite with the given positions of
    /// its body, sorted and without repeats.
    fn literals_of(&self, rule: StatementId, literals: &[usize]) -> (Rule, Vec<usize>) {
        self.expect_rule(rule);
        let Statement::Rule(input) = self.statement(rule).clone() else {
            unreachable!("the statement is a rule")
        };
        let mut positions = literals.to_vec();
        positions.sort_unstable();
        positions.dedup();
        assert!(
            !positions.is_empty()
                && positions
                    .iter()
                    .all(|&position| position < input.body.len()),
            "the `{}` stage named literals {literals:?} of `{input}`",
            self.stage
        );
        (input, positions)
    }

    /// Returns a rule's body with the literals at the given positions
    /// replaced by one literal at the position of the first of them,
    /// and the path of each literal of the rule.
    fn body_with(
        &self,
        rule: &Rule,
        positions: &[usize],
        replacement: Literal,
    ) -> (Vec<Literal>, Vec<LiteralPath>) {
        let mut body = Vec::with_capacity(rule.body.len() + 1 - positions.len());
        let mut replacement = Some(replacement);
        for (position, literal) in rule.body.iter().enumerate() {
            if positions.contains(&position) {
                if let Some(replacement) = replacement.take() {
                    body.push(replacement);
                }
            } else {
                body.push(literal.clone());
            }
        }
        (body, grouped_paths(rule.body.len(), &[positions]))
    }

    /// Panics, naming the stage, if a defining rule's head has a
    /// variable that only negated literals of its body mention.
    fn expect_safe(&self, rule: &Rule) {
        let mut bound: HashSet<&Variable> = HashSet::new();
        for literal in &rule.body {
            if !matches!(literal, Literal::Negative(_)) {
                bound.extend(universal(literal));
            }
        }
        let head: HashSet<&Variable> = rule.head.iter().flat_map(|atom| universal(atom)).collect();
        assert!(
            head.is_subset(&bound),
            "the `{}` stage would fold into `{rule}`, whose head has a variable only \
             negated literals mention",
            self.stage
        );
    }
}

/// Returns the universal variables a component mentions.
fn universal(component: &impl Walk) -> HashSet<&Variable> {
    component
        .variables()
        .into_iter()
        .filter(|variable| variable.kind == VariableKind::Universal)
        .collect()
}
