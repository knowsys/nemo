//! Declared rewrites: the [Commit] methods that change rules and imports,
//! each with a rewrite declaration of how the statement it creates relates
//! to the input, and the correspondences computed from those declarations
//! when the commit finishes.

use std::collections::{HashMap, HashSet};

use crate::{
    model::{
        Atom, Directive, Identifier, ImportDirective, Rule, Statement, Term, TermKind, Variable,
        VariableKind,
    },
    pipeline::{
        Commit, LiteralPath, StatementId,
        matching::CorrespondenceMatcher,
        provenance::{RewriteDeclaration, SubstitutionRecord},
    },
};

/// A rewrite declaration made while the commit runs, recorded when it finishes.
#[derive(Debug)]
pub(super) enum PendingDeclaration {
    /// The created statement replaces an input rule.
    Replaces {
        /// The created statement.
        created: StatementId,
        /// The input rule it replaces.
        input: StatementId,
        /// An import's view, or `None` for a rule.
        view: Option<Rule>,
        /// The path of each literal of the input rule.
        literals: Vec<LiteralPath>,
        /// The occurrences a substitution replaced.
        substituted: Vec<SubstitutionRecord>,
    },
    /// The created statement defines a new predicate.
    Defines {
        /// The created statement.
        created: StatementId,
        /// The resolved name of the defined predicate.
        predicate: String,
        /// An import's view, or `None` for a rule.
        view: Option<Rule>,
    },
    /// The created rule defines a filter.
    DefinesFilter {
        /// The created statement.
        created: StatementId,
    },
}

impl PendingDeclaration {
    /// Returns the statement the rewrite declaration is about.
    fn created(&self) -> StatementId {
        match self {
            PendingDeclaration::Replaces { created, .. }
            | PendingDeclaration::Defines { created, .. }
            | PendingDeclaration::DefinesFilter { created } => *created,
        }
    }
}

impl Commit<'_> {
    /// Replaces an input rule by a rewritten rule, in place.
    ///
    /// `literals` gives the [path](LiteralPath) of each literal of the input rule, in order.
    /// Once its literals over predicates this commit defines are unfolded along those paths,
    /// the new rule contains an instance of the input rule.
    /// A literal of the new rule that no path leads to only restricts the rule's matches.
    /// Proofs over the new rule are mapped back onto the input rule through these paths,
    /// and the paths are checked only as far as that mapping needs.
    ///
    /// # Panics
    ///
    /// Panics if `input` is not a live rule the stage may rewrite.
    /// The commit panics when it finishes if the input head, or an input literal,
    /// does not match the literal at its path.
    pub fn replace_rule(
        &mut self,
        input: StatementId,
        rule: Rule,
        literals: Vec<LiteralPath>,
    ) -> StatementId {
        self.replace_rule_substituted(input, rule, literals, Vec::new())
    }

    /// Adds a rule defining a new predicate, before the first of its
    /// sources the stage may rewrite.
    ///
    /// The rule is the predicate's definition.
    /// A rule that replaces an input rule and uses the predicate
    /// gives paths into this rule's body.
    ///
    /// # Panics
    ///
    /// Panics if the head is not one atom of distinct universal
    /// variables, or if the input uses its predicate.
    pub fn define(&mut self, rule: Rule, sources: &[StatementId]) -> StatementId {
        let predicate = self.new_definition(&rule);
        let created = self.insert(sources, Statement::Rule(rule));
        self.pending.push(PendingDeclaration::Defines {
            created,
            predicate,
            view: None,
        });
        created
    }

    /// Adds an import defining a new predicate,
    /// before the first of its sources the stage may rewrite.
    ///
    /// The view is a rule over input predicates whose head is the import's atom,
    /// and it defines the import's rows.
    /// The import's atom is its predicate over its column variables.
    /// An import that names no columns (as file imports do)
    /// takes the variables of the view's head as its columns.
    ///
    /// # Panics
    ///
    /// Panics if the view's head is not the import's atom of distinct universal variables,
    /// or if the input uses the predicate.
    pub fn define_import(
        &mut self,
        import: ImportDirective,
        view: Rule,
        sources: &[StatementId],
    ) -> StatementId {
        self.expect_view(&import, &view);
        let predicate = self.new_definition(&view);
        let created = self.insert(sources, Statement::Directive(Directive::Import(import)));
        self.pending.push(PendingDeclaration::Defines {
            created,
            predicate,
            view: Some(view),
        });
        created
    }

    /// Adds a filter rule, before the first of its sources the stage may rewrite.
    ///
    /// The rule defines a new predicate that other statements use only to restrict their matches,
    /// in an import's `where` clause or as an extra body literal.
    /// Proofs never expand it.
    ///
    /// # Panics
    ///
    /// Panics if the head is not one atom, or if the input uses its predicate.
    pub fn define_filter(&mut self, rule: Rule, sources: &[StatementId]) -> StatementId {
        let [head] = rule.head.as_slice() else {
            panic!(
                "the `{}` stage defined the filter `{rule}`, whose head is not one atom",
                self.stage
            );
        };
        self.expect_new(&head.predicate);
        let created = self.insert(sources, Statement::Rule(rule));
        self.pending
            .push(PendingDeclaration::DefinesFilter { created });
        created
    }

    /// Removes an import whose rows no rule reads any more,
    /// because the stage's rules query its endpoint directly.
    ///
    /// Its predicate stays in the program, without a stored table.
    ///
    /// # Panics
    ///
    /// Panics if `import` is not a live import the stage may rewrite,
    /// or if its predicate does not resolve.
    /// The commit panics when it finishes if a statement other than
    /// a `@declare` directive still names the predicate.
    pub fn remove_import(&mut self, import: StatementId) {
        self.position(import);
        let Statement::Directive(Directive::Import(directive)) = self.pipeline.statement(import)
        else {
            panic!(
                "the `{}` stage removed `{}` as an import, but it is not an import",
                self.stage,
                self.pipeline.statement(import)
            );
        };
        let name = self
            .resolve(&directive.predicate)
            .unwrap_or_else(|problem| {
                panic!("`{}` does not resolve: {problem}", directive.predicate)
            });
        self.take_out(import);
        self.removed_imports.push((import, name));
    }

    /// Panics, naming the stage, if a statement other than a `@declare` directive
    /// names the predicate of an import removed with [remove_import](Self::remove_import).
    pub(super) fn expect_unread(&self) {
        if self.removed_imports.is_empty() {
            return;
        }
        let readers: Vec<StatementId> = self
            .statements
            .iter()
            .copied()
            .filter(|&id| {
                !matches!(
                    self.pipeline.statement(id),
                    Statement::Directive(Directive::Declare(_))
                )
            })
            .collect();
        let named = self.names_of(&readers);
        for (import, name) in &self.removed_imports {
            assert!(
                !named.contains(name),
                "the `{}` stage removed the import `{}`, but the program still names `{name}`",
                self.stage,
                self.pipeline.statement(*import)
            );
        }
    }

    /// Replaces an input rule by a rewritten rule, in place,
    /// recording the occurrences a substitution replaced.
    ///
    /// The other arguments and the panics are those of [replace_rule](Self::replace_rule).
    pub(super) fn replace_rule_substituted(
        &mut self,
        input: StatementId,
        rule: Rule,
        literals: Vec<LiteralPath>,
        substituted: Vec<SubstitutionRecord>,
    ) -> StatementId {
        self.expect_rule(input);
        let position = self.position(input);
        self.statements.remove(position);
        let created = self.derive_at(&[input], vec![Statement::Rule(rule)], position)[0];
        self.pending.push(PendingDeclaration::Replaces {
            created,
            input,
            view: None,
            literals,
            substituted,
        });
        created
    }

    /// Replaces an input rule by an import whose view is the rule itself, in place,
    /// as [extract_rule](Self::extract_rule) describes.
    pub(super) fn replace_by_import(
        &mut self,
        input: StatementId,
        import: ImportDirective,
        sources: &[StatementId],
    ) -> StatementId {
        self.expect_rule(input);
        let view = self.rule(input).clone();
        self.expect_view(&import, &view);
        let literals = (0..view.body.len())
            .map(|position| vec![position])
            .collect();
        let position = self.position(input);
        self.statements.remove(position);
        let mut all = vec![input];
        all.extend(sources.iter().copied().filter(|&source| source != input));
        let created = self.derive_at(
            &all,
            vec![Statement::Directive(Directive::Import(import))],
            position,
        )[0];
        self.pending.push(PendingDeclaration::Replaces {
            created,
            input,
            view: Some(view),
            literals,
            substituted: Vec::new(),
        });
        created
    }

    /// Records the rewrite declaration of every created statement still live
    /// on the provenance edge that created it.
    ///
    /// For a rule that replaces an input rule, this computes the correspondence:
    /// what every input variable became, from matching each input literal
    /// against the literal at its path.
    ///
    /// # Panics
    ///
    /// Panics if a predicate is defined twice, or if an input literal
    /// does not match the literal at its path.
    pub(super) fn record_declarations(&mut self) {
        let pending: Vec<PendingDeclaration> = std::mem::take(&mut self.pending)
            .into_iter()
            .filter(|pending| self.statements.contains(&pending.created()))
            .collect();

        let definitions = self.defined_rules(&pending);

        let mut declarations = Vec::with_capacity(pending.len());
        {
            let matcher = CorrespondenceMatcher {
                definitions: &definitions,
                scope: self.scope(),
            };
            for pending in pending {
                let declaration = match pending {
                    PendingDeclaration::Replaces {
                        created,
                        input,
                        view,
                        literals,
                        substituted,
                    } => {
                        let input_rule = self.rule(input);
                        let new_rule = view.as_ref().unwrap_or_else(|| self.rule(created));
                        let correspondence = matcher
                            .correspond(input_rule, new_rule, &literals)
                            .unwrap_or_else(|reason| {
                                panic!(
                                    "the `{}` stage declared that `{new_rule}` replaces \
                                     `{input_rule}`, but {reason}",
                                    self.stage
                                )
                            });
                        (
                            created,
                            RewriteDeclaration::Replaces {
                                view,
                                correspondence,
                                substituted,
                            },
                        )
                    }
                    PendingDeclaration::Defines { created, view, .. } => {
                        (created, RewriteDeclaration::Defines { view })
                    }
                    PendingDeclaration::DefinesFilter { created } => {
                        (created, RewriteDeclaration::DefinesFilter)
                    }
                };
                declarations.push(declaration);
            }
        }
        for (created, declaration) in declarations {
            let edge = self
                .pipeline
                .creating_edge(created)
                .expect("created statements have an edge");
            self.pipeline.edge_mut(edge).declaration = Some(declaration);
        }
    }

    /// Returns the rules defining the predicates the pending rewrite declarations define,
    /// by predicate.
    ///
    /// # Panics
    ///
    /// Panics if a predicate is defined twice.
    fn defined_rules(&self, pending: &[PendingDeclaration]) -> HashMap<String, Rule> {
        let mut rules: HashMap<String, Rule> = HashMap::new();
        for pending in pending {
            let PendingDeclaration::Defines {
                created,
                predicate,
                view,
            } = pending
            else {
                continue;
            };
            let rule = match view {
                Some(view) => view.clone(),
                None => self.rule(*created).clone(),
            };
            assert!(
                rules.insert(predicate.clone(), rule).is_none(),
                "the `{}` stage defined `{predicate}` twice",
                self.stage
            );
        }
        rules
    }

    /// Returns the rule a statement is.
    ///
    /// # Panics
    ///
    /// Panics if the statement is not a rule.
    fn rule(&self, id: StatementId) -> &Rule {
        match self.pipeline.statement(id) {
            Statement::Rule(rule) => rule,
            other => panic!("`{other}` is not a rule"),
        }
    }

    /// Panics, naming the stage, if a statement is not a rule.
    pub(super) fn expect_rule(&self, id: StatementId) {
        assert!(
            matches!(self.pipeline.statement(id), Statement::Rule(_)),
            "the `{}` stage rewrote `{}` as a rule",
            self.stage,
            self.pipeline.statement(id)
        );
    }

    /// Returns the resolved name of the predicate a definition defines,
    /// checking its shape: one head atom of distinct universal variables
    /// over a predicate the input does not use.
    fn new_definition(&self, rule: &Rule) -> String {
        let [head] = rule.head.as_slice() else {
            panic!(
                "the `{}` stage defines by `{rule}`, whose head is not one atom",
                self.stage
            );
        };
        assert!(
            distinct_universals(head),
            "the `{}` stage defines by `{rule}`, whose head is not made of distinct \
             universal variables",
            self.stage
        );
        self.expect_new(&head.predicate)
    }

    /// Panics, naming the stage, unless a view's head is the import's atom:
    /// its predicate over its column variables,
    /// or over distinct universal variables if it names no columns.
    fn expect_view(&self, import: &ImportDirective, view: &Rule) {
        let fits = match view.head.as_slice() {
            [head] if import.variables.is_empty() => {
                head.predicate == import.predicate && distinct_universals(head)
            }
            [head] => {
                *head
                    == Atom::new(
                        import.predicate.clone(),
                        import
                            .variables
                            .iter()
                            .map(|name| Term::variable(Variable::universal(name))),
                    )
            }
            _ => false,
        };
        assert!(
            fits,
            "the `{}` stage gave `{import}` the view `{view}`, whose head is not the \
             import's atom",
            self.stage
        );
    }

    /// Returns the resolved name of a predicate the input does not use,
    /// panicking, naming the stage, if it does.
    fn expect_new(&self, predicate: &Identifier) -> String {
        let name = self
            .resolve(predicate)
            .unwrap_or_else(|problem| panic!("`{predicate}` does not resolve: {problem}"));
        assert!(
            !self.input_names().contains(&name),
            "the `{}` stage introduced `{predicate}`, which the input uses",
            self.stage
        );
        name
    }

    /// Returns the resolved predicate names the input uses.
    fn input_names(&self) -> &HashSet<String> {
        self.input_names
            .get_or_init(|| self.names_of(self.pipeline.revision_statements(self.base)))
    }
}

/// Returns whether an atom's terms are distinct universal variables.
fn distinct_universals(atom: &Atom) -> bool {
    let mut seen = HashSet::with_capacity(atom.terms.len());
    atom.terms.iter().all(|term| {
        matches!(&term.kind, TermKind::Variable(variable)
            if variable.kind == VariableKind::Universal && seen.insert(variable.name.as_str()))
    })
}
