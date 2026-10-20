//! The rewrite of one rule: its remote atoms are grouped into components,
//! each component becomes a bound import with its binding rules,
//! and the rule is assembled over these imports.

use std::collections::{HashMap, HashSet};

use nemo_io::sparql::{
    SparqlQuery,
    compose::{Binding, Composition},
    spargebra::algebra::GraphPattern,
};

use crate::{
    engine::transform::sparql_rewrite::{
        components::{ComputedArgument, LocalGroup, RemoteAtom, RemoteComponent},
        context::RewriteContext,
        variables::{joined_variables, universal, universal_variables},
    },
    model::{Atom, DeclareDirective, Identifier, ImportDirective, Literal, Rule, Term, Variable},
    pipeline::{Commit, StatementId},
};

/// Returns a rule variable's name in a query: the query's name for it, or its own.
fn named(names: &HashMap<String, String>, variable: &str) -> String {
    names
        .get(variable)
        .cloned()
        .unwrap_or_else(|| variable.to_owned())
}

/// What rewriting one rule produces.
pub(super) struct RuleRewrite {
    /// Per replaced component, what replaces it, in order.
    pub(super) replacements: Vec<ComponentReplacement>,
    /// The rewritten rule, or `None` if the rule folded into the import of its one replacement.
    pub(super) rule: Option<Rule>,
    /// The candidate imports the rule read, the rewrite's other provenance sources.
    pub(super) imports: Vec<StatementId>,
}

/// One component's replacement: the bound import replacing its atoms,
/// with its binding rules and declaration.
pub(super) struct ComponentReplacement {
    /// The binding rules restricting the import's requests.
    pub(super) bindings: Vec<Rule>,
    /// The declaration copied from the candidates.
    pub(super) declaration: Option<DeclareDirective>,
    /// The bound import.
    pub(super) import: ImportDirective,
    /// The literals defining the import's rows, from the atoms it replaces.
    pub(super) definition: Vec<Literal>,
    /// The body positions of the atoms the import replaces.
    pub(super) positions: Vec<usize>,
}

impl RewriteContext {
    /// Rewrites one rule, taking fresh names from the commit,
    /// or returns `None` if rewriting would not reduce what the rule fetches.
    pub(super) fn rewrite(&mut self, rule: &Rule, commit: &mut Commit<'_>) -> Option<RuleRewrite> {
        // Named arguments are resolved only at translation, so the stage
        // leaves rules using them alone.
        let labeled = rule
            .head
            .iter()
            .chain(rule.body.iter().filter_map(|literal| match literal {
                Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
                Literal::Operation(_) => None,
            }))
            .any(|atom| atom.labels.is_some());
        if labeled {
            return None;
        }
        RuleRewriter::new(self, commit, rule).run()
    }
}

/// One rule's rewrite in progress.
pub(super) struct RuleRewriter<'a, 'c> {
    /// What the stage knows of the program.
    pub(super) context: &'a mut RewriteContext,
    /// The commit fresh names are taken from.
    commit: &'a mut Commit<'c>,
    /// The rule.
    pub(super) rule: &'a Rule,
    /// Every universal variable the rule mentions, from which private variable names must differ.
    variables: HashSet<String>,
    /// The universal variables the rule joins on.
    joined: HashSet<String>,
    /// The universal variables of the head.
    head_variables: HashSet<String>,
    /// How many candidate queries have been instantiated,
    /// the suffix telling each instance's private variables apart.
    instances: usize,
    /// The replacements of the components replaced so far, in order.
    pub(super) replacements: Vec<ComponentReplacement>,
    /// The candidate imports read so far.
    imports: Vec<StatementId>,
    /// Per replaced component, the literal replacing it,
    /// with the position of the component's first atom.
    literals: Vec<(usize, Literal)>,
    /// The body positions of the atoms the literals replace.
    removed: HashSet<usize>,
}

impl<'a, 'c> RuleRewriter<'a, 'c> {
    /// Begins rewriting a rule in a context.
    fn new(context: &'a mut RewriteContext, commit: &'a mut Commit<'c>, rule: &'a Rule) -> Self {
        Self {
            context,
            commit,
            rule,
            variables: universal_variables(rule),
            joined: joined_variables(rule),
            head_variables: rule.head.iter().flat_map(universal).collect(),
            instances: 0,
            replacements: Vec::new(),
            imports: Vec::new(),
            literals: Vec::new(),
            removed: HashSet::new(),
        }
    }

    /// Returns a fresh bare predicate name from a stem and a role.
    pub(super) fn fresh(&mut self, stem: &str, role: &str) -> String {
        match self.commit.fresh(&format!("{stem}_{role}")) {
            Identifier::Bare(name) => name,
            other => unreachable!("fresh names are bare: {other}"),
        }
    }

    /// Runs the rewrite:
    /// the remote atoms are resolved and grouped,
    /// every component whose replacement reduces what it fetches is replaced,
    /// and the rule is assembled over the replacing literals.
    ///
    /// Returns `None` if nothing is replaced, or if a component's query cannot be composed.
    /// The rule then goes to translation as written.
    fn run(mut self) -> Option<RuleRewrite> {
        let remote = self.context.remote_atoms(self.rule, &self.variables)?;
        if remote.is_empty() {
            return None;
        }
        let computed: Vec<ComputedArgument> = remote
            .iter()
            .flat_map(|atom| atom.computed.iter().cloned())
            .collect();
        // The rule compares a computed argument's column with its term.
        self.joined
            .extend(computed.iter().map(|argument| argument.variable.clone()));
        let local: Vec<usize> = (0..self.rule.body.len())
            .filter(|&position| !remote.iter().any(|atom| atom.position == position))
            .collect();
        let groups = LocalGroup::all(self.rule, &local, &computed);
        for component in self.context.components(&remote, &groups) {
            let atoms = component.atoms(&remote);
            let variables = component.variables(&remote);
            let overlapping = self
                .context
                .overlapping(&groups, &component, &variables, self.rule);
            // A plain atom sharing no variable with a local group would
            // fetch the same rows as the candidate import, so it stays.
            if component.plain(&remote) && overlapping.is_empty() {
                continue;
            }
            // Nothing to select: a query needs a variable.
            let Some(projected) = self.projection(&atoms, &variables) else {
                continue;
            };
            self.replace(&component, &atoms, &variables, &projected, &overlapping)?;
        }
        if self.literals.is_empty() {
            return None;
        }
        Some(self.finish())
    }

    /// Returns what a component must project:
    /// those of its variables the rest of the rule reads,
    /// the head and the body outside the component,
    /// and its computed arguments, which the rule compares with their terms.
    ///
    /// Returns its first variable if the rest reads none, and `None` if it has none at all.
    fn projection(&self, atoms: &[&RemoteAtom], variables: &[String]) -> Option<Vec<String>> {
        let mut elsewhere = self.head_variables.clone();
        for (position, literal) in self.rule.body.iter().enumerate() {
            if atoms.iter().any(|atom| atom.position == position) {
                continue;
            }
            elsewhere.extend(universal(literal));
        }
        elsewhere.extend(atoms.iter().flat_map(|atom| {
            atom.computed
                .iter()
                .map(|argument| argument.variable.clone())
        }));
        let projected: Vec<String> = variables
            .iter()
            .filter(|variable| elsewhere.contains(*variable))
            .cloned()
            .collect();
        if !projected.is_empty() {
            return Some(projected);
        }
        variables.first().map(|variable| vec![variable.clone()])
    }

    /// Replaces a component:
    /// composes its query, makes the binding rules of the groups sharing variables with it,
    /// forms the bound import, and records the literal reading the import in place of the atoms.
    ///
    /// Returns `None` if the query cannot be composed.
    fn replace(
        &mut self,
        component: &RemoteComponent,
        atoms: &[&RemoteAtom],
        variables: &[String],
        projected: &[String],
        overlapping: &[&LocalGroup],
    ) -> Option<()> {
        let query = self.query(atoms, projected)?;
        let stem = self.context.candidate(atoms[0]).stem.clone();
        let (bindings, binding_atoms): (Vec<Rule>, Vec<Atom>) = overlapping
            .iter()
            .map(|group| self.binding_rule(&stem, group, variables))
            .unzip();
        let name = self.fresh(&stem, "query");
        // The fresh predicate copies the candidates' declarations, so its
        // rows are checked against the same types.
        let declaration = self.context.declaration_for(&name, projected, atoms);
        let import = self.bound_import(atoms[0], &name, projected, &query, binding_atoms);
        for atom in atoms {
            let id = self.context.candidate(atom).id;
            if !self.imports.contains(&id) {
                self.imports.push(id);
            }
            self.removed.insert(atom.position);
        }
        let mut positions: Vec<usize> = atoms.iter().map(|atom| atom.position).collect();
        positions.sort_unstable();
        let definition = positions
            .iter()
            .map(|&position| {
                let atom = atoms
                    .iter()
                    .find(|atom| atom.position == position)
                    .expect("every position is a replaced atom's");
                self.defining_literal(atom, component.negative())
            })
            .collect();
        // The rule reads a computed argument's column as written.
        let terms: Vec<Term> = projected
            .iter()
            .map(|variable| {
                atoms
                    .iter()
                    .flat_map(|atom| &atom.computed)
                    .find(|argument| &argument.variable == variable)
                    .map_or_else(
                        || Term::variable(Variable::universal(variable)),
                        |argument| argument.term.clone(),
                    )
            })
            .collect();
        let replacement = Atom::new(Identifier::bare(&name), terms);
        self.literals.push((
            positions[0],
            if component.negative() {
                Literal::Negative(replacement)
            } else {
                Literal::Positive(replacement)
            },
        ));
        self.replacements.push(ComponentReplacement {
            bindings,
            declaration,
            import,
            definition,
            positions,
        });
        Some(())
    }

    /// Returns a replaced atom as the import's definition reads it:
    /// a computed argument as its fresh variable,
    /// and positive if the component is read negatively, as the import holds what it matches.
    fn defining_literal(&self, atom: &RemoteAtom, negative: bool) -> Literal {
        let (Literal::Positive(written) | Literal::Negative(written)) =
            &self.rule.body[atom.position]
        else {
            unreachable!("a remote atom is an atom of the body")
        };
        let mut defining = written.clone();
        for argument in &atom.computed {
            defining.terms[argument.position] =
                Term::variable(Variable::universal(&argument.variable));
        }
        if atom.negated && !negative {
            Literal::Negative(defining)
        } else {
            Literal::Positive(defining)
        }
    }

    /// Returns a component's query: the atoms' patterns instantiated and joined,
    /// the absorbed negated atoms added as `FILTER NOT EXISTS` conditions,
    /// projected onto the variables the rest of the rule reads.
    ///
    /// The query of a component read negatively is its one atom's own pattern.
    /// The query of a single atom keeps its dataset.
    fn query(&mut self, atoms: &[&RemoteAtom], projected: &[String]) -> Option<SparqlQuery> {
        let names = self.query_names(atoms);
        let mut composition = Composition::default();
        for atom in atoms {
            let pattern = self.instantiate(atom, &names)?;
            if atom.negated {
                composition.exclude(pattern);
            } else {
                composition.join(pattern);
            }
        }
        if composition.joins_nothing() {
            // Without a positive pattern, the exclusion built above is
            // unused: the query is the atom's own pattern, instantiated
            // again.
            composition = Composition::default();
            composition.join(self.instantiate(atoms[0], &names)?);
        }
        let dataset = (atoms.len() == 1)
            .then(|| self.context.candidate(atoms[0]).query.dataset().cloned())
            .flatten();
        let projected: Vec<String> = projected
            .iter()
            .map(|variable| named(&names, variable))
            .collect();
        composition.select(&projected, dataset)
    }

    /// Returns the names a component's query gives rule variables other than their own:
    /// a query that [keeps its names](SparqlQuery::keeps_names) names each variable
    /// as it names the column binding it.
    ///
    /// Such a query stands alone, so no other instance's names can clash with these.
    fn query_names(&self, atoms: &[&RemoteAtom]) -> HashMap<String, String> {
        let mut names = HashMap::new();
        for atom in atoms {
            let query = &self.context.candidate(atom).query;
            if !query.keeps_names() {
                continue;
            }
            for (binding, own) in atom.bindings.iter().zip(query.variables()) {
                if let Binding::Variable(variable) = binding {
                    names.entry(variable.clone()).or_insert_with(|| own.clone());
                }
            }
        }
        names
    }

    /// Returns an atom's candidate query instantiated at the atom under the query's names,
    /// its private variables numbered differently from every other instance's.
    fn instantiate(
        &mut self,
        atom: &RemoteAtom,
        names: &HashMap<String, String>,
    ) -> Option<GraphPattern> {
        self.instances += 1;
        let bindings: Vec<Binding> = atom
            .bindings
            .iter()
            .map(|binding| match binding {
                Binding::Variable(variable) => Binding::Variable(named(names, variable)),
                constant => constant.clone(),
            })
            .collect();
        let joined: HashSet<String> = self
            .joined
            .iter()
            .map(|variable| named(names, variable))
            .collect();
        self.context.candidate(atom).query.instantiate(
            &bindings,
            &self.instances.to_string(),
            &self.variables,
            &joined,
        )
    }

    /// Assembles the rewrite: the rule over the new imports,
    /// each replacement at the position of its component's first atom.
    ///
    /// A rule that only copies one bound import into a head predicate no other rule writes
    /// assembles no rule, since the import then defines the head predicate itself.
    fn finish(mut self) -> RuleRewrite {
        let mut body = Vec::with_capacity(self.rule.body.len());
        for (position, literal) in self.rule.body.iter().enumerate() {
            for (at, replacement) in &self.literals {
                if *at == position {
                    body.push(replacement.clone());
                }
            }
            if !self.removed.contains(&position) {
                body.push(literal.clone());
            }
        }
        let rule = (!self.fold(&body)).then(|| Rule {
            attributes: self.rule.attributes.clone(),
            head: self.rule.head.clone(),
            body,
            span: self.rule.span,
        });
        RuleRewrite {
            replacements: self.replacements,
            rule,
            imports: self.imports,
        }
    }
}
