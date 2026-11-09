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
    /// The rewritten rule,
    /// or `None` if the rule was folded into the import of its one replacement.
    pub(super) rule: Option<Rule>,
    /// The candidate imports that the rule read,
    /// which are sources of the added statements besides the rule.
    pub(super) imports: Vec<StatementId>,
}

/// What replaces one component: the bound import that replaces its atoms,
/// with its binding rules and its declaration.
pub(super) struct ComponentReplacement {
    /// The binding rules, whose rows restrict the import's requests.
    pub(super) bindings: Vec<Rule>,
    /// The declaration copied from the candidates, if there is one.
    pub(super) declaration: Option<DeclareDirective>,
    /// The bound import.
    pub(super) import: ImportDirective,
    /// The literals that define the import's rows, made from the atoms that it replaces.
    pub(super) definition: Vec<Literal>,
    /// The body positions of the atoms that the import replaces.
    pub(super) positions: Vec<usize>,
}

impl RewriteContext {
    /// Rewrites one rule, with fresh names from the commit,
    /// or returns `None` if the rule stays as written.
    ///
    /// A rule stays as written if rewriting would not reduce what it fetches,
    /// or if it uses named arguments.
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
    /// The commit from which fresh names are taken.
    commit: &'a mut Commit<'c>,
    /// The rule.
    pub(super) rule: &'a Rule,
    /// Every universal variable that the rule mentions,
    /// from which private variable names must differ.
    variables: HashSet<String>,
    /// The universal variables that the rule joins on,
    /// with the fresh variables of the computed arguments.
    joined: HashSet<String>,
    /// The universal variables of the head.
    head_variables: HashSet<String>,
    /// The number of candidate queries instantiated so far,
    /// which is the suffix that distinguishes each instance's private variables.
    instances: usize,
    /// The replacements of the components replaced so far, in order.
    pub(super) replacements: Vec<ComponentReplacement>,
    /// The candidate imports read so far.
    imports: Vec<StatementId>,
    /// Per replaced component, the literal that replaces it,
    /// with the position of the component's first atom.
    literals: Vec<(usize, Literal)>,
    /// The body positions of the atoms that the literals replace.
    removed: HashSet<usize>,
}

impl<'a, 'c> RuleRewriter<'a, 'c> {
    /// Creates the rewrite of a rule in a context.
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

    /// Runs the rewrite.
    ///
    /// The remote atoms are resolved and grouped,
    /// every component whose replacement reduces what it fetches is replaced,
    /// and the rule is assembled over the literals that replace the components.
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
            let mut overlapping = self
                .context
                .overlapping(&groups, &component, &variables, self.rule);
            // A query that groups or nests a subquery would compute it in full again
            // for each page of bindings, so it stays unbound.
            if atoms
                .iter()
                .any(|atom| self.context.candidate(atom).query.computes_in_full())
            {
                overlapping.clear();
            }
            // A plain atom that shares no variable with a local group would
            // fetch the same rows as the candidate import, so it stays.
            if component.plain(&remote) && overlapping.is_empty() {
                continue;
            }
            // A component without variables has nothing to select,
            // and a query needs a variable.
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

    /// Returns the variables that a component's query must project.
    ///
    /// These are the component's variables that the rest of the rule reads,
    /// in the head or in the body outside the component,
    /// and the fresh variables of its computed arguments,
    /// which the rule compares with their terms.
    /// If the rest of the rule reads none, the projection is the component's first variable,
    /// and the result is `None` if the component has no variable.
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

    /// Replaces a component.
    ///
    /// It composes the component's query, makes the binding rules of the `overlapping`
    /// groups, forms the bound import,
    /// and records the literal that reads the import in place of the atoms.
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

    /// Returns a replaced atom as the import's definition reads it.
    ///
    /// A computed argument becomes its fresh variable.
    /// The atom is positive if the component is read negatively,
    /// since the import contains what the atom matches.
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

    /// Returns a component's query: the atoms' patterns, instantiated and joined,
    /// with the absorbed negated atoms added as `FILTER NOT EXISTS` conditions,
    /// and projected onto the `projected` variables.
    ///
    /// The query of a component that the rule reads negatively is its one atom's own pattern.
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
            // Without a positive pattern, the exclusion built above is unused,
            // and the query is the atom's own pattern, instantiated again.
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

    /// Returns the names that a component's query gives to rule variables,
    /// where they differ from the variables' own names.
    ///
    /// A query that [keeps its names](SparqlQuery::keeps_names) names each variable
    /// as it names the column that binds the variable.
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

    /// Returns an atom's candidate query instantiated at the atom,
    /// with the rule variables named by `names`.
    ///
    /// The instance's private variables are numbered differently from those of every other
    /// instance.
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
    /// with each replacing literal at the position of its component's first atom.
    ///
    /// A rule that only copies one bound import into a head predicate that no other rule
    /// writes is folded, and no rule is assembled, since the import then defines the head
    /// predicate itself.
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
