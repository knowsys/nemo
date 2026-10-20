//! The fresh statements a rewrite makes: the binding rules,
//! the bound import with the declaration it copies from the candidates,
//! and the folding of a copying rule into its import.

use nemo_io::sparql::{SparqlQuery, compose::Binding};

use crate::{
    engine::transform::sparql_rewrite::{
        components::{LocalGroup, RemoteAtom},
        context::RewriteContext,
        rule::RuleRewriter,
        variables::variable_terms,
    },
    model::{
        Atom, ColumnDeclaration, DeclareDirective, Identifier, ImportDirective, Literal, Operation,
        Operator, Rule, Term, TermKind, Variable, VariableKind,
    },
};

impl RuleRewriter<'_, '_> {
    /// Returns the binding rule of a local group sharing variables with a component,
    /// and the atom the import is bound by.
    ///
    /// The rule defines a fresh predicate over the shared variables by copies of the group's
    /// literals, followed by the computations of its computed arguments.
    pub(super) fn binding_rule(
        &mut self,
        stem: &str,
        group: &LocalGroup,
        variables: &[String],
    ) -> (Rule, Atom) {
        let shared: Vec<String> = group
            .variables
            .iter()
            .filter(|variable| variables.contains(*variable))
            .cloned()
            .collect();
        let name = self.fresh(stem, "bindings");
        let computations = group.computed.iter().map(|argument| {
            Literal::Operation(Operation::new(
                Operator::Equal,
                vec![
                    Term::variable(Variable::universal(&argument.variable)),
                    argument.term.clone(),
                ],
            ))
        });
        let body: Vec<Literal> = group
            .positions
            .iter()
            .map(|&position| self.rule.body[position].clone())
            .chain(computations)
            .collect();
        let atom = Atom::new(Identifier::bare(&name), variable_terms(&shared));
        (Rule::new(vec![atom.clone()], body), atom)
    }

    /// Returns the bound import replacing a component: the first atom's
    /// directive under a fresh predicate, with the projected columns,
    /// the composed query in place of the source's `query`, `format`,
    /// and `base` options, and the binding atoms.
    pub(super) fn bound_import(
        &self,
        source: &RemoteAtom,
        name: &str,
        projected: &[String],
        query: &SparqlQuery,
        bindings: Vec<Atom>,
    ) -> ImportDirective {
        let mut directive = self.context.candidate(source).directive.clone();
        directive.predicate = Identifier::bare(name);
        directive.variables = projected.to_vec();
        directive.options.retain(|(key, _)| {
            !matches!(&key.kind, TermKind::Constant(Identifier::Bare(name))
                if matches!(name.as_str(), "query" | "format" | "base"))
        });
        directive
            .options
            .push((Term::constant("query"), Term::string(query.text())));
        directive.bindings = bindings;
        directive.span = None;
        directive
    }

    /// Folds the rule into its one bound import if the rule only copies that import into its
    /// head, returning whether it folded.
    ///
    /// The import then defines the head predicate directly,
    /// and the copied declaration moves to the head predicate,
    /// unless the head predicate has a declaration of its own.
    /// No other rule may write the head predicate, and it must not be a candidate.
    pub(super) fn fold(&mut self, body: &[Literal]) -> bool {
        let [head] = self.rule.head.as_slice() else {
            return false;
        };
        let [Literal::Positive(atom)] = body else {
            return false;
        };
        let [replacement] = self.replacements.as_mut_slice() else {
            return false;
        };
        let import = &mut replacement.import;
        if import.predicate != atom.predicate
            || head.terms != atom.terms
            || !head.terms.iter().all(|term| {
                matches!(&term.kind, TermKind::Variable(variable)
                    if variable.kind == VariableKind::Universal)
            })
        {
            return false;
        }
        let Ok(name) = self.context.scope.resolve(&head.predicate, head.span) else {
            return false;
        };
        if self.context.writers.get(&name) != Some(&1)
            || self.context.candidates.contains_key(&name)
        {
            return false;
        }
        import.predicate = head.predicate.clone();
        let head_declared = self.context.declarations.contains_key(&name);
        if head_declared {
            replacement.declaration = None;
        } else if let Some(declaration) = &mut replacement.declaration {
            declaration.predicate = head.predicate.clone();
        }
        true
    }
}

impl RewriteContext {
    /// Returns the declaration a fresh import copies from the candidate imports it replaces:
    /// per projected variable, the column declared at the atom position binding it.
    ///
    /// Returns `None` unless every projected variable is bound at declared positions only,
    /// which all declare the same type.
    pub(super) fn declaration_for(
        &self,
        name: &str,
        projected: &[String],
        atoms: &[&RemoteAtom],
    ) -> Option<DeclareDirective> {
        let mut columns = Vec::with_capacity(projected.len());
        for variable in projected {
            let mut column: Option<&ColumnDeclaration> = None;
            for atom in atoms {
                for (position, binding) in atom.bindings.iter().enumerate() {
                    if !matches!(binding, Binding::Variable(bound) if bound == variable) {
                        continue;
                    }
                    let declared = self
                        .declarations
                        .get(&atom.candidate)?
                        .columns
                        .get(position)?;
                    match column {
                        Some(column) if column.datatype != declared.datatype => return None,
                        _ => column = Some(declared),
                    }
                }
            }
            columns.push(ColumnDeclaration {
                name: None,
                datatype: column?.datatype.clone(),
            });
        }
        Some(DeclareDirective {
            predicate: Identifier::bare(name),
            columns,
            span: None,
        })
    }
}
