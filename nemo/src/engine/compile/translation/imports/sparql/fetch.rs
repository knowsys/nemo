//! Bound SPARQL imports (imports with a `where` clause): compiling one into the fetch rule
//! the engine evaluates during reasoning.

use std::collections::{HashMap, HashSet};

use nemo_io::{selection::RowSelection, sparql::QueryShape};
use nemo_physical::{
    diagnostics::{Count, Diagnostic},
    management::TableId,
};

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::compiled::{CompiledFetch, CompiledRule, FetchColumn, HeadAtom},
        compile::translation::{
            Translator,
            compiler::RuleCompiler,
            imports::{PendingImport, sparql::SparqlImportSpec},
            unsupported,
        },
    },
    model::{Atom, TermKind, VariableKind},
};

/// Returns, per column variable named after the predicate,
/// its position in the query's projection, which the binding atoms refer to.
///
/// Every column must be named.
/// A name given twice was refused when the import's columns were checked.
fn column_positions(
    import: &PendingImport,
    source_width: usize,
) -> Result<HashMap<&str, usize>, Diagnostic> {
    if import.variables.is_empty() {
        return Err(TranslationError::ImportColumnCount {
            declared: Count(0),
            arity: source_width.into(),
        }
        .at(import.span)
        .with_hint("name the columns after the predicate: `@import q(?x, ?y) from …`"));
    }
    Ok(import
        .variables
        .iter()
        .enumerate()
        .map(|(position, variable)| (variable.as_str(), position))
        .collect())
}

impl<'a> Translator<'a> {
    /// Compiles a bound import into its fetch rule.
    ///
    /// The binding atoms are the body (constants and repetitions normalizing as in any rule),
    /// and the import's table is the head.
    /// The fetch records which delivered column of each atom binds which column of the query.
    /// The engine evaluates the rule by fetching instead of joining.
    /// Otherwise it is stratified and evaluated like any other rule,
    /// including after `extend` adds facts.
    pub(super) fn bound_sparql_import(
        &mut self,
        import: &PendingImport,
        spec: &SparqlImportSpec,
        table: TableId,
        selection: RowSelection,
    ) -> Result<(CompiledRule, Vec<Option<String>>), Diagnostic> {
        let positions = column_positions(import, spec.layout.source_width())?;
        if let QueryShape::Opaque(reason) = spec.query.shape() {
            return Err(TranslationError::OpaqueQueryBound { reason }.at(import.span));
        }
        let scratch = self.fetch_scratch(&import.predicate, spec.layout.delivered())?;

        let mut compiler = RuleCompiler::new(self);
        let mut atoms = Vec::with_capacity(import.bindings.len());
        for atom in &import.bindings {
            atoms.push(compiler.binding_atom(atom, &import.predicate, &positions)?);
        }
        debug_assert!(
            compiler.deferred.is_empty(),
            "binding atoms hold no expressions"
        );

        // The head: the delivered columns, each its column variable's
        // id, bound by a binding atom or fresh if no atom binds it.
        let mut columns = Vec::with_capacity(spec.layout.delivered());
        for (column, variable) in import.variables.iter().enumerate() {
            if spec.layout.delivers(column) {
                columns.push(compiler.variable(variable));
            }
        }
        let names = compiler.source_names(compiler.variables);

        let compiled = CompiledRule {
            variables: compiler.variables,
            // A fetch rule joins nothing, and its order stays the
            // identity: the fetch records each match's variable values
            // by variable id, and the proof search reads them back the
            // same way. The order chooser never sees a fetch rule, since
            // fetch rules move to their reserved rule indices after it
            // has run.
            order: (0..compiler.variables).collect(),
            body: compiler.body,
            filters: compiler.filters,
            bindings: Vec::new(),
            computed: Vec::new(),
            negated: Vec::new(),
            heads: vec![HeadAtom { table, columns }],
            aggregate: None,
            existential: None,
            never_fires: false,
            nondeterministic: false,
            last_applied: 0,
            fetch: Some(CompiledFetch {
                endpoint: spec.endpoint.clone(),
                query: spec.query.clone(),
                headers: spec.headers.clone(),
                page_size: spec.page_size,
                selection,
                predicate: import.predicate.clone(),
                atoms,
                scratch,
                span: import.span,
                on_error: spec.on_error,
            }),
        };
        Ok((compiled, names))
    }

    /// Returns the scratch table a bound import's fetches deduplicate through, of the head's
    /// arity.
    ///
    /// It persists across translations,
    /// and one of another arity (after a `reload` projected the import differently) is replaced.
    fn fetch_scratch(&mut self, predicate: &str, arity: usize) -> Result<TableId, Diagnostic> {
        let kept = match self.scratch.get(predicate) {
            Some(&scratch) if self.manager.arity(scratch) == arity => Some(scratch),
            Some(&scratch) => {
                self.manager.drop_table(scratch);
                None
            }
            None => None,
        };
        Ok(match kept {
            Some(scratch) => scratch,
            None => {
                let scratch = self.manager.register(format!("{predicate}#fetch"), arity);
                self.scratch.insert(predicate.to_owned(), scratch);
                scratch
            }
        })
    }
}

impl RuleCompiler<'_, '_> {
    /// Compiles one binding atom of a bound import as a body atom,
    /// and records what each of its delivered columns is for the fetch,
    /// read off the atom before the compiler normalizes it.
    ///
    /// A variable repeated within the atom binds its column once,
    /// since a `VALUES` block names each variable once,
    /// and the compiler's identity filter keeps the repetition's meaning.
    fn binding_atom(
        &mut self,
        atom: &Atom,
        predicate: &str,
        positions: &HashMap<&str, usize>,
    ) -> Result<Vec<FetchColumn>, Diagnostic> {
        if atom.labels.is_some() {
            return Err(unsupported("a named argument in a binding atom", atom.span));
        }
        let name = self.translator.resolve(&atom.predicate, atom.span)?;
        if name == predicate {
            return Err(TranslationError::BindingSelf { predicate: name }.at(atom.span));
        }
        let layout = self.translator.plan.layout_of(&name, atom.terms.len());
        let mut columns = Vec::with_capacity(atom.terms.len());
        let mut bound_here: HashSet<usize> = HashSet::new();
        for (position, term) in atom.terms.iter().enumerate() {
            let column = match &term.kind {
                TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                    match positions.get(variable.name.as_str()) {
                        Some(&position) if bound_here.insert(position) => {
                            FetchColumn::Bound(position)
                        }
                        Some(_) => FetchColumn::Free,
                        None => {
                            return Err(TranslationError::BindingVariableUnknown {
                                variable: variable.name.clone(),
                            }
                            .at(term.span.or(atom.span)));
                        }
                    }
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Anonymous => {
                    FetchColumn::Free
                }
                TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                    FetchColumn::Constant(self.translator.global(&variable.name, term.span)?)
                }
                TermKind::Variable(_) => {
                    return Err(TranslationError::BindingTermKind.at(term.span.or(atom.span)));
                }
                _ if term.is_ground() => FetchColumn::Constant(self.translator.evaluate(term)?),
                _ => {
                    return Err(TranslationError::BindingTermKind.at(term.span.or(atom.span)));
                }
            };
            if !layout.is_pruned(position) {
                columns.push(column);
            }
        }
        self.body_atom(atom)?;
        Ok(columns)
    }
}
