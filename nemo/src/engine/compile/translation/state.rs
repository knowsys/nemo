//! The translation state that the engine keeps from one translation to the next.

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    management::TableId,
    value::{DataValue, LabelTypes, Type},
};

use crate::{
    engine::compile::{import_plan::ImportPlan, translation::resolve_identifier, types},
    model::{Atom, Identifier, Span, Term, TypeExpression, Variable},
};

/// The translation state that outlives one translation.
///
/// Later translations start from it, such as those of
/// [extend](crate::engine::Engine::extend) and of rows added through
/// [import_rows](crate::engine::Engine::import_rows).
/// - The parts that statements determine (prefixes, base, parameters, declarations, `@type`
///   definitions and their types, and inputs) are rebuilt from the full list of statements in
///   every translation, and [clear_statement_state](Self::clear_statement_state) empties them
///   beforehand.
/// - The other parts (`seeds`, `open`, `plan` and `scratch`) describe what the tables already
///   store. Preloaded facts and imports are not read again and cannot derive these parts
///   again, so they persist.
#[derive(Debug, Default, Clone)]
pub(crate) struct TranslationState {
    /// The declared base IRI, against which bare names are resolved.
    pub(crate) base_iri: Option<String>,
    /// The declared prefixes.
    pub(crate) prefixes: HashMap<String, String>,
    /// The values of the global parameters, by name.
    pub(crate) globals: HashMap<String, DataValue>,
    /// The declared column types, by resolved predicate name.
    pub(crate) declarations: HashMap<String, types::PredicateDeclaration>,
    /// The `@type` definitions, by resolved label.
    pub(crate) type_definitions: HashMap<String, (TypeExpression, Option<Span>)>,
    /// The types that the `@type` definitions declare, by resolved label.
    pub(crate) label_types: LabelTypes,
    /// Per table, the initial column types from the facts and import formats.
    pub(crate) seeds: HashMap<TableId, Vec<Type>>,
    /// The imported predicates whose column types are not known before loading,
    /// and the `@input` predicates.
    pub(crate) open: HashSet<String>,
    /// The resolved `@input` predicates.
    pub(crate) inputs: HashSet<String>,
    /// What the loads of imported predicates left out.
    pub(crate) plan: ImportPlan,
    /// Per predicate of an import with atoms in its `where` clause,
    /// the scratch table through which its fetches deduplicate their rows.
    ///
    /// It is registered once, and reused across translations while its arity stays the same.
    pub(crate) scratch: HashMap<String, TableId>,
}

impl TranslationState {
    /// Empties the parts that statements alone determine,
    /// and keeps the parts that stored data determines.
    ///
    /// The next translation of the full list of statements rebuilds the emptied parts.
    pub(crate) fn clear_statement_state(&mut self) {
        self.base_iri = None;
        self.prefixes.clear();
        self.globals.clear();
        self.declarations.clear();
        self.type_definitions.clear();
        self.label_types = LabelTypes::default();
        self.inputs.clear();
    }

    /// Resolves an identifier against the declared prefixes and base, outside a translation.
    ///
    /// Returns `None` if the prefix is unknown or the identifier names a module.
    pub(crate) fn resolve(&self, identifier: &Identifier) -> Option<String> {
        resolve_identifier(self.base_iri.as_deref(), &self.prefixes, identifier)
    }

    /// Returns the terms of an atom that was translated, in declaration order.
    ///
    /// A named argument is placed at its declared column,
    /// and a column that the atom leaves out becomes an anonymous variable.
    /// An atom without named arguments, or of a predicate without a declaration,
    /// keeps its terms.
    pub(crate) fn positional(&self, atom: &Atom) -> Vec<Term> {
        let Some(labels) = &atom.labels else {
            return atom.terms.clone();
        };
        let Some(declaration) = self
            .resolve(&atom.predicate)
            .and_then(|name| self.declarations.get(&name))
        else {
            return atom.terms.clone();
        };
        let mut placed = vec![Term::variable(Variable::anonymous()); declaration.names.len()];
        for (label, term) in labels.iter().zip(&atom.terms) {
            if let Some(position) = declaration.position(label) {
                placed[position] = term.clone();
            }
        }
        placed
    }
}
