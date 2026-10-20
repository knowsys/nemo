//! The translation state the engine keeps from one translation to the next
//! (the initial load, `extend`, `reload`, and `import_rows`).

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    management::TableId,
    value::{DataValue, LabelTypes, Type},
};

use crate::{
    engine::compile::{import_plan::ImportPlan, translation::resolve_identifier, types},
    model::{Atom, Identifier, Span, Term, TypeExpression, Variable},
};

/// The translation state that outlives one translation,
/// which `extend` and rows added through `import_rows` translate against.
///
/// The parts derived from statements (prefixes, base, parameters, declarations, `@type`
/// definitions and their types, inputs) are rebuilt from the full statement list on every translation,
/// and [clear_statement_state](Self::clear_statement_state) empties them beforehand.
/// The other parts (seeds, open, plan, blanks, scratch) describe what the tables already store,
/// which skipped facts and unread imports cannot derive again, so they persist.
#[derive(Debug, Default, Clone)]
pub(crate) struct TranslationState {
    /// The declared base IRI bare names resolve against.
    pub(crate) base_iri: Option<String>,
    /// The declared prefixes.
    pub(crate) prefixes: HashMap<String, String>,
    /// The resolved global parameters.
    pub(crate) globals: HashMap<String, DataValue>,
    /// The declared column types, by resolved predicate name.
    pub(crate) declarations: HashMap<String, types::PredicateDeclaration>,
    /// The `@type` definitions, by resolved label.
    pub(crate) type_definitions: HashMap<String, (TypeExpression, Option<Span>)>,
    /// The types the `@type` definitions declare, by resolved label.
    pub(crate) label_types: LabelTypes,
    /// The seeded column types per table, from the facts and the import formats.
    pub(crate) seeds: HashMap<TableId, Vec<Type>>,
    /// Import-fed predicates whose columns are not statically known.
    pub(crate) open: HashSet<String>,
    /// The `@input` predicates, resolved.
    pub(crate) inputs: HashSet<String>,
    /// What the loaders of import-fed predicates left out.
    pub(crate) plan: ImportPlan,
    /// The program's blank nodes: per label, the null it stands for.
    pub(crate) blanks: HashMap<String, u64>,
    /// Per bound import's predicate, the scratch table its fetches deduplicate through,
    /// registered once and reused across translations.
    pub(crate) scratch: HashMap<String, TableId>,
}

impl TranslationState {
    /// Empties what statements alone determine, keeping what stored data determines.
    ///
    /// The next translation of the full statement list rebuilds the former.
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

    /// Returns the terms of an atom that translated, in declaration
    /// order: a named argument at its declared column, a column it
    /// leaves out as an anonymous variable.
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
