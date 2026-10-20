//! The renaming one instantiation applies to a module's names.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::link::{InstancesByAlias, InstantiatedModule, forbid_owned_name},
    model::{Identifier, IdentifierRenaming, IdentifierRole, Span},
};

/// The renaming one instantiation applies to a module's names.
pub(super) struct InstanceRenaming<'a> {
    /// The module being instantiated.
    module: &'a InstantiatedModule,
    /// The instantiations the module itself makes.
    nested: &'a InstancesByAlias,
    /// Their aliases: no name of the module may resolve to a name they own.
    owned: Vec<String>,
}

impl<'a> InstanceRenaming<'a> {
    /// Returns the renaming of a module's names, given the instantiations it
    /// makes itself.
    pub(super) fn new(module: &'a InstantiatedModule, nested: &'a InstancesByAlias) -> Self {
        Self {
            module,
            nested,
            owned: nested.aliases().map(str::to_owned).collect(),
        }
    }
}

impl IdentifierRenaming for InstanceRenaming<'_> {
    fn identifier(
        &mut self,
        identifier: &Identifier,
        role: IdentifierRole,
        span: Option<Span>,
    ) -> Result<Option<Identifier>, Diagnostic> {
        if let Identifier::Qualified { module, name } = identifier {
            return self.nested.resolve(module, name, span).map(Some);
        }
        match role {
            // The engine matches these by their exact text, so they are not
            // names in the program's namespace and nothing rewrites them.
            IdentifierRole::Reserved => Ok(None),
            // A value means the same thing everywhere; resolving it in
            // the module's own scope is all an instantiation does to it.
            IdentifierRole::Value => Ok(Some(Identifier::Iri(
                self.module.scope.resolve(identifier, span)?,
            ))),
            IdentifierRole::Predicate => {
                // Checked as written, so the error can say what the name resolves to.
                forbid_owned_name(identifier, &self.module.scope, &self.owned, span)?;
                let resolved = self.module.scope.resolve(identifier, span)?;
                match self.module.bindings.get(&resolved) {
                    // An input *is* the predicate it is bound to, named
                    // in the using program's scope, not this one's.
                    Some(actual) => Ok(Some(actual.clone())),
                    None => Ok(Some(Identifier::Iri(format!(
                        "{}{resolved}",
                        self.module.namespace
                    )))),
                }
            }
        }
    }

    fn global(&mut self, name: &str) -> Option<String> {
        // A module's globals are the module's: it can neither read the
        // using program's nor collide with them.
        Some(format!("{}{name}", self.module.namespace))
    }

    fn blank(&mut self, label: &str) -> Option<String> {
        // A blank node names a value of the instance's own, as one names
        // a value of the document in an RDF file: every instance has its own.
        Some(format!("{}{label}", self.module.namespace))
    }
}
