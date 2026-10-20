//! The instantiations in scope of a program or module, by alias,
//! and the names a reference reaches through each.

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ModuleError,
    model::{Identifier, Span, UseDirective},
};

/// The module instantiations of one program or module, by alias.
#[derive(Debug, Default)]
pub(super) struct InstancesByAlias {
    /// The instantiations, by alias.
    pub(super) by_alias: HashMap<String, InstanceInterface>,
}

impl InstancesByAlias {
    /// Records one instantiation, or returns an error if its alias is already taken.
    pub(super) fn add(
        &mut self,
        directive: &UseDirective,
        instance: InstanceInterface,
    ) -> Result<(), Diagnostic> {
        if let Some(earlier) = self.by_alias.insert(directive.alias.clone(), instance) {
            let mut problem = ModuleError::DuplicateAlias {
                alias: directive.alias.clone(),
            }
            .at(directive.span);
            if let Some(span) = earlier.span {
                problem = problem.with_label(span, "first used here");
            }
            return Err(problem);
        }
        Ok(())
    }

    /// Returns the aliases in scope.
    pub(super) fn aliases(&self) -> impl Iterator<Item = &str> {
        self.by_alias.keys().map(String::as_str)
    }

    /// Resolves an `alias::name` reference.
    ///
    /// A public name can stand for another instantiation's public name,
    /// if a `@use` bound an input to one,
    /// so the reference is followed until it reaches a name that is not qualified.
    /// A chain that visits no name twice takes at most as many steps as there are public names,
    /// so a longer one is a cycle.
    pub(super) fn resolve(
        &self,
        module: &str,
        name: &str,
        span: Option<Span>,
    ) -> Result<Identifier, Diagnostic> {
        let mut reached = self.lookup(module, name, span)?;
        let names: usize = self
            .by_alias
            .values()
            .map(|instance| instance.public.len())
            .sum();
        for _ in 0..=names {
            let Identifier::Qualified {
                module: next,
                name: offered,
            } = &reached
            else {
                return Ok(reached);
            };
            let (next, offered) = (next.clone(), offered.clone());
            reached = self.lookup(&next, &offered, span)?;
        }
        Err(ModuleError::PassedInCycle {
            module: module.to_owned(),
            name: name.to_owned(),
        }
        .at(span))
    }

    /// Returns what an `alias::name` reference stands for,
    /// without following the qualified name an input may be bound to.
    fn lookup(
        &self,
        module: &str,
        name: &str,
        span: Option<Span>,
    ) -> Result<Identifier, Diagnostic> {
        let Some(instance) = self.by_alias.get(module) else {
            let mut known: Vec<&str> = self.aliases().collect();
            known.sort_unstable();
            let mut problem = ModuleError::UnknownModule {
                module: module.to_owned(),
            }
            .at(span);
            if !known.is_empty() {
                problem =
                    problem.with_note(format!("the aliases in use are: {}", known.join(", ")));
            }
            return Err(problem);
        };
        let Some(resolved) = instance.public.get(name) else {
            let mut known: Vec<&str> = instance.public.keys().map(String::as_str).collect();
            known.sort_unstable();
            let mut problem = ModuleError::NotOffered {
                module: module.to_owned(),
                name: name.to_owned(),
            }
            .at(span);
            if !known.is_empty() {
                problem = problem.with_note(format!("`{module}` offers: {}", known.join(", ")));
            }
            return Err(problem);
        };
        Ok(resolved.clone())
    }
}

/// One module instantiation: which names a reference may reach, and what
/// each of them stands for.
#[derive(Debug)]
pub(super) struct InstanceInterface {
    /// The names the module offers with `@output`, by the unqualified name a reference uses.
    ///
    /// A name of the module's own maps to what the instantiation renamed it to.
    /// An input maps to the predicate it was bound to, which belongs to the using program.
    pub(super) public: HashMap<String, Identifier>,
    /// The span of the `@use`, for diagnostics.
    pub(super) span: Option<Span>,
}
