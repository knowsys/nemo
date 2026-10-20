//! The types a check's analysis inferred.

use std::collections::HashMap;

use nemo_physical::value::Type;

use crate::{
    engine::{compile::compiled::CompiledProgram, registered::Registered},
    model::{Identifier, Span},
    pipeline::NameScope,
};

use super::RuleTypes;

/// The types the analysis of a [check](crate::engine::Engine::check) inferred:
/// what each predicate's columns can hold,
/// and what each rule's variables can hold once the rule's conditions narrowed them.
///
/// [TypeSyntax](crate::values::TypeSyntax) writes a type in declaration syntax.
#[derive(Debug, Clone)]
pub struct InferredTypes {
    /// The scope the program's names resolve in.
    scope: NameScope,
    /// The column types of the program's predicates, by resolved name.
    predicates: HashMap<String, Vec<Type>>,
    /// The variable types of the rules with a source position, in program order.
    rules: Vec<RuleTypes>,
}

impl InferredTypes {
    /// Collects the types a check inferred for a program:
    /// the column types of the predicates it writes, given by resolved name,
    /// and the variable types of its compiled rules.
    ///
    /// `scope` is the written program's, and `tables` the predicates the check registered.
    /// A check runs no import pass, so every column of an import is stored and typed.
    pub(crate) fn of_check<'a>(
        scope: NameScope,
        written: impl Iterator<Item = &'a str>,
        tables: &HashMap<String, Registered>,
        compiled: &CompiledProgram,
    ) -> Self {
        let predicates = written
            .filter_map(|name| {
                let Registered { table, .. } = tables.get(name)?;
                let types = compiled.column_types.get(table)?;
                Some((name.to_owned(), types.clone()))
            })
            .collect();
        Self {
            scope,
            predicates,
            rules: rule_types(compiled),
        }
    }

    /// Returns the types each column of a predicate can hold,
    /// its name resolved against the program's `@base` and `@prefix` declarations.
    ///
    /// Returns `None` for a name the program does not use.
    pub fn predicate(&self, name: &Identifier) -> Option<&[Type]> {
        let name = self.scope.resolve(name, None).ok()?;
        self.predicates.get(&name).map(Vec::as_slice)
    }

    /// Returns the variable types of the rules, in program order:
    /// one entry per rule as written, with a source position.
    pub fn rules(&self) -> &[RuleTypes] {
        &self.rules
    }
}

/// Returns the variable types of a compiled program's rules, one entry per source position.
///
/// The stages may compile one written rule into several,
/// each holding the variable the others hold under the same name,
/// so a variable's type is the union of its types in all of them.
fn rule_types(compiled: &CompiledProgram) -> Vec<RuleTypes> {
    let mut rules: Vec<(Span, Vec<(String, Type)>)> = Vec::new();
    let sources = compiled.names.iter().zip(&compiled.spans);
    for ((names, &span), columns) in sources.zip(&compiled.rule_columns) {
        // A rule that fetches from an endpoint has no columns to report.
        let Some(span) = span.filter(|_| !columns.is_empty()) else {
            continue;
        };
        let position = match rules.iter().position(|(other, _)| *other == span) {
            Some(position) => position,
            None => {
                rules.push((span, Vec::new()));
                rules.len() - 1
            }
        };
        let variables = &mut rules[position].1;
        for (name, datatype) in names.iter().zip(columns) {
            let Some(name) = name else {
                continue;
            };
            match variables.iter_mut().find(|(other, _)| other == name) {
                Some((_, known)) => *known = known.union(datatype),
                None => variables.push((name.clone(), datatype.clone())),
            }
        }
    }
    rules
        .into_iter()
        .map(|(span, variables)| RuleTypes::new(span, variables))
        .collect()
}
