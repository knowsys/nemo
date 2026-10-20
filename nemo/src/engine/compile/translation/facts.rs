//! Facts: translating one into an encoded row of its table.

use nemo_physical::{
    diagnostics::Diagnostic,
    trie::RowBuffer,
    value::{Type, ValueDomain, ValueRef},
};

use crate::{
    diagnostics::{AboutPredicate, TranslationError, TypeError},
    engine::{compile::layout::ColumnLayout, compile::translation::Translator, compile::types},
    model::{Fact, Span},
    values::TypeSyntax,
};

impl<'a> Translator<'a> {
    /// Translates a fact into an encoded row of its table.
    ///
    /// Every value is checked at its column as written, a pruned one included,
    /// and the delivered ones are stored.
    /// Rows added through `import_rows` are checked here too.
    pub(crate) fn fact(&mut self, fact: &Fact) -> Result<(), Diagnostic> {
        let positional = self.positional_fact(fact)?;
        let fact = positional.as_ref().unwrap_or(fact);
        let name = self.resolve(&fact.predicate, fact.span)?;
        let layout = self.fact_layout(&name, fact)?;
        let table = self.table(&fact.predicate, layout.delivered(), fact.span)?;

        let mut values = Vec::with_capacity(fact.terms.len());
        for (position, term) in fact.terms.iter().enumerate() {
            let value = self.evaluate(term)?;
            self.check_value_types(value.as_ref(), term.span)?;
            self.check_fact_declaration(&name, position, value.as_ref(), term.span)?;
            values.push((value, term.span));
        }
        let values = layout.deliver(values);
        let arity = values.len();
        let mut row = Vec::with_capacity(arity);
        let mut column_types = Vec::with_capacity(arity);
        for (value, span) in values {
            column_types.push(Type::of_value(value.as_ref()));
            let encoded = self
                .dictionary
                .encode(value.as_ref())
                .ok_or_else(|| TranslationError::Unrepresentable.at(span))?;
            row.push(encoded);
        }
        let seeds = self
            .seeds
            .entry(table)
            .or_insert_with(|| vec![Type::EMPTY; arity]);
        for (position, column_type) in column_types.into_iter().enumerate() {
            seeds[position] = seeds[position].union(&column_type);
        }
        self.facts
            .entry(table)
            .or_insert_with(|| RowBuffer::new(arity))
            .push_row(&row);
        Ok(())
    }

    /// Returns the layout that takes a fact's columns, as written, to its table's columns:
    /// the delivered columns of a pruned predicate, every column otherwise.
    ///
    /// A row at the delivered width of a pruned predicate is an error,
    /// since its values would land in the wrong columns.
    fn fact_layout(&self, name: &str, fact: &Fact) -> Result<ColumnLayout, Diagnostic> {
        let layout = self.plan.layout_of(name, fact.terms.len());
        if layout.prunes() {
            return Ok(layout);
        }
        let kept = self.plan.stored_layout(name, fact.terms.len()).kept();
        if kept != fact.terms.len() {
            return Err(TranslationError::PrunedPredicateRows {
                name: name.to_owned(),
                written: kept.into(),
                stored: fact.terms.len().into(),
            }
            .at(fact.span));
        }
        Ok(layout)
    }

    /// Checks a fact's value against the declared type of its column as written,
    /// through every level of its structure.
    ///
    /// A value of a type the declaration excludes is an error before anything runs.
    /// A tuple of the wrong shape is excluded like a string in an integer column.
    fn check_fact_declaration(
        &self,
        name: &str,
        position: usize,
        value: ValueRef<'_>,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let Some(declared) = self
            .declarations
            .get(name)
            .and_then(|declaration| declaration.types.get(position))
        else {
            return Ok(());
        };
        if declared.admits(value, &self.label_types) {
            return Ok(());
        }
        let typed = Type::of_value(value);
        let mut problem = TypeError::FactOutsideDeclaration {
            name: name.to_owned(),
            column: position + 1,
            declared: TypeSyntax::of(declared).to_string(),
            value: TypeSyntax::of(&typed).to_string(),
        }
        .at(span)
        .with_predicate(name.to_owned());
        if !declared.domains().contains(ValueDomain::Tuple)
            && let Some(hint) = types::builtin_hint(&typed)
        {
            problem = problem.with_hint(hint);
        }
        Err(problem)
    }
}
