//! Facts: translating a fact into an encoded row of its table,
//! with the checks, the encoding and the seeding that imported rows share.

use nemo_physical::{
    diagnostics::Diagnostic,
    management::TableId,
    storage::StorageValue,
    trie::RowBuffer,
    value::{DomainSet, Type, ValueDomain, ValueRef},
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
    /// Every value is checked against the declaration of its kept column, even a pruned one,
    /// and the values of the delivered columns are stored.
    pub(crate) fn fact(&mut self, fact: &Fact) -> Result<(), Diagnostic> {
        let positional = self.positional_fact(fact)?;
        let fact = positional.as_ref().unwrap_or(fact);
        let name = self.resolve(&fact.predicate, fact.span)?;
        let layout = self.row_layout(&name, fact.terms.len(), fact.span)?;
        let table = self.table(&fact.predicate, layout.delivered(), fact.span)?;

        let mut values = Vec::with_capacity(fact.terms.len());
        for (position, term) in fact.terms.iter().enumerate() {
            let value = self.evaluate(term)?;
            self.check_value(&name, position, value.as_ref(), term.span)?;
            values.push((value, term.span));
        }
        let values = layout.deliver(values);
        let arity = values.len();
        let mut row = Vec::with_capacity(arity);
        let mut column_types = Vec::with_capacity(arity);
        for (value, span) in values {
            column_types.push(Type::of_value(value.as_ref()));
            row.push(self.encode_value(value.as_ref(), span)?);
        }
        self.seed(table, &column_types);
        self.facts
            .entry(table)
            .or_insert_with(|| RowBuffer::new(arity))
            .push_row(&row);
        Ok(())
    }

    /// Checks a value of a fact or of an imported row at its kept column:
    /// its structures against the `@type` of their labels,
    /// and the value against the column's declaration.
    pub(super) fn check_value(
        &self,
        name: &str,
        position: usize,
        value: ValueRef<'_>,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        self.check_value_types(value, span)?;
        self.check_fact_declaration(name, position, value, span)
    }

    /// Returns whether [check_value](Self::check_value) accepts every value of a column's type,
    /// so that a block of rows need not check the column value by value.
    ///
    /// Only structures are checked against a `@type`,
    /// and a declaration admits every value of a type within it.
    pub(super) fn accepts_column(&self, name: &str, position: usize, column_type: &Type) -> bool {
        !column_type
            .domains()
            .intersects(DomainSet::TUPLE | DomainSet::MAP)
            && self
                .column_declaration(name, position)
                .is_none_or(|declared| column_type.is_subset(declared, &self.label_types))
    }

    /// Encodes a value for its table.
    ///
    /// A value that the dictionary cannot represent is an error.
    pub(super) fn encode_value(
        &mut self,
        value: ValueRef<'_>,
        span: Option<Span>,
    ) -> Result<StorageValue, Diagnostic> {
        self.dictionary
            .encode(value)
            .ok_or_else(|| TranslationError::Unrepresentable.at(span))
    }

    /// Adds the types of the values stored in a table to the types that the type analysis
    /// starts its columns from.
    pub(super) fn seed(&mut self, table: TableId, column_types: &[Type]) {
        let seeds = self
            .seeds
            .entry(table)
            .or_insert_with(|| vec![Type::EMPTY; column_types.len()]);
        for (seed, column_type) in seeds.iter_mut().zip(column_types) {
            *seed = seed.union(column_type);
        }
    }

    /// Returns the type that a predicate's declaration gives its kept column at `position`,
    /// if it has one.
    fn column_declaration(&self, name: &str, position: usize) -> Option<&Type> {
        self.declarations
            .get(name)
            .and_then(|declaration| declaration.types.get(position))
    }

    /// Returns the layout that maps the columns of a fact or a row with `arity` values to its
    /// table's columns.
    ///
    /// For a pruned predicate, these are the delivered columns, and otherwise every column.
    /// A row with as many values as a pruned predicate has delivered columns is an error,
    /// since its values would land in the wrong columns.
    pub(super) fn row_layout(
        &self,
        name: &str,
        arity: usize,
        span: Option<Span>,
    ) -> Result<ColumnLayout, Diagnostic> {
        let layout = self.plan.layout_of(name, arity);
        if layout.prunes() {
            return Ok(layout);
        }
        let kept = self.plan.stored_layout(name, arity).kept();
        if kept != arity {
            return Err(TranslationError::PrunedPredicateRows {
                name: name.to_owned(),
                written: kept.into(),
                stored: arity.into(),
            }
            .at(span));
        }
        Ok(layout)
    }

    /// Checks a fact's value against the declared type of its kept column,
    /// through every level of its structure.
    ///
    /// A value of a type that the declaration excludes is an error before anything runs.
    /// A tuple of the wrong shape is excluded like a string in an integer column.
    fn check_fact_declaration(
        &self,
        name: &str,
        position: usize,
        value: ValueRef<'_>,
        span: Option<Span>,
    ) -> Result<(), Diagnostic> {
        let Some(declared) = self.column_declaration(name, position) else {
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
