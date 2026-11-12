//! The compiled program, and the compiled forms of its rules.
//!
//! The *rule columns* of a compiled rule are its variables, numbered from 0,
//! followed by its computed columns.
//! The parts of a compiled rule refer to the values of a body match by rule column.

mod aggregate;
mod body;
mod fetch;
mod head;
mod negation;
mod rule;

pub(crate) use self::{
    aggregate::{AggregateColumn, CompiledAggregate, CompiledAggregateFunction},
    body::{BodyAtom, ComputedColumn, FilterOrigin, RuleBinding, RuleFilter},
    fetch::{CompiledFetch, FetchColumn},
    head::{ExistentialAtom, ExistentialHead, ExistentialTerm, HeadAtom},
    negation::{NegatedAtom, NegatedColumn},
    rule::CompiledRule,
};

use std::collections::HashMap;

use nemo_physical::{management::TableId, value::Type};

use crate::{engine::ExportSpec, model::Span, pipeline::StatementId};

/// A rule column, which is a variable or, for a variable that an assignment binds,
/// the assignment's computed column.
///
/// The variables are numbered in the order in which the positive body atoms first bind them.
pub(crate) type VariableId = usize;

/// What a compiled rule is in the program: its statement, and what messages show of it.
#[derive(Debug, Clone)]
pub(crate) struct RuleSource {
    /// The rule's text, rendered from the rule.
    pub(crate) text: String,
    /// The pipeline statement that the rule was compiled from.
    pub(crate) statement: StatementId,
    /// The name of each rule column as written in the rule,
    /// or `None` for a column without a written name.
    pub(crate) names: Vec<Option<String>>,
    /// The rule's source position.
    pub(crate) span: Option<Span>,
}

/// The compiled program, which the engine evaluates.
///
/// The fields with one entry per rule (`sources` and `rule_columns`)
/// match [rules](Self::rules) by index.
#[derive(Debug)]
pub(crate) struct CompiledProgram {
    /// The rules, in program order.
    pub(crate) rules: Vec<CompiledRule>,
    /// The source of each rule.
    pub(crate) sources: Vec<RuleSource>,
    /// The strata in dependency order, each a list of rule indices.
    ///
    /// Every predicate that a stratum's rules negate is complete before the stratum is
    /// evaluated.
    pub(crate) strata: Vec<Vec<usize>>,
    /// The resolved export directives, in program order.
    pub(crate) exports: Vec<ExportSpec>,
    /// Per table, the types that each column can hold,
    /// as the [type analysis](crate::engine::compile::types) inferred them.
    pub(crate) column_types: HashMap<TableId, Vec<Type>>,
    /// Per rule, the types that each rule column can hold once the rule's conditions have
    /// narrowed them, as the type analysis inferred them.
    pub(crate) rule_columns: Vec<Vec<Type>>,
}
