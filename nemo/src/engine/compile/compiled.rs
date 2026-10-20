//! The compiled forms rules execute in.

mod aggregate;
mod body;
#[cfg(feature = "sparql")]
mod fetch;
mod head;
mod negation;
mod rule;

#[cfg(feature = "sparql")]
pub(crate) use self::fetch::{CompiledFetch, FetchColumn};
pub(crate) use self::{
    aggregate::{AggregateColumn, CompiledAggregate, CompiledAggregateFunction},
    body::{BodyAtom, ComputedColumn, FilterOrigin, RuleBinding, RuleFilter},
    head::{ExistentialAtom, ExistentialHead, ExistentialTerm, HeadAtom},
    negation::{NegatedAtom, NegatedColumn},
    rule::CompiledRule,
};

use std::collections::HashMap;

use nemo_physical::{management::TableId, value::Type};

use crate::model::Span;

/// A rule variable.
///
/// Ids are assigned in first-occurrence order over the body, counting from 0.
pub(crate) type VariableId = usize;

/// The compiled program: rules over registered tables.
#[derive(Debug)]
pub(crate) struct CompiledProgram {
    /// The rules, in program order.
    pub(crate) rules: Vec<CompiledRule>,
    /// The rules' rendered texts, matching `rules` by index,
    /// used as the rules' labels in telemetry.
    pub(crate) texts: Vec<String>,
    /// The rules' statements in the pipeline, matching `rules` by index.
    pub(crate) statements: Vec<crate::pipeline::StatementId>,
    /// Per rule, the source name of each variable by variable id,
    /// or `None` for a column the normalization invented.
    pub(crate) names: Vec<Vec<Option<String>>>,
    /// The rules' source positions, matching `rules` by index.
    pub(crate) spans: Vec<Option<Span>>,
    /// The strata: rule indices grouped for evaluation, in dependency order.
    ///
    /// A stratum's negated predicates are final before it runs.
    pub(crate) strata: Vec<Vec<usize>>,
    /// The export directives, resolved, in directive order.
    pub(crate) exports: Vec<crate::engine::ExportSpec>,
    /// The types each table's columns can hold, as the type analysis
    /// inferred them.
    pub(crate) column_types: HashMap<TableId, Vec<Type>>,
    /// Per rule, the types each of its columns can hold once its conditions narrowed them,
    /// as the type analysis inferred them, matching `rules` by index.
    pub(crate) rule_columns: Vec<Vec<Type>>,
}
