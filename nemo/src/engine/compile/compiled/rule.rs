//! Compiled rules.

use crate::engine::compile::compiled::{
    BodyAtom, CompiledAggregate, CompiledFetch, ComputedColumn, ExistentialHead, HeadAtom,
    NegatedAtom, RuleBinding, RuleFilter, VariableId,
};

/// One rule, normalized for execution.
///
/// - The variables of a body atom are distinct: constants and repeated variables become
///   filters.
/// - Assignments, expressions in the head or in an aggregate, head constants and repeated
///   head variables become computed columns.
#[derive(Debug)]
pub(crate) struct CompiledRule {
    /// The number of variables.
    pub(crate) variables: usize,
    /// The variables in the order in which the rule's joins bind them,
    /// a permutation of the variables.
    ///
    /// [order](crate::engine::compile::order) chooses it.
    pub(crate) order: Vec<VariableId>,
    /// The positive body atoms.
    pub(crate) body: Vec<BodyAtom>,
    /// The filters over rule columns.
    pub(crate) filters: Vec<RuleFilter>,
    /// The body columns whose arguments are expressions over other columns.
    pub(crate) bindings: Vec<RuleBinding>,
    /// The computed columns.
    ///
    /// Computed column `k` is rule column `variables + k`.
    pub(crate) computed: Vec<ComputedColumn>,
    /// The negated body atoms.
    pub(crate) negated: Vec<NegatedAtom>,
    /// The head atoms.
    pub(crate) heads: Vec<HeadAtom>,
    /// The aggregate head, if the rule has one.
    pub(crate) aggregate: Option<CompiledAggregate>,
    /// The existential head, if the rule has `!`-variables.
    pub(crate) existential: Option<ExistentialHead>,
    /// Whether a constant condition proved that the rule never fires.
    pub(crate) never_fires: bool,
    /// Whether any of the rule's programs calls a nondeterministic function, such as `RAND()`.
    ///
    /// Such a rule must read [exact windows](Self::exact_windows),
    /// since reading a body row again would compute a new value,
    /// and deduplication against the head cannot remove a row with a fresh value.
    pub(crate) nondeterministic: bool,
    /// The engine step of the rule's last application, or 0 before the first.
    pub(crate) last_applied: u64,
    /// The fetch that the rule performs instead of a join,
    /// if the rule is compiled from a SPARQL import with a `where` clause.
    ///
    /// The body atoms are then the atoms of the `where` clause,
    /// and the head is the imported predicate.
    pub(crate) fetch: Option<CompiledFetch>,
}

impl CompiledRule {
    /// Returns whether the rule must read exact delta windows,
    /// rather than windows that start at a block boundary.
    ///
    /// This is the case for a rule that calls a nondeterministic function,
    /// since reading a row again would compute a new value.
    /// It is also the case for a rule that fetches, since reading a binding again costs a
    /// request.
    pub(crate) fn exact_windows(&self) -> bool {
        self.fetches() || self.nondeterministic
    }

    /// Returns whether the rule fetches from an endpoint instead of joining.
    pub(crate) fn fetches(&self) -> bool {
        self.fetch.is_some()
    }

    /// Creates a rule that does nothing.
    ///
    /// It keeps a rule index free for a rule that the translation compiles later.
    pub(crate) fn inert() -> Self {
        Self {
            variables: 0,
            order: Vec::new(),
            body: Vec::new(),
            filters: Vec::new(),
            bindings: Vec::new(),
            computed: Vec::new(),
            negated: Vec::new(),
            heads: Vec::new(),
            aggregate: None,
            existential: None,
            never_fires: true,
            nondeterministic: false,
            last_applied: 0,
            fetch: None,
        }
    }
}
