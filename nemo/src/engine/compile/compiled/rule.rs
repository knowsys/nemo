//! Compiled rules.

#[cfg(feature = "sparql")]
use crate::engine::compile::compiled::CompiledFetch;
use crate::engine::compile::compiled::{
    BodyAtom, CompiledAggregate, ComputedColumn, ExistentialHead, HeadAtom, NegatedAtom,
    RuleBinding, RuleFilter, VariableId,
};

/// One rule, normalized for execution.
///
/// Body atoms carry distinct variables (constants and repeated variables become filters),
/// and assignments, expressions in the head or in an aggregate, head constants and
/// repeated head variables become computed columns.
/// The rule's columns are its variables, then its computed columns.
#[derive(Debug)]
pub(crate) struct CompiledRule {
    /// Number of variables.
    pub(crate) variables: usize,
    /// The variables in the order the rule's joins bind them. Chosen
    /// by [order](crate::engine::compile::order), a permutation of the variables.
    pub(crate) order: Vec<VariableId>,
    /// The positive body atoms.
    pub(crate) body: Vec<BodyAtom>,
    /// The filter conditions over body variables.
    pub(crate) filters: Vec<RuleFilter>,
    /// Body columns bound to expressions over other columns, probed
    /// rather than enumerated during the join.
    pub(crate) bindings: Vec<RuleBinding>,
    /// The computed columns extending the join: assignments, expressions in the head or in an
    /// aggregate, head constants and duplicated head variables.
    ///
    /// Computed column `k` is the rule's column `variables + k`.
    pub(crate) computed: Vec<ComputedColumn>,
    /// The negated body atoms, prepared for subtraction.
    pub(crate) negated: Vec<NegatedAtom>,
    /// The head atoms, over the rule's columns.
    pub(crate) heads: Vec<HeadAtom>,
    /// The aggregate head, if the rule has one.
    pub(crate) aggregate: Option<CompiledAggregate>,
    /// The existential head, if the rule has `!`-variables.
    pub(crate) existential: Option<ExistentialHead>,
    /// Whether a constant condition proved the rule can never fire.
    pub(crate) never_fires: bool,
    /// Whether any of the rule's programs calls a nondeterministic function (such as `RAND()`).
    ///
    /// Such a rule must read *exact* delta windows:
    /// re-reading a body row would compute a new value,
    /// and head deduplication cannot remove a row that carries a fresh value.
    /// Its last application is therefore kept as a wall,
    /// a revision that consolidation does not merge across.
    pub(crate) nondeterministic: bool,
    /// The engine step of the rule's last application, or 0 before the first.
    pub(crate) last_applied: u64,
    /// The remote fetch this rule performs instead of a join,
    /// for a SPARQL import with a `where` clause.
    ///
    /// The body atoms are the atoms of the `where` clause, the head is the imported predicate.
    #[cfg(feature = "sparql")]
    pub(crate) fetch: Option<CompiledFetch>,
}

impl CompiledRule {
    /// Returns whether the rule must read exact delta windows
    /// rather than windows starting at a block boundary.
    ///
    /// This holds for a rule that calls a nondeterministic function
    /// (re-reading a row would compute a new value)
    /// and for a rule that fetches (re-reading a binding costs a request).
    pub(crate) fn exact_windows(&self) -> bool {
        #[cfg(feature = "sparql")]
        if self.fetch.is_some() {
            return true;
        }
        self.nondeterministic
    }

    /// Returns whether the rule fetches from an endpoint instead of joining.
    pub(crate) fn fetches(&self) -> bool {
        #[cfg(feature = "sparql")]
        if self.fetch.is_some() {
            return true;
        }
        false
    }

    /// Creates a rule that does nothing: a placeholder holding a rule index for
    /// one compiled later in the translation.
    #[cfg(feature = "sparql")]
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
