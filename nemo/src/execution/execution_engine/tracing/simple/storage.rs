//! This module defines [ExecutionTrace] which holds traces for relevant facts
//! that were computed during the "simple" tracing

use std::hash::Hash;

use indexmap::{IndexSet, set::MutableValues};

use crate::{
    execution::planning::normalization::atom::ground::GroundAtom,
    rule_model::{
        pipeline::id::ProgramComponentId, programs::handle::ProgramHandle,
        substitution::Substitution,
    },
};

/// Represents the application of a rule to derive a specific fact
#[derive(Debug)]
pub struct TraceRuleApplication {
    /// [ProgramComponentId] identifying the rule that corresponds
    /// to the normalized rule of this applicatin
    rule_id: ProgramComponentId,
    /// Variable assignment used during the rule application
    assignment: Substitution,
    /// Index of the head atom which produced the fact under consideration
    position: usize
}

impl TraceRuleApplication {
    /// Create new [TraceRuleApplication].
    pub fn new(rule_id: ProgramComponentId, assignment: Substitution, _position: usize) -> Self {
        Self {
            rule_id,
            assignment,
            _position,
        }
    }

    /// Return the [ProgramComponentId] of the rule used in this application.
    pub fn rule(&self) -> ProgramComponentId {
        self.rule_id
    }

    /// Return the [Substitution] of this application.
    pub fn assignment(&self) -> &Substitution {
        &self.assignment
    }

    /// Return the index of the head atom of this application.
    pub fn head_index(&self) -> usize {
        self._position
    }
}

/// Handle to a traced fact within an [ExecutionTrace].
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct TraceFactHandle(usize);

/// Encodes the origin of a fact
#[derive(Debug)]
pub enum TraceDerivation {
    /// Fact was part of the input to the chase
    Input,
    /// Fact was derived during the chase
    Derived(TraceRuleApplication, Vec<TraceFactHandle>),
}

/// Encodes current status of the derivation of a given fact
#[derive(Debug)]
pub enum TraceStatus {
    /// It is not yet known whether this fact derived during chase
    Unknown,
    /// Fact was derived during the chase with the given [TraceDerivation]
    Success(TraceDerivation),
    /// Fact was not derived during the chase
    Fail,
}

impl TraceStatus {
    /// Return true when fact was successfully derived
    /// and false otherwise.
    pub fn is_success(&self) -> bool {
        matches!(self, TraceStatus::Success(_))
    }

    /// Return true if it has already been decided whether
    /// a given fact has been derived and false otherwise.
    pub fn is_known(&self) -> bool {
        !matches!(self, TraceStatus::Unknown)
    }
}

/// Fact which was considered during the construction of an [ExecutionTrace]
#[derive(Debug)]
pub struct TracedFact {
    /// The considered fact
    fact: GroundAtom,
    /// Its current status with respect to its derivability in the chase
    status: TraceStatus,
}

impl PartialEq for TracedFact {
    fn eq(&self, other: &Self) -> bool {
        self.fact == other.fact
    }
}

impl Eq for TracedFact {}

impl Hash for TracedFact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fact.hash(state);
    }
}

impl TracedFact {
    /// Return the [TraceStatus] of this fact.
    pub fn status(&self) -> &TraceStatus {
        &self.status
    }

    /// Return the underlying [GroundAtom] for this fact.
    pub fn fact(&self) -> &GroundAtom {
        &self.fact
    }
}

/// Structure for recording execution traces
/// while not recomputing traces for the same facts occurring multiple times
#[derive(Debug)]
pub struct ExecutionTrace {
    /// Program over which the trace is computed
    program_handle: ProgramHandle,

    /// All the facts considered during tracing
    facts: IndexSet<TracedFact>,
}

impl ExecutionTrace {
    /// Create a new [ExecutionTrace].
    pub fn new(program_handle: ProgramHandle) -> Self {
        Self {
            program_handle,
            facts: IndexSet::default(),
        }
    }

    /// Return the [ProgramHandle] of the program
    /// over which the traces were computed.
    pub fn program(&self) -> &ProgramHandle {
        &self.program_handle
    }

    /// Given a [TraceFactHandle] return a reference to the corresponding [TracedFact].
    ///
    /// # Panics
    /// Panics if [TraceFactHandle] does not point to a valid [TracedFact].
    /// This cannot happen if the [TraceFactHandle] was obtained by this [ExecutionTrace].
    pub fn get_fact(&self, handle: TraceFactHandle) -> &TracedFact {
        self.facts.get_index(handle.0).expect("invalid fact handle")
    }

    /// Given a [TraceFactHandle] return a mutable reference to the corresponding [TracedFact].
    ///
    /// # Panics
    /// Panics if [TraceFactHandle] does not point to a valid [TracedFact].
    /// This cannot happen if the [TraceFactHandle] was obtained by this [ExecutionTrace].
    pub fn get_fact_mut(&mut self, handle: TraceFactHandle) -> &mut TracedFact {
        self.facts
            .get_index_mut2(handle.0)
            .expect("invalid fact handle")
    }

    /// Search for a given [GroundAtom], returning its [TraceFactHandle].
    ///
    /// Returns `None` if [GroundAtom] has not been registered yet.
    pub fn find_fact(&self, fact: &GroundAtom) -> Option<TraceFactHandle> {
        let traced_fact = TracedFact {
            fact: fact.clone(),
            status: TraceStatus::Unknown,
        };

        self.facts.get_index_of(&traced_fact).map(TraceFactHandle)
    }

    /// Registers a new [GroundAtom].
    ///
    /// If the fact was not already known then it will return a fresh handle
    /// with the status [TraceStatus::Unknown].
    /// Otherwise a handle to the existing fact will be returned.
    pub fn register_fact(&mut self, fact: GroundAtom) -> TraceFactHandle {
        TraceFactHandle(
            self.facts
                .insert_full(TracedFact {
                    fact,
                    status: TraceStatus::Unknown,
                })
                .0,
        )
    }

    /// Return the [TraceStatus] of a given fact identified by its [TraceFactHandle].
    ///
    /// # Panics
    /// Panics if [TraceFactHandle] does not point to a valid [TracedFact].
    /// This cannot happen if the [TraceFactHandle] was obtained by this [ExecutionTrace].
    pub fn status(&self, handle: TraceFactHandle) -> &TraceStatus {
        &self.get_fact(handle).status
    }

    /// Update the [TraceStatus] of a given fact identified by its [TraceFactHandle].
    ///
    /// # Panics
    /// Panics if [TraceFactHandle] does not point to a valid [TracedFact].
    /// This cannot happen if the [TraceFactHandle] was obtained by this [ExecutionTrace].
    pub fn update_status(&mut self, handle: TraceFactHandle, status: TraceStatus) {
        self.get_fact_mut(handle).status = status;
    }
}
