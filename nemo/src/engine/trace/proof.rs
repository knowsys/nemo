//! Proof search: a grounded backward search for derivations over the stored tables.
//!
//! The search first asks the fact's table at which revision it added the fact.
//! If that revision belongs to loaded data, the fact is a leaf.
//! Otherwise, the application log names the rule application of that step, which wrote the
//! fact.
//! The rule's body is grounded by the fact's values and evaluated again,
//! as an ordinary operation plan over the data from before the application.
//! Each row that this evaluation finds is a *witness*,
//! with a value for every variable of the rule,
//! and the premises of a witness are proven in turn.
//! The premises of a witness were all added before the fact,
//! so the search never meets a cycle, and it can remember every outcome.
//!
//! The submodules:
//!
//! - [search] finds proofs over the compiled program.
//! - `fetch` proves the facts that a bound import fetched from an endpoint.
//! - [lift] maps each proof onto the written program.
//! - [response] converts the lifted proofs to the types of [crate::trace].
//! - [parts] traces every row of some predicates, and hands the response out in parts.

mod fetch;
mod lift;
mod parts;
mod response;
mod search;

pub(crate) use response::{Responder, written_rule};

use lift::{LiftedGraph, Lifter};

use std::{collections::HashMap, rc::Rc};

use nemo_physical::{
    diagnostics::Diagnostic, management::TableId, storage::StorageValue, value::DataValue,
};

use crate::{
    engine::{Engine, compile::compiled::CompiledRule},
    pipeline::StatementId,
    trace::{TracePart, TraceResponse, TracedFact, VERSION},
};

/// One proven fact, with how it holds.
#[derive(Debug)]
struct ProofNode {
    /// The fact's table.
    table: TableId,
    /// The fact's encoded row.
    row: Vec<StorageValue>,
    /// The explanation.
    proof: Explanation,
}

/// How a fact holds.
#[derive(Debug)]
enum Explanation {
    /// The fact was loaded, not derived.
    Loaded,
    /// The fact follows from the premises by a rule instance.
    Derived {
        /// The index of the rule in the compiled program.
        rule: usize,
        /// The engine step of the application that derived the fact,
        /// or the revision at which the fact was added if the application log has no entry
        /// for it.
        step: u64,
        /// The witness, which is the rule's combined row,
        /// with the variables in the rule's order and then the computed columns.
        ///
        /// It is empty for an aggregate node without a witness row.
        witness: Vec<StorageValue>,
        /// The positive premises.
        premises: Vec<Rc<ProofNode>>,
        /// For an aggregate rule, the number of distinct contributions
        /// the group's first aggregate ranged over.
        matches: Option<usize>,
        /// Whether a head column calls a nondeterministic function, such as `RAND()`,
        /// whose value the search cannot reproduce.
        nondeterministic: bool,
    },
}

/// A trace in batches, which keeps what one batch of facts has in common with the next.
///
/// It keeps the facts proven, the facts lifted with their nodes, and the rules listed,
/// so that the batches add up to one response in which each fact is proven and listed once.
#[derive(Debug, Default)]
struct BatchedTrace {
    /// The outcome of every fact that the search met.
    outcomes: search::Outcomes,
    /// The facts lifted, with their nodes.
    graph: LiftedGraph,
    /// The position in the response of each rule listed so far, by its statement.
    listed: HashMap<StatementId, usize>,
}

/// What one batch of facts adds to a trace response.
struct Batch {
    /// The nodes and rules the batch adds.
    part: TracePart,
    /// Each fact of the batch, with its node.
    facts: Vec<TracedFact>,
}

impl BatchedTrace {
    /// Traces a batch of facts, given by table and decoded values.
    fn batch(
        &mut self,
        engine: &mut Engine,
        facts: &[(TableId, Vec<DataValue>)],
    ) -> Result<Batch, Diagnostic> {
        let mut proofs: Vec<Option<Rc<ProofNode>>> = Vec::with_capacity(facts.len());
        {
            let mut search = search::ProofSearch::over(engine, &mut self.outcomes)?;
            for (table, values) in facts {
                proofs.push(search.prove(*table, values)?);
            }
        }

        let roots: Vec<Option<usize>> = {
            let lifter = Lifter::new(engine);
            proofs
                .iter()
                .map(|proof| {
                    proof
                        .as_ref()
                        .map(|node| lifter.lift(node, &mut self.graph))
                })
                .collect()
        };
        let canonical_constants = engine.render_constants(self.graph.constants());

        let responder = Responder::new(engine, canonical_constants);
        let part = responder.part(&mut self.graph, &mut self.listed);
        Ok(Batch {
            part,
            facts: responder.traced(facts, &roots),
        })
    }
}

/// Traces the resolved facts against the engine's stores, and returns the response.
pub(crate) fn run(
    engine: &mut Engine,
    facts: Vec<(TableId, Vec<DataValue>)>,
) -> Result<TraceResponse, Diagnostic> {
    let Batch { part, facts } = BatchedTrace::default().batch(engine, &facts)?;
    Ok(TraceResponse {
        version: VERSION,
        facts,
        nodes: part.nodes,
        rules: part.rules,
    })
}

/// Returns whether a rule writes into the table,
/// through an ordinary head, its aggregate head, or an existential insert.
pub(super) fn targets(rule: &CompiledRule, table: TableId) -> bool {
    rule.heads.iter().any(|head| head.table == table)
        || rule
            .aggregate
            .as_ref()
            .is_some_and(|aggregate| aggregate.table == table)
        || rule.existential.as_ref().is_some_and(|existential| {
            existential
                .inserts
                .iter()
                .any(|insert| insert.table == table)
        })
}
