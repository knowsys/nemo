//! Proof search: grounded backward derivation over the stored tables.
//!
//! The search first finds the earliest revision block of the fact's table that holds the fact.
//! If that block was loaded (extensional), the fact is a leaf.
//! Otherwise, the application log names the rule applications that wrote the block.
//! For each of them, the rule body is grounded by the fact's values,
//! restricted to the data from before the application,
//! and evaluated again as an ordinary operation plan to find a witness.
//! The witness's premises are then proven in turn.
//!
//! A block is the finest revision range the tables still distinguish after consolidation,
//! so a premise can lie in the same block as the fact it supports.
//! The search therefore guards against cycles along the current path,
//! and backtracks across witnesses and candidates.
//! Any tree the search returns is a valid proof,
//! and the true derivation is always in the search space.
//!
//! The submodules:
//!
//! - [search] finds proofs over the compiled program.
//! - [fetch] proves facts fetched from an endpoint by a bound import.
//! - [lift] maps each proof onto the written program.
//! - [response] converts it to the [crate::trace] protocol types.
//! - [pages] traces a predicate's rows one page at a time.

#[cfg(feature = "sparql")]
mod fetch;
mod lift;
mod pages;
mod response;
mod search;

pub(crate) use response::{Responder, written_rule};

use lift::{LiftedNode, Lifter};

use std::rc::Rc;

use nemo_physical::{
    diagnostics::Diagnostic, management::TableId, storage::StorageValue, value::DataValue,
};

use crate::{
    engine::{Engine, compile::compiled::CompiledRule, evaluate::extensional::ExtensionalOrigin},
    trace::{TraceResponse, TracedFact, VERSION},
};

/// One proven fact with its explanation.
#[derive(Debug)]
struct ProofNode {
    /// The fact's table.
    table: TableId,
    /// The fact's encoded row.
    row: Vec<StorageValue>,
    /// The explanation.
    proof: Proof,
}

/// How a fact holds.
#[derive(Debug)]
enum Proof {
    /// The fact was loaded, not derived.
    Asserted {
        /// Where its rows came from.
        origin: ExtensionalOrigin,
    },
    /// The fact follows from the premises by a rule instance.
    Derived {
        /// The rule's index.
        rule: usize,
        /// The engine step of the producing application,
        /// or the end of the fact's revision block if the application log has no entry for it.
        step: u64,
        /// The witness: the rule's combined row
        /// (the variables in the rule's order, then computed columns),
        /// or empty for an aggregate node without a witness row.
        witness: Vec<StorageValue>,
        /// The positive premises.
        premises: Vec<Rc<ProofNode>>,
        /// For an aggregate rule, the number of distinct contributions
        /// the group's first aggregate ranged over.
        matches: Option<usize>,
        /// Whether a head column calls a nondeterministic function
        /// (such as `RAND()` or `STRUUID()`), whose value the search cannot reproduce.
        nondeterministic: bool,
    },
}

/// Traces the resolved facts against the engine's stores, returning the
/// protocol response.
pub(crate) fn run(
    engine: &mut Engine,
    facts: Vec<(TableId, Vec<DataValue>)>,
) -> Result<TraceResponse, Diagnostic> {
    let mut proofs: Vec<Option<Rc<ProofNode>>> = Vec::with_capacity(facts.len());
    {
        let mut search = search::ProofSearch::over(engine)?;
        for (table, values) in &facts {
            proofs.push(search.prove(*table, values)?);
        }
    }

    let lifted: Vec<Option<LiftedNode>> = {
        let lifter = Lifter::new(engine);
        proofs
            .iter()
            .map(|proof| proof.as_ref().map(|node| lifter.lift(node)))
            .collect()
    };
    let mut constants = Vec::new();
    for proof in lifted.iter().flatten() {
        proof.constants(&mut constants);
    }
    let canonical_constants = engine.render_constants(constants);

    let responder = Responder::new(engine, canonical_constants);
    let mut traced = Vec::with_capacity(facts.len());
    for ((table, values), proof) in facts.iter().zip(&lifted) {
        let fact = responder.fact(*table, values);
        traced.push(TracedFact {
            fact,
            holds: proof.is_some(),
            tree: proof.as_ref().map(|node| responder.node(node)),
        });
    }
    Ok(TraceResponse {
        version: VERSION,
        facts: traced,
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
