//! Proving a fact that a bound import's fetch rule stored.
//!
//! Such a fact is one solution that the endpoint returned
//! for a request built from one binding row per binding atom.
//! Each binding row is rebuilt from the fact's values at the bound columns,
//! the atom's constants, and some row added before the fetch that matches at the free columns.
//! These rows are the premises, and the endpoint is not queried again.

use std::rc::Rc;

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{Revision, TableId},
    plan::{FilterSpec, Marker, OperationPlan},
    storage::StorageValue,
};

use crate::engine::{
    compile::compiled::{CompiledFetch, FetchColumn},
    evaluate::body::{filtered, same_constant},
    trace::proof::{
        ProofNode,
        search::{CandidateApplication, ProofSearch},
    },
};

/// Returns the position in the stored row of each column of the query,
/// or `None` for a column the selection drops.
fn delivered_positions(fetch: &CompiledFetch) -> Vec<Option<usize>> {
    let written = fetch.query.width();
    let mut delivered: Vec<Option<usize>> = Vec::with_capacity(written);
    let mut next = 0;
    for position in 0..written {
        if fetch.selection.keeps(position) {
            delivered.push(Some(next));
            next += 1;
        } else {
            delivered.push(None);
        }
    }
    delivered
}

impl ProofSearch<'_> {
    /// Attempts to explain a fact by a fetch rule, as the module describes.
    pub(super) fn attempt_fetch(
        &mut self,
        candidate: &CandidateApplication<'_>,
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
        let program = self.program;
        let rule = &program.rules[candidate.rule];
        let fetch = rule
            .fetch
            .as_ref()
            .expect("a fetch candidate's rule carries its fetch");
        let Some(head) = rule
            .heads
            .first()
            .filter(|head| head.table == candidate.table)
        else {
            return Ok(None);
        };
        let delivered = delivered_positions(fetch);

        // The witness has every variable's value, from the fact and the binding rows,
        // in the order of the variables.
        let mut witness = vec![candidate.row[0]; rule.variables];
        for (&column, &value) in head.columns.iter().zip(candidate.row) {
            witness[column] = value;
        }
        let mut premises = Vec::with_capacity(rule.body.len());
        for (atom, columns) in rule.body.iter().zip(&fetch.atoms) {
            let Some(premise) = self.binding_premise(
                atom.table,
                columns,
                &delivered,
                candidate.row,
                candidate.step,
            )?
            else {
                return Ok(None);
            };
            for (column, &variable) in atom.variables.iter().enumerate() {
                witness[variable] = premise[column];
            }
            match self.search(atom.table, &premise)? {
                Some(node) => premises.push(node),
                None => return Ok(None),
            }
        }
        Ok(Some(candidate.derived(witness, premises, None, false)))
    }

    /// Returns the binding row of one binding atom behind the fact.
    ///
    /// The bound columns come from the fact, and the constants from the dictionary.
    /// The free columns come from a row added before the step that agrees with the other
    /// columns.
    /// Returns `None` if no row added before the step agrees,
    /// or if a constant of the atom is not in the dictionary.
    fn binding_premise(
        &mut self,
        table: TableId,
        columns: &[FetchColumn],
        delivered: &[Option<usize>],
        row: &[StorageValue],
        step: u64,
    ) -> Result<Option<Vec<StorageValue>>, Diagnostic> {
        let mut known: Vec<Option<StorageValue>> = Vec::with_capacity(columns.len());
        for column in columns {
            known.push(match column {
                FetchColumn::Bound(position) => delivered[*position].map(|at| row[at]),
                FetchColumn::Constant(value) => match self.dictionary.probe(value.as_ref()) {
                    Some(value) => Some(value),
                    // A constant that is not in the dictionary is in no row,
                    // so the atom never matched.
                    None => return Ok(None),
                },
                FetchColumn::Free => None,
            });
        }
        if known.iter().all(Option::is_some) {
            let row: Vec<StorageValue> = known
                .into_iter()
                .map(|value| value.expect("every entry was just checked to be known"))
                .collect();
            return Ok(self.added_before(table, &row, step).then_some(row));
        }
        self.any_row(table, &known, step)
    }

    /// Returns some row of a table that was added before the step
    /// and agrees with the columns that `known` has a value for.
    fn any_row(
        &mut self,
        table: TableId,
        known: &[Option<StorageValue>],
        step: u64,
    ) -> Result<Option<Vec<StorageValue>>, Diagnostic> {
        let mut plan = OperationPlan::new();
        let markers: Vec<Marker> = known.iter().map(|_| plan.marker()).collect();
        let end = self.aligned_end(table, step);
        let load = plan.load(table, Revision(0)..end, markers.clone());
        let mut filters = Vec::new();
        for (column, value) in known.iter().enumerate() {
            let Some(value) = value else { continue };
            let decoded = self.decode(std::slice::from_ref(value)).remove(0);
            filters.push(FilterSpec::from_program(
                vec![markers[column]],
                same_constant(decoded),
            ));
        }
        let node = filtered(&mut plan, load, filters);
        let output = plan.output(node);
        let context = self.contexts.at(step);
        let results = plan.execute(self.manager, self.dictionary, context);
        let mut found = None;
        results.outputs[output].for_each_row(|row| {
            if found.is_none() && self.added_before(table, row, step) {
                found = Some(row.to_vec());
            }
        });
        Ok(found)
    }
}
