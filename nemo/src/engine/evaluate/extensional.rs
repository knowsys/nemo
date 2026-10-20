//! Which revision blocks of a table hold input facts rather than derived ones.
//!
//! Consolidation never merges these blocks with derived rows,
//! so recovery and reload can drop exactly the derived rows.

use std::{collections::HashSet, ops::Range};

use nemo_physical::management::{Revision, TableId};

use crate::{
    engine::{
        Engine,
        compile::compiled::{self, CompiledProgram},
    },
    telemetry::EngineEvent,
};

/// The origin of a block of extensional rows,
/// which tells what a [reload](Engine::reload) can and cannot reproduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtensionalOrigin {
    /// The program's own facts and imports, which the statements reproduce.
    Load,
    /// The rows added through [import_rows](Engine::import_rows),
    /// which no statement holds.
    ImportRows,
}

/// One block of extensional rows in a table's revision sequence.
///
/// The boundaries of these blocks are walls of every consolidation
/// (revisions no merge crosses),
/// so extensional rows never merge with derived ones,
/// and dropping the derived rows of a table drops whole revision ranges.
#[derive(Debug, Clone)]
pub(crate) struct ExtensionalBlock {
    /// The covered revisions.
    pub(crate) range: Range<Revision>,
    /// Where the rows came from.
    pub(crate) origin: ExtensionalOrigin,
}

impl Engine {
    /// Drops the derived rows that new input facts may invalidate, before reasoning continues.
    ///
    /// After input facts are added,
    /// derived facts that depend on them through negation or aggregation may no longer be
    /// derivable, so they are dropped and derived again.
    /// The rules to reset are found by a fixpoint over the rules:
    ///
    /// - A table *grows* if it gained input facts,
    ///   or if a positive body atom of a rule with the table as head reads a growing table,
    ///   since reasoning will derive new rows into it.
    /// - A rule is *reset* if a negated atom, or any body atom of an aggregating rule,
    ///   reads a growing or dropped table
    ///   (exactly the dependencies stratification calls negative).
    ///   It is also reset if any body atom reads a dropped table
    ///   (its old conclusions may depend on removed rows),
    ///   or if one of its head tables was dropped (its rows must be derived again).
    /// - Resetting a rule whose conclusions may be invalid drops the derived rows of its head
    ///   tables.
    ///   A rule reset only to derive a dropped head again keeps its heads,
    ///   since deriving the same rows twice is harmless.
    ///   The exception is a rule that calls a nondeterministic function (such as `RAND()`),
    ///   since running it again would add new values beside the old rows,
    ///   so its heads are dropped too.
    ///
    /// Reset rules return to `last_applied = 0`.
    /// Programs without negation or aggregation never drop a row.
    pub(super) fn recover(&mut self, program: &mut CompiledProgram) {
        if self.dirty.is_empty() {
            return;
        }
        let mut grown: HashSet<TableId> = std::mem::take(&mut self.dirty);
        let mut reset = vec![false; program.rules.len()];
        let mut dropped: HashSet<TableId> = HashSet::new();
        loop {
            let mut changed = false;
            for (index, rule) in program.rules.iter().enumerate() {
                if rule.body.iter().any(|atom| grown.contains(&atom.table)) {
                    for table in Self::head_tables(rule) {
                        changed |= grown.insert(table);
                    }
                }
                if reset[index] {
                    continue;
                }
                let invalidated = rule
                    .negated
                    .iter()
                    .any(|atom| grown.contains(&atom.table) || dropped.contains(&atom.table))
                    || (rule.aggregate.is_some()
                        && rule.body.iter().any(|atom| {
                            grown.contains(&atom.table) || dropped.contains(&atom.table)
                        }))
                    || rule.body.iter().any(|atom| dropped.contains(&atom.table));
                let owes = Self::head_tables(rule).any(|table| dropped.contains(&table));
                if !invalidated && !owes {
                    continue;
                }
                reset[index] = true;
                changed = true;
                if invalidated || rule.nondeterministic {
                    for table in Self::head_tables(rule) {
                        dropped.insert(table);
                    }
                }
            }
            if !changed {
                break;
            }
        }
        if dropped.is_empty() && !reset.contains(&true) {
            return;
        }

        let mut removed = 0usize;
        for &table in &dropped {
            removed += self.drop_derived(table);
        }
        self.derived = self.derived.saturating_sub(removed);
        for (rule, flag) in program.rules.iter_mut().zip(&reset) {
            if *flag {
                rule.last_applied = 0;
            }
        }
        if self.telemetry.enabled() {
            self.telemetry.record(&EngineEvent::Invalidated {
                tables: dropped.len() as u64,
                rows: removed as u64,
            });
        }
        tracing::debug!(
            tables = dropped.len(),
            rows = removed,
            "invalidated derived rows"
        );
    }

    /// Drops a table's derived rows, every revision outside its extensional blocks,
    /// returning the number of rows removed.
    ///
    /// The blocks' boundaries were walls of every consolidation,
    /// so no stored block crosses them.
    pub(crate) fn drop_derived(&mut self, table: TableId) -> usize {
        let end = Revision(self.step.max(1));
        let mut removed = 0;
        let mut cursor = Revision(0);
        let blocks: Vec<Range<Revision>> = self
            .extensional
            .get(&table)
            .into_iter()
            .flatten()
            .map(|block| block.range.clone())
            .collect();
        for block in blocks {
            if block.start > cursor {
                removed += self.manager.drop_rows(table, cursor..block.start);
            }
            cursor = block.end;
        }
        if end > cursor {
            removed += self.manager.drop_rows(table, cursor..end);
        }
        removed
    }

    /// Records a stored extensional batch at the given revision,
    /// coalescing with an adjacent block of the same origin.
    pub(crate) fn record_extensional(
        &mut self,
        table: TableId,
        revision: Revision,
        origin: ExtensionalOrigin,
    ) {
        let next = Revision(revision.0 + 1);
        let blocks = self.extensional.entry(table).or_default();
        if let Some(last) = blocks.last_mut()
            && last.range.end == revision
            && last.origin == origin
        {
            last.range.end = next;
            return;
        }
        blocks.push(ExtensionalBlock {
            range: revision..next,
            origin,
        });
    }

    /// Returns one rule's head tables:
    /// the ordinary heads, the aggregate head, and the existential insert atoms.
    pub(crate) fn head_tables(rule: &compiled::CompiledRule) -> impl Iterator<Item = TableId> + '_ {
        rule.heads
            .iter()
            .map(|head| head.table)
            .chain(rule.aggregate.iter().map(|aggregate| aggregate.table))
            .chain(
                rule.existential
                    .iter()
                    .flat_map(|existential| existential.inserts.iter().map(|insert| insert.table)),
            )
    }
}
