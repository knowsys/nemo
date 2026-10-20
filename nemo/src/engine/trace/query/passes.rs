//! The valid/filter two-pass over a shaped request.
//!
//! Bottom-up, each node's fact set is what its subtree's constraints allow.
//! Top-down, each set shrinks to what participates in a surviving parent derivation.

use std::ops::ControlFlow;

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{ColumnOrder, Revision, TableId},
    plan::{NodeId, OperationPlan},
    storage::StorageValue,
    trie::{RowBuffer, Trie},
};

use crate::{
    diagnostics::TraceError,
    engine::{
        compile::compiled::CompiledRule,
        evaluate::body::BodySpace,
        trace::query::{QueryEvaluation, request::CompiledNode},
    },
    trace::query::EntryQuery,
};

impl<'a> QueryEvaluation<'a> {
    /// Computes every node's valid set bottom-up, children first.
    pub(super) fn valid(
        &mut self,
        nodes: &mut [CompiledNode],
        index: usize,
    ) -> Result<(), Diagnostic> {
        if let Some((_, _, children)) = &nodes[index].expansion {
            for child in children.clone().into_iter().flatten() {
                self.valid(nodes, child)?;
            }
        }
        let selectors = self.selected(nodes, index)?;
        let valid = match nodes[index].expansion.clone() {
            // A leaf: the selectors intersected with the store, or the
            // whole predicate.
            None => match selectors {
                None => None,
                Some(selectors) => {
                    let (table, arity) = (nodes[index].table, nodes[index].arity);
                    Some(self.leaf_set(table, arity, selectors))
                }
            },
            // An inner node: the head projection of the rule's body over
            // the children's valid sets, intersected with the selectors.
            Some((rule_index, head_index, children)) => {
                let child_sets = self.child_sets(nodes, rule_index, &children);
                let arity = nodes[index].arity;
                Some(self.inner_set(arity, rule_index, head_index, &child_sets, selectors))
            }
        };
        nodes[index].valid = valid;
        Ok(())
    }

    /// Returns a node's selectors as an intermediate table of its arity,
    /// or `None` if the node is unconstrained.
    fn selected(
        &mut self,
        nodes: &[CompiledNode],
        index: usize,
    ) -> Result<Option<TableId>, Diagnostic> {
        let Some(rows) = self.selector_rows(nodes, index)? else {
            return Ok(None);
        };
        let arity = nodes[index].arity;
        let mut buffer = RowBuffer::new(arity);
        for row in &rows {
            buffer.push_row(row);
        }
        Ok(Some(self.store(arity, Trie::from_buffer(buffer))))
    }

    /// Returns a leaf's valid set: its selectors intersected with the store.
    fn leaf_set(&mut self, table: TableId, arity: usize, selectors: TableId) -> TableId {
        let mut plan = OperationPlan::new();
        let markers: Vec<_> = (0..arity).map(|_| plan.marker()).collect();
        let stored = plan.load(table, Revision(0)..Revision(self.now), markers.clone());
        let selected = plan.load(selectors, Revision(0)..Revision(1), markers.clone());
        let both = plan.join(vec![stored, selected], markers);
        let output = plan.output(both);
        let mut results = plan.execute(self.manager, self.dictionary, self.context);
        let rows = std::mem::take(&mut results.outputs[output]);
        self.store(arity, rows)
    }

    /// Returns an inner node's valid set:
    /// the head projection of the rule's body over the children's valid sets,
    /// intersected with the selectors if the node has any.
    fn inner_set(
        &mut self,
        arity: usize,
        rule_index: usize,
        head_index: usize,
        child_sets: &[Option<TableId>],
        selectors: Option<TableId>,
    ) -> TableId {
        let program = self.program;
        let rule = &program.rules[rule_index];
        let mut plan = OperationPlan::new();
        let (joined, space) = self.body_join(&mut plan, rule, child_sets);
        let head_markers = space.columns(&rule.heads[head_index].columns);
        let mut node = plan.project(joined, head_markers.clone());
        if let Some(selectors) = selectors {
            let selected = plan.load(selectors, Revision(0)..Revision(1), head_markers.clone());
            node = plan.join(vec![node, selected], head_markers);
        }
        let output = plan.output(node);
        let mut results = plan.execute(self.manager, self.dictionary, self.context);
        let rows = std::mem::take(&mut results.outputs[output]);
        self.store(arity, rows)
    }

    /// Shrinks every child's set top-down to what participates in a surviving parent
    /// derivation, then recurses.
    pub(super) fn filter(&mut self, nodes: &mut [CompiledNode], index: usize) {
        let Some((rule_index, head_index, children)) = nodes[index].expansion.clone() else {
            return;
        };
        let child_sets = self.child_sets(nodes, rule_index, &children);
        let program = self.program;
        let rule = &program.rules[rule_index];

        let mut plan = OperationPlan::new();
        let (mut joined, space) = self.body_join(&mut plan, rule, &child_sets);
        // The surviving parent facts constrain the witnesses.
        if let Some(parent) = nodes[index].filtered {
            let head_markers = space.columns(&rule.heads[head_index].columns);
            let survivors = plan.load(parent, Revision(0)..Revision(1), head_markers);
            joined = plan.join(vec![joined, survivors], space.result_columns.clone());
        }
        // Each requested child's facts are the witnesses' projection
        // onto its atom.
        let mut outputs = Vec::new();
        for (position, child) in children.iter().enumerate() {
            let Some(child) = child else { continue };
            let projected = plan.project(joined, space.atom_markers(&rule.body[position]));
            outputs.push((*child, plan.output(projected)));
        }
        let mut results = plan.execute(self.manager, self.dictionary, self.context);
        for (child, output) in outputs {
            let rows = std::mem::take(&mut results.outputs[output]);
            nodes[child].filtered = Some(self.store(nodes[child].arity, rows));
        }
        for child in children.into_iter().flatten() {
            self.filter(nodes, child);
        }
    }

    /// Returns the valid sets of an expansion's children, by body position,
    /// or `None` for an atom no child constrains.
    fn child_sets(
        &self,
        nodes: &[CompiledNode],
        rule_index: usize,
        children: &[Option<usize>],
    ) -> Vec<Option<TableId>> {
        let rule = &self.program.rules[rule_index];
        (0..rule.body.len())
            .map(|position| {
                children
                    .get(position)
                    .copied()
                    .flatten()
                    .and_then(|child| nodes[child].valid)
            })
            .collect()
    }

    /// Resolves a node's selectors into encoded rows, or `None` if the node is unconstrained.
    fn selector_rows(
        &mut self,
        nodes: &[CompiledNode],
        index: usize,
    ) -> Result<Option<Vec<Vec<StorageValue>>>, Diagnostic> {
        let node = &nodes[index];
        if node.selectors.is_empty() {
            return Ok(None);
        }
        let mut rows = Vec::with_capacity(node.selectors.len());
        for grounded in self.grounded.get(&index).into_iter().flatten() {
            let row: Option<Vec<StorageValue>> = grounded
                .iter()
                .map(|value| self.dictionary.encode(value.as_ref()))
                .collect();
            if let Some(row) = row {
                rows.push(row);
            }
        }
        for selector in &node.selectors {
            let EntryQuery::Entry(id) = selector else {
                continue;
            };
            match self.row_at(node.table, *id) {
                Some(row) => rows.push(row),
                None => {
                    return Err(TraceError::UnknownEntry {
                        predicate: self.manager.name(node.table).to_owned(),
                        id: *id,
                    }
                    .diagnostic());
                }
            }
        }
        Ok(Some(rows))
    }

    /// Returns the stored row at the given position of a predicate's
    /// enumeration in its own column order, which entry ids refer to.
    fn row_at(&mut self, table: TableId, id: usize) -> Option<Vec<StorageValue>> {
        let arity = self.manager.arity(table);
        let identity = ColumnOrder::identity(arity);
        let range = Revision(0)..Revision(self.now);
        self.manager.ensure_order(table, range.clone(), &identity);
        let mut found = None;
        self.manager
            .for_each_row_from(table, range, &identity, id, |row| {
                found = Some(row.to_vec());
                ControlFlow::Break(())
            });
        found
    }

    /// Returns the rule's body join over the given fact sets per atom,
    /// as one plan node with a column per variable, and the rule's columns as plan markers.
    ///
    /// Every atom reads its child's set if one constrains it, and its whole table otherwise.
    /// The negations hold against the whole store.
    /// A rule that can never fire matches nothing:
    /// the condition that proved it is folded away, not among its filters.
    pub(super) fn body_join(
        &self,
        plan: &mut OperationPlan,
        rule: &'a CompiledRule,
        child_sets: &[Option<TableId>],
    ) -> (NodeId, BodySpace<'a>) {
        let space = BodySpace::new(plan, rule);
        let mut inputs = Vec::with_capacity(rule.body.len());
        for (position, atom) in rule.body.iter().enumerate() {
            let input = if rule.never_fires {
                plan.load(
                    atom.table,
                    Revision(0)..Revision(0),
                    space.atom_markers(atom),
                )
            } else {
                let set = child_sets.get(position).copied().flatten();
                self.set_load(plan, atom.table, set, space.atom_markers(atom))
            };
            inputs.push(input);
        }
        let node = space.join(plan, inputs, space.order.clone(), &[]);
        let (node, _) = space.subtract_negations(plan, node, Revision(0)..Revision(self.now));
        (node, space)
    }
}
