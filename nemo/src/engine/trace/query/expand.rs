//! Server-side expansion of a request.
//!
//! Each node is expanded by one written rule that derives the node's whole fact set,
//! and the rule's premises become the node's children, down to the depth bound.

use nemo_physical::{management::TableId, plan::OperationPlan};

use crate::engine::{
    compile::compiled::CompiledRule,
    trace::query::{
        MAX_NODES, QueryEvaluation,
        request::{NodeFacts, QueryArenas, QueryContext},
        route::Realization,
    },
};

/// Returns whether the query protocol may expand a rule, one deriving its head by a join.
///
/// Aggregate and existential rules are not expandable,
/// and neither is a bound import's fetch rule, whose facts were fetched from an endpoint.
fn expandable(rule: &CompiledRule) -> bool {
    #[cfg(feature = "sparql")]
    if rule.fetch.is_some() {
        return false;
    }
    rule.aggregate.is_none() && rule.existential.is_none()
}

impl QueryEvaluation<'_> {
    /// Expands a written node.
    ///
    /// The expansion chooses a written rule that derives the node's whole fact set,
    /// makes the rule's premises its children, computes their sets top-down, and recurses,
    /// until no rule derives the whole set, or the depth or node bound is reached.
    pub(super) fn expand(
        &mut self,
        arenas: &mut QueryArenas,
        context: &QueryContext<'_>,
        written: usize,
        depth: usize,
    ) {
        if depth == 0 || arenas.written.len() >= MAX_NODES {
            return;
        }
        let NodeFacts::Compiled(compiled) = arenas.written[written].facts else {
            return;
        };
        let (table, set) = (
            arenas.compiled[compiled].table,
            arenas.compiled[compiled].filtered,
        );
        let total = self.count(table, set);
        if total == 0 {
            return;
        }
        let predicate = arenas.written[written].predicate.clone();
        for &(statement, head) in context.writers(&predicate) {
            let covers = match context.router.realize(statement) {
                // An import that replaces the rule derives the whole set
                // if nothing else writes the predicate.
                Some(Realization::Import(_)) => context.writers(&predicate).len() == 1,
                Some(Realization::Rule(rule)) => self.covers(rule, head, table, set, total),
                None => false,
            };
            if !covers {
                continue;
            }
            if context.rule(statement).is_none() {
                continue;
            }
            let Ok(children) = arenas.expand(context, written, compiled, statement, head, &[])
            else {
                continue;
            };
            // The premises' sets are what participates in deriving this
            // node's set.
            self.filter(&mut arenas.compiled, compiled);
            for child in children {
                self.expand(arenas, context, child, depth - 1);
            }
            return;
        }
    }

    /// Returns whether a compiled rule derives a node's whole fact set,
    /// that is, whether the projection of its witnesses onto the head holds the whole set.
    fn covers(
        &mut self,
        rule_index: usize,
        head: usize,
        table: TableId,
        set: Option<TableId>,
        total: usize,
    ) -> bool {
        let program = self.program;
        let rule = &program.rules[rule_index];
        if !expandable(rule) {
            return false;
        }
        let unconstrained = vec![None; rule.body.len()];
        let mut plan = OperationPlan::new();
        let (joined, space) = self.body_join(&mut plan, rule, &unconstrained);
        let head_markers = space.columns(&rule.heads[head].columns);
        let set = self.set_load(&mut plan, table, set, head_markers.clone());
        let survivors = plan.join(vec![joined, set], space.result_columns.clone());
        let covered = plan.project(survivors, head_markers);
        let coverage = plan.output(covered);
        let results = plan.execute(self.manager, self.dictionary, self.context);
        results.outputs[coverage].row_count() == total
    }
}
