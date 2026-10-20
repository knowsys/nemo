//! Executing a plan.
//!
//! A plan is [laid out](PlanLayout) once and can then be executed many times.
//! An execution
//! 1. has the table manager build the column orders the loads read,
//! 2. runs the layout's steps in topological order, each materializing one node,
//! 3. hands over the tries of the declared outputs.
//!
//! A unit or an aggregation collects its rows as sorted runs
//! and builds one trie from them for each column order its consumers read.
//! The joins of a unit run once per combination of their inputs' blocks;
//! a combination too large for one thread is split into cells that run on the worker pool.

mod aggregate;
mod combination;
mod results;
mod runs;
mod unit;

pub use self::results::{PhaseTimings, PlanResults, WorkCounts};

use crate::{
    dictionary::Dictionary,
    execution::JoinArena,
    function::EvaluationContext,
    management::TableManager,
    parallel::{self, Tasks},
    plan::{
        NodeId, OperationPlan, PlanLayout,
        execute::{results::Accounting, runs::Runs},
        layout::{BlockSource, RequiredOrder, Step},
        node::Node,
    },
    stopwatch::Stopwatch,
    trie::Trie,
};

/// The state of one execution while its steps run.
#[derive(Debug)]
pub(crate) struct Execution<'a> {
    /// The plan.
    plan: &'a OperationPlan,
    /// The plan's layout.
    plan_layout: &'a PlanLayout,
    /// The tables, with every column order the loads read built.
    manager: &'a TableManager,
    /// Per node, its materialized result, filled in topological order.
    results: Vec<Option<Materialized>>,
}

/// What the materializations of an execution share:
/// the dictionary, the evaluation context, the arena of the sequential joins, and the accounting.
#[derive(Debug)]
struct StepContext<'a> {
    /// The dictionary, shared with the joins while they run and exclusive between them.
    dictionary: &'a mut Dictionary,
    /// The context the plan's programs evaluate in.
    evaluation: EvaluationContext,
    /// The arena the sequential joins are built in.
    arena: &'a JoinArena,
    /// The accounting of the execution.
    accounting: Accounting,
}

/// A materialized node: one trie per order of its materialization.
#[derive(Debug)]
struct Materialized {
    /// The tries, one per order of the node's materialization.
    tries: Vec<Trie>,
    /// For a declared output, the index of the trie built in the node's canonical order.
    output: Option<usize>,
}

impl<'a> Execution<'a> {
    /// Runs a plan by its layout; see [OperationPlan::execute].
    pub(crate) fn run(
        plan: &'a OperationPlan,
        plan_layout: &'a PlanLayout,
        manager: &'a mut TableManager,
        dictionary: &mut Dictionary,
        evaluation: EvaluationContext,
        arena: &JoinArena,
    ) -> PlanResults {
        let begun = Stopwatch::start();
        let mut context = StepContext {
            dictionary,
            evaluation,
            arena,
            accounting: Accounting::default(),
        };
        let mut execution = Self::new(plan, plan_layout, manager, &mut context.accounting);
        context.accounting.timings.order = begun.nanos();

        for (position, step) in plan_layout.steps.iter().enumerate() {
            let materialized = match step {
                Step::Unit(unit) => execution.unit(unit, &mut context),
                Step::Aggregate(aggregate) => execution.aggregate(aggregate, &mut context),
                Step::Concatenate { node, order } => {
                    execution.concatenated(*node, *order, &mut context)
                }
            };
            execution.results[step.node().index()] = Some(materialized);

            // The results this step was the last to read are done.
            for done in &plan_layout.released[position] {
                execution.results[done.index()] = None;
            }
        }

        let outputs = execution.into_outputs();
        context.accounting.timings.total = begun.nanos();

        context.accounting.into_results(outputs)
    }

    /// Prepares the execution of a plan:
    /// has the manager build every column order of a stored table the plan reads,
    /// and counts the arrangements built.
    ///
    /// This is the only phase of an execution that changes the manager.
    /// It panics as [OperationPlan::execute] does.
    fn new(
        plan: &'a OperationPlan,
        plan_layout: &'a PlanLayout,
        manager: &'a mut TableManager,
        accounting: &mut Accounting,
    ) -> Self {
        for (index, node) in plan.nodes().iter().enumerate() {
            let Node::Load {
                table,
                range,
                markers,
            } = node
            else {
                continue;
            };

            if !plan_layout.live[index] {
                continue;
            }
            assert_eq!(
                manager.arity(*table),
                markers.len(),
                "a load must cover the table's columns"
            );

            for sources in &plan_layout.sources[index] {
                for source in sources {
                    let BlockSource::Load { order, .. } = source else {
                        unreachable!("a load node's sources are loads");
                    };

                    let work = manager.ensure_order(*table, range.clone(), order);
                    accounting.counts.arrangements += work.arrangements as u64;
                    accounting.counts.arranged_rows += work.rows as u64;
                }
            }
        }

        Self {
            plan,
            plan_layout,
            manager,
            results: (0..plan.nodes().len()).map(|_| None).collect(),
        }
    }

    /// Returns the canonical tries of the declared outputs, in declaration order.
    fn into_outputs(mut self) -> Vec<Trie> {
        let plan = self.plan;

        plan.declared_outputs()
            .iter()
            .map(|output| {
                let mut materialized = self.results[output.index()]
                    .take()
                    .expect("declared outputs are materialized");
                let index = materialized
                    .output
                    .expect("declared outputs keep their canonical trie");

                materialized.tries.swap_remove(index)
            })
            .collect()
    }

    /// Collects the blocks of a node in one of its required orders,
    /// from the sources the layout resolved.
    fn blocks<'s>(&'s self, required: RequiredOrder, into: &mut Vec<&'s Trie>) {
        for source in &self.plan_layout.sources[required.node.index()][required.index] {
            match source {
                BlockSource::Load { node, table, order } => {
                    let Node::Load { range, .. } = self.plan.node(*node) else {
                        unreachable!("load sources name load nodes");
                    };
                    self.manager.tries_into(*table, range.clone(), order, into);
                }
                BlockSource::Materialized { node, trie } => {
                    let materialized = self.results[node.index()]
                        .as_ref()
                        .expect("nodes are computed before they are consumed");
                    into.push(&materialized.tries[*trie]);
                }
            }
        }
    }

    /// Materializes a load, alias or union declared as an output:
    /// its blocks merged into one trie in its canonical order.
    fn concatenated(
        &self,
        node: NodeId,
        order: RequiredOrder,
        context: &mut StepContext<'_>,
    ) -> Materialized {
        let accounting = &mut context.accounting;
        let canonical = self.plan.node_markers(node);

        let mut blocks = Vec::new();
        self.blocks(order, &mut blocks);

        let begun = Stopwatch::start();

        let trie = if blocks.is_empty() {
            Trie::empty(canonical.len())
        } else {
            Trie::merge(&blocks)
        };

        accounting.timings.merge += begun.nanos();
        accounting.counts.rows += trie.row_count() as u64;

        // Consumers read such a node as blocks, never as a trie;
        // the trie is the output's alone.
        Materialized {
            tries: vec![trie],
            output: Some(0),
        }
    }

    /// Builds a node's tries from its runs, one per order of its materialization,
    /// building the orders in parallel if they are large enough.
    fn materialize(&self, node: NodeId, runs: Runs, accounting: &mut Accounting) -> Materialized {
        accounting.counts.rows += runs.len() as u64;
        accounting.see_buffer(runs.largest_buffer_bytes());

        let begun = Stopwatch::start();

        let materialization = self.plan_layout.materializations[node.index()]
            .as_ref()
            .expect("materializing nodes have a materialization");

        let arity = runs.arity();
        let builds = &materialization.builds;

        let tries = runs.with_runs(|sorted| {
            let build = |index: usize| -> Trie {
                match &builds[index] {
                    Some(order) => {
                        Trie::from_sorted_buffer(&runs.reordered_sorted(order.indices()))
                    }
                    None if arity == 0 => Trie::nullary(runs.len() > 0),
                    None => Trie::from_runs(arity, sorted),
                }
            };

            parallel::map_tasks(Tasks::fixed(runs.len() * arity, builds.len()), build)
        });

        accounting.timings.build += begun.nanos();

        Materialized {
            tries,
            output: materialization.output,
        }
    }
}
