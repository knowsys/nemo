//! Units: running the joins of a unit's members and materializing the unit's rows.

use crate::{
    dictionary::Dictionary,
    execution::{
        Cells, JoinArena, JoinShape, JoinTemplate, PendingRows, ProjectingSink, ProjectionScratch,
        RowEvaluator, RowSink, RowStageShape, TrieJoin,
    },
    plan::{
        execute::{
            Execution, Materialized, StepContext, blocks::JoinBlocks, results::Accounting,
            runs::Runs,
        },
        layout::{Delivery, MemberLayout, UnitLayout},
    },
    stopwatch::Stopwatch,
    trie::{RowBuffer, SortedRowBuffer, Trie},
};

impl Execution<'_> {
    /// Materializes a unit.
    ///
    /// Each member's join reads every block of its inputs, and all fill one set of runs.
    /// Computed values are added to the dictionary once every member has run.
    pub(super) fn unit(&self, unit: &UnitLayout, context: &mut StepContext<'_>) -> Materialized {
        let materialization = self.plan_layout.materializations[unit.top.index()]
            .as_ref()
            .expect("units materialize their tops");
        let mut rows = UnitRows::new(materialization.produced.len());
        let arena = context.arena;
        for member in &unit.members {
            let blocks = self.member_blocks(member, arena.bump());
            rows.run_member(member, blocks, context);
        }
        let runs = rows.finish(context);

        self.materialize(unit.top, runs, &mut context.accounting)
    }
}

/// The rows a unit collects while the joins of its members run.
#[derive(Debug)]
struct UnitRows<'a> {
    /// The finished rows.
    ///
    /// The runs arrive sorted and distinct, as segments and cells do,
    /// unless a sink leaves a sequential segment out of order;
    /// one final sort of the buffer covers that.
    runs: Runs,
    /// The rows awaiting their computed values, each with the row stage that collected them.
    pending: Vec<(&'a RowStageShape, PendingRows)>,
    /// Whether a sink left a segment of the buffer out of order.
    unsorted: bool,
}

impl<'a> UnitRows<'a> {
    /// Creates the rows of a unit before any member has run, of the given arity.
    fn new(arity: usize) -> Self {
        Self {
            runs: Runs::new(arity),
            pending: Vec::new(),
            unsorted: false,
        }
    }

    /// Runs a member's join over the blocks of its inputs,
    /// in cells if it is large enough, and takes its rows as the member's delivery says.
    ///
    /// Each block of a subtracted relation is subtracted on its own,
    /// so a row matching any of them is dropped, as matching the union of the blocks drops it.
    fn run_member(
        &mut self,
        member: &'a MemberLayout,
        blocks: JoinBlocks<'_>,
        context: &mut StepContext<'_>,
    ) {
        let dictionary: &Dictionary = context.dictionary;
        let arena = context.arena;
        let accounting = &mut context.accounting;
        accounting.counts.joins += 1;
        accounting.counts.blocks += blocks.count() as u64;

        // A relation without columns holds the empty row or nothing, and no join reads it.
        if member.layers.is_empty() {
            let holds = blocks.inputs[0].iter().any(|block| !block.is_empty());
            if let Delivery::RowStage(stage) = &member.delivery {
                let mut evaluator =
                    RowEvaluator::new(stage, member.emitted, dictionary, context.evaluation);
                if holds {
                    evaluator.accept(&[], 0);
                }
                self.pending.push((stage, evaluator.finish()));
            } else if holds {
                self.runs.begin_segment().push_row(&[]);
            }
            return;
        }

        // The shape borrows the layout, so nothing is cloned or validated per execution.
        let shape = JoinShape {
            layers: &member.layers,
            emitted: member.emitted,
            subtracted: blocks.subtracted_shapes,
            filters: &member.filters,
            bound_layers: &member.bound_layers,
        };
        let template = JoinTemplate {
            shape,
            subtracted: blocks.subtracted,
            dictionary,
            evaluation: context.evaluation,
        };

        let inputs = blocks.inputs;
        match Cells::partition(&shape, inputs) {
            Some(cells) => self.run_cells(&cells, inputs, &member.delivery, template, accounting),
            None => self.run_sequentially(inputs, &member.delivery, template, arena, accounting),
        }
    }

    /// Runs a member's join in cells and takes their rows as the delivery says.
    fn run_cells<'j>(
        &mut self,
        cells: &Cells,
        inputs: &[&[&'j Trie]],
        delivery: &'a Delivery,
        template: JoinTemplate<'j>,
        accounting: &mut Accounting,
    ) {
        let emitted = template.shape.emitted;
        match delivery {
            Delivery::Direct => {
                let drain = |join: &mut TrieJoin<'_>| {
                    let mut buffer = RowBuffer::new(emitted);
                    join.drain(&mut buffer);
                    buffer.assume_sorted()
                };
                let rows = in_cells(cells, template, inputs, drain, accounting);
                self.push_cells(rows, accounting);
            }
            Delivery::RowStage(stage) => {
                let collect = |join: &mut TrieJoin<'_>| evaluated(stage, join, &template);
                let collected = in_cells(cells, template, inputs, collect, accounting);
                self.pending
                    .extend(collected.into_iter().map(|rows| (stage, rows)));
            }
            Delivery::Sink(projection) => {
                // The cells partition emitted layers, so a projection whose group prefix
                // covers them never splits a group of rows across cells, and each cell
                // sorts its own rows on its worker where the projection is no identity.
                // Rows a projection without such a prefix repeats across cells are left to the build.
                let drain = |join: &mut TrieJoin<'_>| {
                    let mut buffer = RowBuffer::new(projection.columns.len());
                    let mut scratch = ProjectionScratch::new(projection);
                    join.drain(&mut ProjectingSink::new(
                        projection,
                        &mut scratch,
                        &mut buffer,
                    ));
                    if projection.group_prefix < projection.columns.len() {
                        buffer.into_sorted()
                    } else {
                        buffer.assume_sorted()
                    }
                };
                let rows = in_cells(cells, template, inputs, drain, accounting);
                self.push_cells(rows, accounting);
            }
        }
    }

    /// Adds the sorted rows of the cells of a join to the runs,
    /// each a run of its own, in cell order.
    fn push_cells(&mut self, cells: Vec<SortedRowBuffer>, accounting: &mut Accounting) {
        for rows in cells {
            accounting.see_buffer(rows.memory_bytes());
            self.runs.push_cell(rows);
        }
    }

    /// Runs a member's join sequentially, built in the arena,
    /// and takes its rows as the delivery says.
    fn run_sequentially<'j>(
        &mut self,
        inputs: &[&[&'j Trie]],
        delivery: &'a Delivery,
        template: JoinTemplate<'j>,
        arena: &'j JoinArena,
        accounting: &mut Accounting,
    ) {
        match delivery {
            Delivery::Direct => {
                sequentially(template, inputs, arena, accounting, |join| {
                    join.drain(self.runs.begin_segment());
                });
            }
            Delivery::RowStage(stage) => {
                let rows = sequentially(template, inputs, arena, accounting, |join| {
                    evaluated(stage, join, &template)
                });
                self.pending.push((stage, rows));
            }
            Delivery::Sink(projection) => {
                let mut scratch = ProjectionScratch::new(projection);
                sequentially(template, inputs, arena, accounting, |join| {
                    let out = self.runs.begin_segment();
                    join.drain(&mut ProjectingSink::new(projection, &mut scratch, out));
                });

                // The segment of an identity projection arrives sorted.
                // Any other leaves its rows in order by the group prefix alone,
                // and sorting within the groups, many and small, makes the segment a run.
                // Without a group prefix there is no order to build on,
                // and the final sort covers the segment.
                if projection.group_prefix < projection.columns.len() {
                    let begun = Stopwatch::start();
                    if projection.group_prefix == 0 {
                        self.unsorted = true;
                    } else {
                        self.runs.sort_last_segment(projection.group_prefix);
                    }
                    accounting.timings.merge += begun.nanos();
                }
            }
        }
    }

    /// Returns the runs once every member has run.
    ///
    /// The buffer is sorted if a sink left it out of order,
    /// and the rows awaiting their computed values are encoded,
    /// now that the dictionary is no longer shared.
    fn finish(self, context: &mut StepContext<'_>) -> Runs {
        let Self {
            mut runs,
            pending,
            unsorted,
        } = self;

        if unsorted {
            let begun = Stopwatch::start();
            runs.sort_buffer();
            context.accounting.timings.merge += begun.nanos();
        }

        // Each collection encodes into a segment of its own:
        // extending distinct, sorted rows keeps them a run.
        for (stage, rows) in pending {
            let begun = Stopwatch::start();
            let errors = rows.encode(&stage.delivered, context.dictionary, runs.begin_segment());
            context.accounting.errors.absorb(&errors);
            context.accounting.timings.encode += begun.nanos();
        }

        runs
    }
}

/// Runs the cells of a join, each through `run` on its join,
/// accounts for their time and evaluation errors,
/// and returns what each cell delivered, in cell order.
fn in_cells<T: Send>(
    cells: &Cells,
    template: JoinTemplate<'_>,
    inputs: &[&[&Trie]],
    run: impl Fn(&mut TrieJoin<'_>) -> T + Sync,
    accounting: &mut Accounting,
) -> Vec<T> {
    let begun = Stopwatch::start();
    let (delivered, errors) = cells.run(template, inputs, run);
    accounting.timings.join += begun.nanos();
    accounting.errors.absorb(&errors);

    delivered
}

/// Builds a join in the arena, runs it through `drain`,
/// and accounts for its time, its setup included, and its evaluation errors.
fn sequentially<'j, T>(
    template: JoinTemplate<'j>,
    inputs: &[&[&'j Trie]],
    arena: &'j JoinArena,
    accounting: &mut Accounting,
    drain: impl FnOnce(&mut TrieJoin<'j>) -> T,
) -> T {
    let begun = Stopwatch::start();
    let mut join = template.build(inputs, arena);
    accounting.timings.setup += begun.nanos();

    let drained = drain(&mut join);
    accounting.timings.join += begun.nanos();
    accounting.errors.absorb(&join.evaluation_errors());

    drained
}

/// Drains a join through a row stage, and returns the rows it kept.
fn evaluated(
    stage: &RowStageShape,
    join: &mut TrieJoin<'_>,
    template: &JoinTemplate<'_>,
) -> PendingRows {
    let emitted = template.shape.emitted;
    let mut evaluator = RowEvaluator::new(stage, emitted, template.dictionary, template.evaluation);
    join.drain(&mut evaluator);
    evaluator.finish()
}
