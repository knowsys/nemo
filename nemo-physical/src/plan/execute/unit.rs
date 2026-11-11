//! Units: running the joins of a unit's members and materializing the unit's rows.

use crate::{
    dictionary::Dictionary,
    execution::{
        Cells, JoinArena, JoinShape, JoinTemplate, PendingRows, ProjectingSink, ProjectionScratch,
        RowEvaluator, RowSink, RowStageShape, TrieJoin,
    },
    function::EvaluationContext,
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
    /// Each member's join reads every block of its inputs, and all members fill one set of runs.
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

/// The join of a member over the blocks of its inputs, as far as the blocks decide its rows.
#[derive(Debug, Clone, Copy)]
pub(super) enum MemberJoin<'j> {
    /// The member has no rows.
    Empty,
    /// The member has the empty row, since its join has no layers and every input has a row.
    EmptyRow,
    /// The member has the rows of the joins that the template builds over its inputs' blocks.
    Template(JoinTemplate<'j>),
}

impl<'j> MemberJoin<'j> {
    /// Returns the join of a member over the blocks of its inputs.
    ///
    /// The programs of the join decode their values through `dictionary`,
    /// and evaluate in `evaluation`.
    pub(super) fn over(
        member: &'j MemberLayout,
        blocks: JoinBlocks<'j>,
        dictionary: &'j Dictionary,
        evaluation: EvaluationContext,
    ) -> Self {
        // A row of a subtracted relation that shares no layer with the join matches every row,
        // so it drops them all.
        if blocks.uncompared.iter().any(|block| !block.is_empty()) {
            return Self::Empty;
        }

        // A join without layers reads only relations without columns.
        // Each has the empty row or nothing, so they are read without a join.
        if member.layers.is_empty() {
            let holds = blocks
                .inputs
                .iter()
                .all(|input| input.iter().any(|block| !block.is_empty()));
            return if holds { Self::EmptyRow } else { Self::Empty };
        }

        // The shape borrows the layout, so nothing is cloned or validated per execution.
        let shape = JoinShape {
            layers: &member.layers,
            emitted: member.emitted,
            subtracted: blocks.subtracted_shapes,
            filters: &member.filters,
            bound_layers: &member.bound_layers,
        };

        Self::Template(JoinTemplate {
            shape,
            subtracted: blocks.subtracted,
            dictionary,
            evaluation,
        })
    }
}

/// The rows a unit collects while the joins of its members run.
#[derive(Debug)]
struct UnitRows<'a> {
    /// The completed rows.
    ///
    /// Each segment and each cell is a sorted run of distinct rows,
    /// unless a sink left a sequential segment out of order,
    /// which one final sort of the buffer fixes.
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
    /// in cells if it is large enough, and collects its rows as the member's delivery says.
    ///
    /// Each block of a subtracted relation is subtracted on its own,
    /// which drops the rows that match any block, as subtracting the whole relation does.
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

        let template = match MemberJoin::over(member, blocks, dictionary, context.evaluation) {
            MemberJoin::Empty => return,
            MemberJoin::EmptyRow => {
                if let Delivery::RowStage(stage) = &member.delivery {
                    let mut evaluator =
                        RowEvaluator::new(stage, member.emitted, dictionary, context.evaluation);
                    evaluator.accept(&[], 0);
                    self.pending.push((stage, evaluator.finish()));
                } else {
                    self.runs.begin_segment().push_row(&[]);
                }
                return;
            }
            MemberJoin::Template(template) => template,
        };

        let inputs = blocks.inputs;
        match Cells::partition(&template.shape, inputs) {
            Some(cells) => self.run_cells(&cells, inputs, &member.delivery, template, accounting),
            None => self.run_sequentially(inputs, &member.delivery, template, arena, accounting),
        }
    }

    /// Runs a member's join in cells, and collects their rows as the delivery says.
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
                // Each cell sorts its own rows on its worker,
                // unless the projection keeps the leading layers in order,
                // which leaves the rows sorted.
                // The cells partition emitted layers,
                // so if the group prefix covers those, no two cells produce the same row.
                // Otherwise a row may repeat across cells, and the build drops the repeats.
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

    /// Runs a member's join sequentially, building it in the arena,
    /// and collects its rows as the delivery says.
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

                // The segment of a projection that keeps the leading layers in order
                // arrives sorted.
                // Any other projection leaves its rows sorted by the group prefix only,
                // and sorting within the groups, which are many and small,
                // makes the segment a run.
                // Without a group prefix there is no order to build on,
                // so the final sort covers the segment.
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
    /// The buffer is sorted if a sink left it out of order.
    /// No join borrows the dictionary at this point,
    /// so the rows awaiting their computed values are encoded.
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

        // Each collection is encoded into a segment of its own, which is a run,
        // since extending distinct, sorted rows keeps them distinct and sorted.
        for (stage, rows) in pending {
            let begun = Stopwatch::start();
            let errors = rows.encode(&stage.delivered, context.dictionary, runs.begin_segment());
            context.accounting.errors.absorb(&errors);
            context.accounting.timings.encode += begun.nanos();
        }

        runs
    }
}

/// Runs the cells of a join, applying `run` to each cell's join,
/// and returns the results in cell order.
///
/// The time and the evaluation errors of the cells are added to `accounting`.
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

/// Builds a join in the arena, applies `drain` to it, and returns the result.
///
/// The time of the join, its setup included, and its evaluation errors
/// are added to `accounting`.
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

/// Drains a join into the evaluator of a row stage, and returns the rows it kept.
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
