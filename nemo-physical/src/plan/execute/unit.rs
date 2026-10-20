//! Units: running the joins of a unit's members and materializing the unit's rows.

use crate::{
    dictionary::Dictionary,
    execution::{
        Cells, JoinShape, JoinTemplate, PendingRows, ProjectingSink, ProjectionScratch,
        RowEvaluator, RowSink, RowStageShape, SubtractedShape, TrieJoin,
    },
    plan::{
        execute::{
            Execution, Materialized, StepContext,
            combination::{JoinBlocks, SequentialJoin},
            results::Accounting,
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
    /// Each member's join runs once per combination of its inputs' blocks, all into one set of runs.
    /// Computed values are added to the dictionary once every member has run.
    pub(super) fn unit(&self, unit: &UnitLayout, context: &mut StepContext<'_>) -> Materialized {
        let materialization = self.plan_layout.materializations[unit.top.index()]
            .as_ref()
            .expect("units materialize their tops");
        let mut rows = UnitRows::new(materialization.produced.len());
        for member in &unit.members {
            let blocks = self.member_blocks(member);
            rows.run_member(member, &blocks, context);
        }
        let runs = rows.finish(context);

        self.materialize(unit.top, runs, &mut context.accounting)
    }

    /// Returns the blocks a member's join reads: those of its inputs and of its subtracted relations.
    fn member_blocks(&self, member: &MemberLayout) -> JoinBlocks<'_> {
        let mut inputs = Vec::with_capacity(member.inputs.len());
        for &required in &member.inputs {
            let mut blocks = Vec::new();
            self.blocks(required, &mut blocks);
            inputs.push(blocks);
        }

        let mut subtracted = Vec::new();
        let mut subtracted_shapes = Vec::new();
        for relation in &member.subtracted {
            let first = subtracted.len();
            self.blocks(relation.order, &mut subtracted);
            subtracted_shapes.extend((first..subtracted.len()).map(|_| SubtractedShape {
                layers: relation.layers.clone(),
            }));
        }

        JoinBlocks {
            inputs,
            subtracted,
            subtracted_shapes,
        }
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

    /// Runs a member's join once per combination of its inputs' blocks,
    /// in cells if a combination is large enough, and takes its rows as the member's delivery says.
    ///
    /// Each block of a subtracted relation is subtracted on its own,
    /// so a row matching any of them is dropped, as matching the union of the blocks drops it.
    fn run_member(
        &mut self,
        member: &'a MemberLayout,
        blocks: &JoinBlocks<'_>,
        context: &mut StepContext<'_>,
    ) {
        let dictionary: &Dictionary = context.dictionary;
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
            subtracted: &blocks.subtracted_shapes,
            filters: &member.filters,
            bound_layers: &member.bound_layers,
        };
        let template = JoinTemplate {
            shape,
            subtracted: &blocks.subtracted,
            dictionary,
            evaluation: context.evaluation,
        };
        let mut sequential = SequentialJoin::new(template, context.arena);
        let mut scratch = None;

        for tries in blocks.combinations() {
            match Cells::partition(&shape, &tries) {
                Some(cells) => {
                    self.run_cells(&cells, &tries, &member.delivery, template, accounting);
                }
                None => self.run_sequentially(
                    &mut sequential,
                    &tries,
                    &member.delivery,
                    template,
                    &mut scratch,
                    accounting,
                ),
            }
            accounting.counts.combinations += 1;
        }
    }

    /// Runs the join of one combination in cells and takes their rows as the delivery says.
    fn run_cells<'j>(
        &mut self,
        cells: &Cells,
        tries: &[&'j Trie],
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
                let rows = in_cells(cells, template, tries, drain, accounting);
                self.push_cells(rows, accounting);
            }
            Delivery::RowStage(stage) => {
                let collect = |join: &mut TrieJoin<'_>| evaluated(stage, join, &template);
                let collected = in_cells(cells, template, tries, collect, accounting);
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
                let rows = in_cells(cells, template, tries, drain, accounting);
                self.push_cells(rows, accounting);
            }
        }
    }

    /// Adds the sorted rows of the cells of a combination to the runs,
    /// each a run of its own, in cell order.
    fn push_cells(&mut self, cells: Vec<SortedRowBuffer>, accounting: &mut Accounting) {
        for rows in cells {
            accounting.see_buffer(rows.memory_bytes());
            self.runs.push_cell(rows);
        }
    }

    /// Runs the join of one combination sequentially and takes its rows as the delivery says.
    ///
    /// The scratch of a projection is created on its first use and kept across the combinations.
    fn run_sequentially<'j>(
        &mut self,
        sequential: &mut SequentialJoin<'j>,
        tries: &[&'j Trie],
        delivery: &'a Delivery,
        template: JoinTemplate<'j>,
        scratch: &mut Option<ProjectionScratch>,
        accounting: &mut Accounting,
    ) {
        match delivery {
            Delivery::Direct => {
                sequential.run(tries, accounting, |join| {
                    join.drain(self.runs.begin_segment());
                });
            }
            Delivery::RowStage(stage) => {
                let rows =
                    sequential.run(tries, accounting, |join| evaluated(stage, join, &template));
                self.pending.push((stage, rows));
            }
            Delivery::Sink(projection) => {
                let scratch = scratch.get_or_insert_with(|| ProjectionScratch::new(projection));
                sequential.run(tries, accounting, |join| {
                    let out = self.runs.begin_segment();
                    join.drain(&mut ProjectingSink::new(projection, scratch, out));
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

/// Runs the cells of a combination, each through `run` on its join,
/// accounts for their time and evaluation errors,
/// and returns what each cell delivered, in cell order.
fn in_cells<T: Send>(
    cells: &Cells,
    template: JoinTemplate<'_>,
    tries: &[&Trie],
    run: impl Fn(&mut TrieJoin<'_>) -> T + Sync,
    accounting: &mut Accounting,
) -> Vec<T> {
    let begun = Stopwatch::start();
    let (delivered, errors) = cells.run(template, tries, run);
    accounting.timings.join += begun.nanos();
    accounting.errors.absorb(&errors);

    delivered
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
