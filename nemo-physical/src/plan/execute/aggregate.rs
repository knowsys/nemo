//! Aggregations: materializing an aggregation node,
//! by counting its input's rows or in one pass over them.

use bumpalo::collections::Vec as BumpVec;

use crate::{
    dictionary::Dictionary,
    execution::{AggregationShape, Aggregator, JoinShape, TrieJoin},
    plan::{
        execute::{Execution, Materialized, StepContext, results::Accounting, runs::Runs},
        layout::{AggregateLayout, AggregatePass, ScanLayout},
    },
    stopwatch::Stopwatch,
    storage::StorageValue,
    trie::{RowBuffer, SortedRowBuffer, Trie},
};

impl Execution<'_> {
    /// Materializes an aggregation node, as its layout's pass computes it.
    pub(super) fn aggregate(
        &self,
        aggregate: &AggregateLayout,
        context: &mut StepContext<'_>,
    ) -> Materialized {
        let arena = context.arena;
        let mut blocks = BumpVec::new_in(arena.bump());
        self.blocks(aggregate.input, &mut blocks);

        let rows = match &aggregate.pass {
            &AggregatePass::CountRows { aggregates } => {
                Self::count_rows(&blocks, aggregates, &mut context.accounting)
            }
            AggregatePass::Scan(scan) => Self::scan(&blocks, scan, context),
        };

        self.materialize(aggregate.node, Runs::of(rows), &mut context.accounting)
    }

    /// Returns a single row with the number of distinct rows across the blocks
    /// in each of its `aggregates` columns, or no row for an empty input, as a scan does.
    ///
    /// A trie's rows are distinct,
    /// and the rows of several blocks are counted by a streaming merge, which builds nothing.
    fn count_rows(
        blocks: &[&Trie],
        aggregates: usize,
        accounting: &mut Accounting,
    ) -> SortedRowBuffer {
        let begun = Stopwatch::start();
        let count = match blocks {
            [] => 0,
            [block] => block.row_count(),
            several => Trie::count_distinct(several),
        };
        accounting.timings.join += begun.nanos();

        let mut buffer = RowBuffer::new(aggregates);
        if count > 0 {
            let count = i64::try_from(count).expect("row counts fit the integer value space");
            buffer.push_row(&vec![StorageValue::integer(count); aggregates]);
        }

        buffer.into_sorted()
    }

    /// Returns the groups an aggregating sink computes in one pass over the blocks, encoded.
    fn scan(blocks: &[&Trie], scan: &ScanLayout, context: &mut StepContext<'_>) -> SortedRowBuffer {
        let arity = scan.layers.len();

        let begun = Stopwatch::start();
        let shared: &Dictionary = context.dictionary;
        let mut aggregator = Aggregator::new(
            AggregationShape {
                group_layers: scan.group_layers,
                aggregates: &scan.aggregates,
            },
            arity,
            shared,
        );
        let shape = JoinShape {
            layers: &scan.layers,
            emitted: arity,
            subtracted: &[],
            filters: &[],
            bound_layers: &[],
        };
        let mut join = TrieJoin::new(
            shape,
            &[blocks],
            &[],
            Some(shared),
            context.evaluation,
            context.arena,
        );
        join.drain(&mut aggregator);
        // The join's borrow of the dictionary ends here, before the groups are encoded into it.
        drop(join);
        let groups = aggregator.finish();
        context.accounting.timings.join += begun.nanos();

        let begun = Stopwatch::start();
        let (buffer, counted) = groups.encode(context.dictionary);
        context.accounting.timings.encode += begun.nanos();
        context.accounting.errors.absorb(&counted);

        buffer
    }
}
