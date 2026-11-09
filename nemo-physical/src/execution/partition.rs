//! Partitioning: splitting a large join into cells that run in parallel.
//!
//! A cell joins the same inputs as the whole join,
//! with some of the leading layers clipped to a range of values.
//! Each cell runs on the worker pool, in its worker's arena,
//! and its rows form a run of their own.

use std::cell::RefCell;

use crate::{
    dictionary::Dictionary,
    execution::{JoinArena, JoinShape, JoinTemplate, TrieJoin},
    parallel::{self, Tasks},
    storage::StorageValue,
    trie::{ColumnView, Trie},
    value::ErrorCounts,
};

// The cells of a join share these across the worker threads, read only.
const _: () = {
    const fn sync<T: Sync>() {}
    sync::<Trie>();
    sync::<Dictionary>();
    sync::<JoinShape<'static>>();
};

/// The range of values one layer is clipped to in a cell.
#[derive(Debug, Clone, Copy)]
struct LayerClip {
    /// The layer.
    layer: usize,
    /// The lower bound, included, or `None` for no lower bound.
    low: Option<StorageValue>,
    /// The upper bound, excluded, or `None` for no upper bound.
    high: Option<StorageValue>,
}

/// One cell of a join: the clips of its partitioned layers.
type Cell = Vec<LayerClip>;

/// The cells a join splits into.
#[derive(Debug)]
pub(crate) struct Cells {
    /// The cells, in the lexicographic order of their ranges.
    ///
    /// If only the first layer is partitioned, the rows of the cells follow each other
    /// in storage order.
    /// A partitioned deeper layer splits the rows of one first-layer value across cells,
    /// so the rows of the cells then interleave.
    cells: Vec<Cell>,
    /// One task per cell, with the decision whether the tasks run on the pool.
    tasks: Tasks,
}

/// What one cell delivers: the result of `run` on its join,
/// and the evaluation errors the join counted.
#[derive(Debug)]
struct CellOutcome<T> {
    /// The result of `run` on the cell's join.
    rows: T,
    /// The evaluation errors the cell's join counted, by kind.
    errors: ErrorCounts,
}

impl Cells {
    /// Splits the join of a shape over the blocks of the given inputs into cells.
    ///
    /// Returns `None` if the join is too little work for the pool, or has no layer to split.
    /// The [work estimate](estimated_work) decides how many cells the split aims at.
    /// Layers are split in order, each into as many parts as its values allow,
    /// until the parts multiply to about that number.
    /// Each part of a deeper layer repeats the intersections above it,
    /// so splitting a deeper layer pays off only if a leading layer has few values
    /// above large subtrees.
    /// Only emitted layers are split, so that the cells produce disjoint rows, each once.
    /// A layer bound to a constant is not split.
    ///
    /// The boundary values of a layer split the longest block column intersected there
    /// into parts of equal length.
    /// Each cell's join clips its cursors to the cell's ranges,
    /// so the cells partition every cursor without copying.
    pub(crate) fn partition(shape: &JoinShape, inputs: &[&[&Trie]]) -> Option<Self> {
        let tasks = Tasks::cut(estimated_work(shape, inputs), usize::MAX);
        if tasks.count() <= 1 || shape.emitted == 0 {
            return None;
        }
        let splits = split_layers(shape, inputs, tasks.count());
        if splits.is_empty() {
            return None;
        }

        // The cartesian product of the layers' ranges, the first layer outermost.
        let mut cells: Vec<Cell> = vec![Vec::new()];
        for (layer, boundaries) in &splits {
            let ranges = (0..=boundaries.len()).map(|bucket| LayerClip {
                layer: *layer,
                low: bucket.checked_sub(1).map(|before| boundaries[before]),
                high: boundaries.get(bucket).copied(),
            });
            cells = cells
                .iter()
                .flat_map(|cell| {
                    ranges.clone().map(move |clip| {
                        let mut extended = cell.clone();
                        extended.push(clip);
                        extended
                    })
                })
                .collect();
        }

        let tasks = tasks.over(cells.len());
        Some(Self { cells, tasks })
    }

    /// Runs the cells on the worker pool, applying `run` to each cell's join.
    ///
    /// `template` builds the joins over the blocks `inputs`.
    /// Returns the results of `run`, in cell order,
    /// and the evaluation errors the joins counted.
    pub(crate) fn run<T: Send>(
        &self,
        template: JoinTemplate<'_>,
        inputs: &[&[&Trie]],
        run: impl Fn(&mut TrieJoin<'_>) -> T + Sync,
    ) -> (Vec<T>, ErrorCounts) {
        let outcomes = parallel::map_tasks(self.tasks, |cell| {
            with_cell_join(template, inputs, &self.cells[cell], &run)
        });

        let mut errors = ErrorCounts::default();
        let delivered = outcomes
            .into_iter()
            .map(|outcome| {
                errors.absorb(&outcome.errors);
                outcome.rows
            })
            .collect();

        (delivered, errors)
    }
}

thread_local! {
    /// The join arena of one worker, kept across the cells it runs.
    ///
    /// A cell never runs tasks on the pool itself,
    /// so a worker never starts another cell while one borrows the arena.
    static CELL_ARENA: RefCell<JoinArena> = RefCell::new(JoinArena::new());
}

/// Builds the join of a cell in the worker's arena, and clips it to the cell.
///
/// Returns the result of `run` on the join, and the join's evaluation errors.
fn with_cell_join<T>(
    template: JoinTemplate<'_>,
    inputs: &[&[&Trie]],
    cell: &Cell,
    run: impl FnOnce(&mut TrieJoin<'_>) -> T,
) -> CellOutcome<T> {
    CELL_ARENA.with(|arena| {
        let mut arena = arena.borrow_mut();
        arena.reset();

        let mut join = template.build(inputs, &arena);
        for clip in cell {
            join.clip_layer(clip.layer, clip.low, clip.high);
        }
        let rows = run(&mut join);

        CellOutcome {
            rows,
            errors: join.evaluation_errors(),
        }
    })
}

/// Returns an estimate of a join's descent work, in cursor value visits,
/// from the column lengths of its inputs' blocks alone.
///
/// A *binding* of a layer is a combination of values at the shallower layers.
/// Per emitted layer, the intersection of `s` cursors with average node sizes `N`
/// costs about `s · min N · log2(1 + max N / min N)` per binding,
/// since the smaller cursor gallops through the larger,
/// and yields about `min N` bindings for the layer below.
/// An input stored in several blocks counts one cursor per nonempty block,
/// since its union moves each of them and the join drops the empty ones.
/// The length of its column is summed over its blocks.
/// A layer below the emitted ones stops at its first match,
/// and a bound layer looks up its one value.
/// Either costs one gallop per cursor, and does not multiply the bindings.
///
/// The length of an input's column caps the bindings at the layer the column is mapped to.
/// That bounds only the input's own prefixes,
/// not bindings that multiply through layers the input does not map.
/// But it keeps a join of a small block with large tables, which is quick,
/// from looking like large work.
fn estimated_work(shape: &JoinShape, inputs: &[&[&Trie]]) -> usize {
    /// Returns the number of bits of a value, at least one,
    /// as an integer stand-in for its logarithm.
    ///
    /// The estimate runs for every join that might be split,
    /// which over many small executions is very often,
    /// so it avoids floating point.
    fn bits(value: usize) -> usize {
        usize::try_from(value.max(1).ilog2()).expect("bit counts are small") + 1
    }

    /// Returns the number of values in a column of an input, across its blocks.
    fn column_length(blocks: &[&Trie], column: usize) -> usize {
        blocks.iter().map(|trie| trie.column(column).len()).sum()
    }

    let mut bindings = 1usize;
    let mut work = 0usize;
    for (index, layer) in shape.layers.iter().enumerate() {
        let mut smallest = usize::MAX;
        let mut largest = 0;
        let mut cap = usize::MAX;
        let mut cursors = 0usize;
        for position in layer {
            let blocks = inputs[position.input];
            let length = column_length(blocks, position.column);
            if length == 0 {
                return 0;
            }
            // The parent nodes of the column's values are the values of the previous column.
            let parents = match position.column {
                0 => 1,
                column => column_length(blocks, column - 1).max(1),
            };
            let average = length.div_ceil(parents);
            smallest = smallest.min(average);
            largest = largest.max(average);
            cap = cap.min(length);
            cursors += blocks.iter().filter(|trie| !trie.is_empty()).count();
        }

        let bound = shape.bound_layers.iter().any(|bound| bound.layer == index);
        if index < shape.emitted && !bound {
            let cost = cursors
                .saturating_mul(smallest)
                .saturating_mul(bits(largest / smallest));
            work = work.saturating_add(bindings.saturating_mul(cost));
            bindings = bindings.saturating_mul(smallest).min(cap);
        } else {
            let cost = cursors.saturating_mul(bits(largest));
            work = work.saturating_add(bindings.saturating_mul(cost));
        }
    }

    work
}

/// Returns the largest number of parts, at most `cap`,
/// into which a layer of `domain` values can be split with balanced parts.
///
/// `p` parts need at least `3 · p · log2 p` values.
/// This is the balls-into-bins bound that keeps the largest part
/// within a constant factor of the average.
fn largest_share(domain: usize, cap: usize) -> usize {
    (2..=cap)
        .rev()
        .find(|&share| {
            3 * share * usize::try_from(share.ilog2()).expect("share counts are small") <= domain
        })
        .unwrap_or(1)
}

/// Returns the layers to split to reach about `target` cells,
/// each with its strictly increasing boundary values.
///
/// [Cells::partition] describes the policy.
fn split_layers(
    shape: &JoinShape,
    inputs: &[&[&Trie]],
    target: usize,
) -> Vec<(usize, Vec<StorageValue>)> {
    let mut splits = Vec::new();
    let mut remaining = target;
    for (layer, positions) in shape.layers.iter().enumerate().take(shape.emitted) {
        // A layer bound to a constant has one value, which no split divides.
        if shape
            .bound_layers
            .iter()
            .any(|bound| bound.layer == layer && bound.is_constant())
        {
            continue;
        }

        let Some(longest) = positions
            .iter()
            .flat_map(|position| {
                inputs[position.input]
                    .iter()
                    .map(|trie| trie.column(position.column))
            })
            .max_by_key(ColumnView::len)
        else {
            break;
        };
        let share = largest_share(longest.len(), remaining);
        if share <= 1 {
            continue;
        }

        // Below the first layer, a column is sorted only within the children of each node,
        // so the samples come out of order.
        let mut boundaries: Vec<StorageValue> = (1..share)
            .map(|bucket| longest.get(bucket * longest.len() / share))
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        if boundaries.is_empty() {
            continue;
        }

        remaining = remaining.div_ceil(boundaries.len() + 1);
        splits.push((layer, boundaries));
        if remaining <= 1 {
            break;
        }
    }

    splits
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        execution::{BoundLayerShape, InputColumn},
        testing::int_trie,
        tuning::Tuning,
        value::DataValue,
    };

    /// Returns the layers of the chain R(a, b) ⋈ S(b, c).
    fn chain() -> Vec<Vec<InputColumn>> {
        vec![
            vec![InputColumn {
                input: 0,
                column: 0,
            }],
            vec![
                InputColumn {
                    input: 0,
                    column: 1,
                },
                InputColumn {
                    input: 1,
                    column: 0,
                },
            ],
            vec![InputColumn {
                input: 1,
                column: 1,
            }],
        ]
    }

    /// Returns a shape over the given layers, emitting every layer.
    fn shape(layers: &[Vec<InputColumn>]) -> JoinShape<'_> {
        JoinShape {
            layers,
            emitted: layers.len(),
            subtracted: &[],
            filters: &[],
            bound_layers: &[],
        }
    }

    /// A layer is split into `p` parts only if it has at least `3 · p · log2 p` values.
    #[test]
    fn shares_need_balanced_buckets() {
        assert_eq!(largest_share(0, 64), 1);
        assert_eq!(largest_share(5, 64), 1);
        assert_eq!(largest_share(6, 64), 2);
        assert_eq!(largest_share(1_000_000, 64), 64);
        assert_eq!(largest_share(1_000_000, 1), 1);
        // 3 · 11 · 3 = 99 is at most 100, and 3 · 12 · 3 = 108 is not.
        assert_eq!(largest_share(100, 64), 11);
    }

    /// A first layer with too few values leaves the split to the second layer,
    /// whose boundaries strictly increase.
    #[test]
    fn few_outer_values_split_the_next_layer() {
        // Three first-layer values with 3000 rows each.
        // The first layer cannot be split into enough parts, so the second layer is split.
        let mut rows = Vec::new();
        for a in 0..3 {
            for b in 0..3000 {
                rows.push([a, b]);
            }
        }
        let left = int_trie(2, &rows);
        let right = int_trie(2, &(0..3000).map(|b| [b, b + 1]).collect::<Vec<_>>());
        let layers = chain();
        let shape = shape(&layers);
        let tries: [&[&Trie]; 2] = [&[&left], &[&right]];

        let splits = split_layers(&shape, &tries, 16);
        assert_eq!(splits.len(), 1, "three values cannot be split");
        assert_eq!(splits[0].0, 1);
        assert_eq!(splits[0].1.len(), 15);
        // The second column restarts under every first-layer value,
        // and boundaries out of order would make cells overlap.
        assert!(
            splits[0].1.is_sorted_by(|low, high| low < high),
            "the boundaries strictly increase"
        );

        // A first layer of many values is split into all the parts itself.
        let long = int_trie(2, &(0..10_000).map(|a| [a, a]).collect::<Vec<_>>());
        let splits = split_layers(&shape, &[&[&long], &[&right]], 16);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].0, 0);
        assert_eq!(splits[0].1.len(), 15);

        // Only emitted layers are split.
        let first_only = JoinShape {
            emitted: 1,
            ..shape
        };
        assert!(split_layers(&first_only, &tries, 16).is_empty());
        assert!(estimated_work(&shape, &[&[&long], &[&right]]) > 0);
    }

    /// A layer bound to a constant is not split, and the next layer is split instead.
    #[test]
    fn layers_bound_to_constants_do_not_split() {
        // The first layer has many values but is bound to one of them,
        // so the second layer is split.
        let left = int_trie(2, &(0..10_000).map(|a| [a, a]).collect::<Vec<_>>());
        let right = int_trie(2, &(0..10_000).map(|b| [b, b + 1]).collect::<Vec<_>>());
        let layers = chain();
        let bound = [BoundLayerShape::constant(0, DataValue::integer(5))];
        let shape = JoinShape {
            bound_layers: &bound,
            ..shape(&layers)
        };

        let splits = split_layers(&shape, &[&[&left], &[&right]], 16);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].0, 1);
    }

    /// A join of a small input with a large one is estimated as less work than a join
    /// of two large ones, and as too little work for the pool.
    /// A join with an empty input is estimated as no work.
    #[test]
    fn work_estimates_follow_the_data() {
        let layers = chain();
        let shape = shape(&layers);
        let small = int_trie(2, &[[1, 2], [2, 3]]);
        let large = int_trie(2, &(0..5000).map(|a| [a, a + 1]).collect::<Vec<_>>());
        let tiny = estimated_work(&shape, &[&[&small], &[&large]]);
        let big = estimated_work(&shape, &[&[&large], &[&large]]);
        assert!(
            tiny < big,
            "a join of two rows against a large table is little work"
        );
        assert!(tiny < Tuning::get().parallel_work);
        let empty = Trie::empty(2);
        assert_eq!(estimated_work(&shape, &[&[&empty], &[&large]]), 0);
    }
}
