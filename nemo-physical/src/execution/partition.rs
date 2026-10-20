//! Partitioning: splitting a large join into cells that run in parallel.
//!
//! A cell joins the same tries as the whole join,
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
    /// The lowest value of the range; `None` leaves it open below.
    low: Option<StorageValue>,
    /// The value the range ends before; `None` leaves it open above.
    high: Option<StorageValue>,
}

/// One cell of a join: the clips of its partitioned layers.
type Cell = Vec<LayerClip>;

/// The cells a join splits into.
#[derive(Debug)]
pub(crate) struct Cells {
    /// The cells, in the lexicographic order of their ranges.
    ///
    /// If only the first layer is partitioned, their rows chain into one sorted sequence.
    /// A partitioned deeper layer splits the rows of a first layer value across cells,
    /// whose rows then overlap.
    cells: Vec<Cell>,
    /// The cut that produced the cells, one task per cell.
    tasks: Tasks,
}

/// What one cell delivers: what its run returned and the evaluation errors its join counted.
#[derive(Debug)]
struct CellOutcome<T> {
    /// What the run of the cell's join returned.
    rows: T,
    /// The evaluation errors the cell's join counted, by kind.
    errors: ErrorCounts,
}

impl Cells {
    /// Splits the join of a shape over the given tries into cells,
    /// or returns `None` if it is too little work for the pool or has no layer to split.
    ///
    /// The number of cells follows the [work estimate](estimated_work).
    /// The first layer takes the shares first, and deeper layers take only what it cannot fill.
    /// Every share of a deeper layer runs the intersections above it once more,
    /// so a deeper split pays only if a leading layer has few values above large subtrees.
    /// Only the emitted layers split, so that the cells produce disjoint rows, each once,
    /// and none bound to a constant.
    ///
    /// The boundary values come from splitting the longest column intersected at a layer
    /// at equal positions.
    /// Each cell's join clips its cursors to its ranges,
    /// so the cells partition every cursor without copying.
    pub(crate) fn partition(shape: &JoinShape, tries: &[&Trie]) -> Option<Self> {
        let tasks = Tasks::cut(estimated_work(shape, tries), usize::MAX);
        if tasks.count() <= 1 || shape.emitted == 0 {
            return None;
        }
        let splits = split_layers(shape, tries, tasks.count());
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

    /// Runs the cells on the worker pool, each through `run` on its join,
    /// and returns what each cell delivered, in cell order,
    /// with the evaluation errors their joins counted.
    pub(crate) fn run<T: Send>(
        &self,
        template: JoinTemplate<'_>,
        tries: &[&Trie],
        run: impl Fn(&mut TrieJoin<'_>) -> T + Sync,
    ) -> (Vec<T>, ErrorCounts) {
        let outcomes = parallel::map_tasks(self.tasks, |cell| {
            with_cell_join(template, tries, &self.cells[cell], &run)
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
    /// A cell never enters the pool itself, so the borrow never nests.
    static CELL_ARENA: RefCell<JoinArena> = RefCell::new(JoinArena::new());
}

/// Builds the join of a cell in the worker's arena, clips it to the cell,
/// and returns the outcome of running it through `run`.
fn with_cell_join<T>(
    template: JoinTemplate<'_>,
    tries: &[&Trie],
    cell: &Cell,
    run: impl FnOnce(&mut TrieJoin<'_>) -> T,
) -> CellOutcome<T> {
    CELL_ARENA.with(|arena| {
        let mut arena = arena.borrow_mut();
        arena.reset();

        let mut join = template.build(tries, &arena);
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
/// from the column lengths of its tries alone.
///
/// Per emitted layer, the intersection of `s` cursors with average block sizes `N`
/// costs about `s · min N · log2(1 + max N / min N)` per binding of the layers above,
/// galloping the smaller cursor through the larger,
/// and yields about `min N` bindings for the layer below.
/// An existence layer beyond the emitted ones stops at its first match,
/// and a bound layer looks up its one value:
/// one gallop per cursor either way, and no multiplication of bindings.
///
/// The length of an input's column caps the bindings at the layer it carries.
/// That bounds only the input's own prefixes,
/// not bindings multiplying through markers the input lacks,
/// but it keeps a join of a small block against large tables,
/// done in microseconds, from ever looking like large work.
fn estimated_work(shape: &JoinShape, tries: &[&Trie]) -> usize {
    /// Returns the bits of a value, at least one: the integer stand-in for the logarithm.
    ///
    /// The estimate runs once per join that might split,
    /// which can be millions of times over many small executions,
    /// so it stays integral.
    fn bits(value: usize) -> usize {
        usize::try_from(value.max(1).ilog2()).expect("bit counts are small") + 1
    }

    let mut bindings = 1usize;
    let mut work = 0usize;
    for (index, layer) in shape.layers.iter().enumerate() {
        let mut smallest = usize::MAX;
        let mut largest = 0;
        let mut cap = usize::MAX;
        for position in layer {
            let trie = tries[position.input];
            let length = trie.column(position.column).len();
            if length == 0 {
                return 0;
            }
            let parents = if position.column == 0 {
                1
            } else {
                trie.column(position.column - 1).len().max(1)
            };
            let average = length.div_ceil(parents);
            smallest = smallest.min(average);
            largest = largest.max(average);
            cap = cap.min(length);
        }

        let bound = shape.bound_layers.iter().any(|bound| bound.layer == index);
        if index < shape.emitted && !bound {
            let cost = layer
                .len()
                .saturating_mul(smallest)
                .saturating_mul(bits(largest / smallest));
            work = work.saturating_add(bindings.saturating_mul(cost));
            bindings = bindings.saturating_mul(smallest).min(cap);
        } else {
            let cost = layer.len().saturating_mul(bits(largest));
            work = work.saturating_add(bindings.saturating_mul(cost));
        }
    }

    work
}

/// Returns the largest share, at most `cap`, a layer of the given number of values splits into
/// with balanced buckets.
///
/// `p` shares need at least `3 · p · log2 p` values,
/// the balls-into-bins bound that keeps the largest bucket within a constant of the average.
fn largest_share(domain: usize, cap: usize) -> usize {
    (2..=cap)
        .rev()
        .find(|&share| {
            3 * share * usize::try_from(share.ilog2()).expect("share counts are small") <= domain
        })
        .unwrap_or(1)
}

/// Returns the layers a join splits to reach about `target` cells,
/// each with its strictly increasing boundary values; see [Cells::partition] for the policy.
fn split_layers(
    shape: &JoinShape,
    tries: &[&Trie],
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
            .map(|position| tries[position.input].column(position.column))
            .max_by_key(ColumnView::len)
        else {
            break;
        };
        let share = largest_share(longest.len(), remaining);
        if share <= 1 {
            continue;
        }

        // Below the first layer, a column is sorted only within each block of its parent,
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

    #[test]
    fn shares_need_balanced_buckets() {
        assert_eq!(largest_share(0, 64), 1);
        assert_eq!(largest_share(5, 64), 1);
        assert_eq!(largest_share(6, 64), 2);
        assert_eq!(largest_share(1_000_000, 64), 64);
        assert_eq!(largest_share(1_000_000, 1), 1);
        // 3 · 11 · 3 = 99 fits 100; 3 · 12 · 3 = 108 does not.
        assert_eq!(largest_share(100, 64), 11);
    }

    #[test]
    fn few_outer_values_split_the_next_layer() {
        // Three first-layer values above 3000 rows each:
        // the first layer cannot fill the budget, so the second takes the shares.
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
        let tries = [&left, &right];

        let splits = split_layers(&shape, &tries, 16);
        assert_eq!(splits.len(), 1, "three values cannot be split");
        assert_eq!(splits[0].0, 1);
        assert_eq!(splits[0].1.len(), 15);
        // The second column restarts under every first-layer value;
        // boundaries out of order would make cells overlap.
        assert!(
            splits[0].1.is_sorted_by(|low, high| low < high),
            "the boundaries strictly increase"
        );

        // A first layer of many values takes every share itself.
        let long = int_trie(2, &(0..10_000).map(|a| [a, a]).collect::<Vec<_>>());
        let splits = split_layers(&shape, &[&long, &right], 16);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].0, 0);
        assert_eq!(splits[0].1.len(), 15);

        // Only the emitted layers split.
        let first_only = JoinShape {
            emitted: 1,
            ..shape
        };
        assert!(split_layers(&first_only, &tries, 16).is_empty());
        assert!(estimated_work(&shape, &[&long, &right]) > 0);
    }

    #[test]
    fn layers_bound_to_constants_do_not_split() {
        // The first layer has many values but is bound to one of them,
        // so the second layer takes the shares.
        let left = int_trie(2, &(0..10_000).map(|a| [a, a]).collect::<Vec<_>>());
        let right = int_trie(2, &(0..10_000).map(|b| [b, b + 1]).collect::<Vec<_>>());
        let layers = chain();
        let bound = [BoundLayerShape::constant(0, DataValue::integer(5))];
        let shape = JoinShape {
            bound_layers: &bound,
            ..shape(&layers)
        };

        let splits = split_layers(&shape, &[&left, &right], 16);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].0, 1);
    }

    #[test]
    fn work_estimates_follow_the_data() {
        let layers = chain();
        let shape = shape(&layers);
        let small = int_trie(2, &[[1, 2], [2, 3]]);
        let large = int_trie(2, &(0..5000).map(|a| [a, a + 1]).collect::<Vec<_>>());
        let tiny = estimated_work(&shape, &[&small, &large]);
        let big = estimated_work(&shape, &[&large, &large]);
        assert!(
            tiny < big,
            "a join of two rows against a large table is little work"
        );
        assert!(tiny < Tuning::get().parallel_work);
        let empty = Trie::empty(2);
        assert_eq!(estimated_work(&shape, &[&empty, &large]), 0);
    }
}
