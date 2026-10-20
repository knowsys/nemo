//! Tasks that edit disjoint pieces of one slice in place.

use crate::parallel::Tasks;

/// Runs `run` over consecutive pieces of `slice`, editing them in place.
///
/// The pieces are given by their ends:
/// `ends` holds the ascending offsets at which each piece ends,
/// the last one being the slice's length,
/// so the pieces are `0..ends[0]`, `ends[0]..ends[1]`, and so on.
/// `run` receives one piece as a mutable slice and may do anything with it.
/// Every piece is edited by exactly one call,
/// and no two calls share memory,
/// so `run` needs no coordination with its other calls.
///
/// # Example
///
/// ```
/// use nemo_physical::parallel::{for_each_piece, Tasks};
///
/// // Three runs of numbers, each to be sorted on its own.
/// let mut values = vec![3, 1, 2, 9, 8, 7, 6, 5, 4];
/// let ends = [3, 4, 9];
/// for_each_piece(&mut values, &ends, Tasks::of(2), |piece| piece.sort());
/// assert_eq!(values, [1, 2, 3, 9, 4, 5, 6, 7, 8]);
/// ```
pub fn for_each_piece<T: Send>(
    slice: &mut [T],
    ends: &[usize],
    tasks: Tasks,
    run: impl Fn(&mut [T]) + Sync,
) {
    if ends.is_empty() {
        return;
    }

    debug_assert_eq!(*ends.last().expect("checked above"), slice.len());

    match tasks.pool() {
        Some(pool) => {
            let per_task = ends.len().div_ceil(tasks.count);
            pool.install(|| pieces(slice, 0, ends, per_task, &run));
        }
        None => pieces_in_order(slice, 0, ends, &run),
    }
}

/// Runs the pieces one after another;
/// `offset` is where `slice` begins within the whole,
/// so the ends translate to local positions.
fn pieces_in_order<T>(slice: &mut [T], offset: usize, ends: &[usize], run: &impl Fn(&mut [T])) {
    let mut start = 0;

    for &end in ends {
        let end = end - offset;
        run(&mut slice[start..end]);
        start = end;
    }
}

/// Halves the pieces into stealable jobs
/// until a job holds at most `per_task` of them,
/// the recursive part of [for_each_piece].
fn pieces<T: Send>(
    slice: &mut [T],
    offset: usize,
    ends: &[usize],
    per_task: usize,
    run: &(impl Fn(&mut [T]) + Sync),
) {
    if ends.len() <= per_task {
        return pieces_in_order(slice, offset, ends, run);
    }

    let middle = ends.len() / 2;
    let cut = ends[middle - 1] - offset;
    let (left, right) = slice.split_at_mut(cut);
    let (left_ends, right_ends) = ends.split_at(middle);

    rayon_core::join(
        || pieces(left, offset, left_ends, per_task, run),
        || pieces(right, offset + cut, right_ends, per_task, run),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pieces_are_edited_in_place_and_disjointly() {
        let ends = [3, 3, 10, 500, 1000];
        let expected: Vec<usize> = (0..1000)
            .map(|i| {
                if i < 3 {
                    0
                } else if i < 10 {
                    3
                } else if i < 500 {
                    10
                } else {
                    500
                }
            })
            .collect();
        let flatten = |piece: &mut [usize]| {
            let first = piece.first().copied().unwrap_or(0);
            for value in piece.iter_mut() {
                *value = first;
            }
        };
        for tasks in [Tasks::of(1), Tasks::of(2), Tasks::of(5), Tasks::of(9)] {
            let mut values: Vec<usize> = (0..1000).collect();
            for_each_piece(&mut values, &ends, tasks, flatten);
            assert_eq!(values, expected);
        }
        for_each_piece(&mut [0usize; 0], &[], Tasks::of(4), |_| panic!("no pieces"));
    }
}
