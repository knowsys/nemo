//! Tasks whose results come back together, in task order.

use crate::parallel::Tasks;

/// Runs the tasks, returning their results in task order.
///
/// The caller has cut its work into `tasks.count()` tasks
/// and describes them by `run`,
/// which computes the result of the task with the given index.
/// `run` may be called from several worker threads at once,
/// so it reads its input through shared references
/// and writes nothing but its result.
/// The results come back as one vector, the task at each index.
///
/// When the tasks may not use the pool,
/// they run one after another on the calling thread.
/// Otherwise they are scheduled by work stealing:
/// the calling thread joins the workers,
/// and a worker that finishes early takes on tasks not yet started,
/// so uneven tasks balance across the workers.
/// The call returns once every task has run.
///
/// A panic in any task propagates to the caller.
///
/// # Example
///
/// ```
/// use nemo_physical::parallel::{map_tasks, Tasks};
///
/// let values: Vec<u64> = (1..=1000).collect();
/// let sums = map_tasks(Tasks::of(50), |task| {
///     values[task * 20..(task + 1) * 20].iter().sum::<u64>()
/// });
/// assert_eq!(sums.len(), 50);
/// assert_eq!(sums.iter().sum::<u64>(), 500_500);
/// ```
pub fn map_tasks<T: Send>(tasks: Tasks, run: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let Some(pool) = tasks.pool() else {
        return (0..tasks.count).map(run).collect();
    };

    let mut slots: Vec<Option<T>> = Vec::with_capacity(tasks.count);
    slots.resize_with(tasks.count, || None);

    pool.install(|| fill(&mut slots, 0, &run));

    slots
        .into_iter()
        .map(|slot| slot.expect("every task ran"))
        .collect()
}

/// Fills the result slots by binary splitting:
/// each half is a stealable job,
/// so the pool balances the tasks however unevenly they run.
fn fill<T: Send>(slots: &mut [Option<T>], offset: usize, run: &(impl Fn(usize) -> T + Sync)) {
    match slots {
        [] => {}
        [slot] => *slot = Some(run(offset)),
        _ => {
            let middle = slots.len() / 2;
            let (left, right) = slots.split_at_mut(middle);
            rayon_core::join(
                || fill(left, offset, run),
                || fill(right, offset + middle, run),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tasks_come_back_in_order() {
        let squares = map_tasks(Tasks::of(37), |index| index * index);
        assert_eq!(
            squares,
            (0..37).map(|index| index * index).collect::<Vec<_>>()
        );
        assert_eq!(map_tasks(Tasks::of(0), |index| index), Vec::<usize>::new());
        assert_eq!(map_tasks(Tasks::of(1), |index| index + 1), vec![1]);
        assert_eq!(map_tasks(Tasks::cut(0, 5), |index| index), vec![0]);
    }
}
