//! Tasks whose items are passed to a consumer on the calling thread as they are made.
//!
//! The *consumer* is the calling thread, which passes each item to a callback.

use std::{
    ops::ControlFlow,
    sync::{Condvar, Mutex, PoisonError, mpsc},
};

use crate::parallel::{Tasks, Workers};

/// The total weight of the items that one worker has made and the consumer has not taken yet,
/// and whether the consumer has stopped.
#[derive(Debug, Default)]
struct Held {
    /// The weight held, and whether the consumer has stopped taking items.
    state: Mutex<(usize, bool)>,
    /// Signalled when the consumer takes an item or stops.
    taken: Condvar,
}

impl Held {
    /// Waits until an item of the given weight may be held, and adds its weight.
    ///
    /// The item may be held at once if nothing is held,
    /// and otherwise once the held weight plus its weight is at most `ahead`.
    /// The wait also ends once the consumer has stopped.
    /// Returns whether the consumer still takes items.
    fn admit(&self, weight: usize, ahead: usize) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.0 > 0 && state.0 + weight > ahead && !state.1 {
            state = self
                .taken
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.0 += weight;
        !state.1
    }

    /// Releases the weight of an item that the consumer took.
    fn release(&self, weight: usize) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).0 -= weight;
        self.taken.notify_one();
    }

    /// Marks the consumer as stopped, and wakes the waiting worker.
    fn stop(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).1 = true;
        self.taken.notify_all();
    }
}

/// A guard that stops every worker's [Held] when it is dropped,
/// so that no worker keeps waiting for a consumer that has left,
/// whether by returning or by panicking.
struct Stopping<'a>(&'a [Held]);

impl Drop for Stopping<'_> {
    /// Marks the consumer as stopped in every worker's [Held].
    fn drop(&mut self) {
        for held in self.0 {
            held.stop();
        }
    }
}

/// Runs the tasks and passes the items they emit to `consume` on the calling thread,
/// in task order, and within a task in the order of emission.
///
/// `produce` runs the task with the given index,
/// and passes each item that it makes to the emit function that it is given.
/// The emit function returns whether items are still wanted,
/// and a task stops at the first `false`.
/// `produce` may be called from several worker threads at once,
/// so it reads its input through shared references.
/// `consume` receives each item with the index of its task.
/// It ends the stream early by breaking, and no item is consumed after that.
/// `weight` returns the weight of an item, and `ahead` the weight of the items of a task
/// that its worker may hold.
///
/// If the tasks may use the pool, they run there while the calling thread consumes,
/// so even a single task overlaps with the consumption of its items.
/// With `n` workers, worker `w` runs the tasks `w`, `w + n`, `w + 2n`, … in order,
/// and passes their items through a channel of its own.
/// A worker holds the items that it made and that were not consumed yet,
/// up to a total weight of `ahead(task)` for its current task, or a single item of any weight.
/// It waits before an item that would exceed that.
/// So the task whose items the consumer waits for has always started,
/// and for each worker, the items made and not yet consumed weigh at most the largest `ahead`
/// plus two items, however long or short the tasks are.
/// A later task whose items weigh at most its `ahead` is made in full
/// while the consumer still takes the items of earlier tasks.
/// If the tasks may not use the pool, each task runs on the calling thread,
/// and every item is consumed as soon as it is emitted.
///
/// # Panics
///
/// A panic in a task or in `consume` is propagated once every task has stopped.
///
/// # Example
///
/// ```
/// use std::ops::ControlFlow;
///
/// use nemo_physical::parallel::{stream_tasks, Tasks};
///
/// // Each task emits its index three times, and `consume` receives them in order.
/// let mut seen = Vec::new();
/// stream_tasks(
///     Tasks::of(4),
///     |_| 1,
///     |_| 1,
///     |task, emit| {
///         for _ in 0..3 {
///             if !emit(task) {
///                 return;
///             }
///         }
///     },
///     |task, item| {
///         assert_eq!(task, item);
///         seen.push(item);
///         ControlFlow::Continue(())
///     },
/// );
/// assert_eq!(seen, [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]);
/// ```
pub fn stream_tasks<T: Send>(
    tasks: Tasks,
    ahead: impl Fn(usize) -> usize + Sync,
    weight: impl Fn(&T) -> usize + Sync,
    produce: impl Fn(usize, &mut dyn FnMut(T) -> bool) + Sync,
    mut consume: impl FnMut(usize, T) -> ControlFlow<()>,
) {
    let Some(pool) = Workers::get().pool().filter(|_| tasks.parallel) else {
        for task in 0..tasks.count {
            let mut flow = ControlFlow::Continue(());
            produce(task, &mut |item| {
                if flow.is_continue() {
                    flow = consume(task, item);
                }
                flow.is_continue()
            });
            if flow.is_break() {
                return;
            }
        }
        return;
    };

    let workers = Workers::get().threads().min(tasks.count);
    // A task's items go through its worker's channel with their weights, and `None` ends the task.
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..workers)
        .map(|_| mpsc::channel::<Option<(usize, T)>>())
        .unzip();
    let helds: Vec<Held> = (0..workers).map(|_| Held::default()).collect();
    let (produce, ahead, weight, helds) = (&produce, &ahead, &weight, &helds);

    pool.in_place_scope(|scope| {
        for (worker, sender) in senders.into_iter().enumerate() {
            scope.spawn(move |_| {
                let held = &helds[worker];
                for task in (worker..tasks.count).step_by(workers) {
                    let ahead = ahead(task);
                    let mut open = true;
                    produce(task, &mut |item| {
                        let weight = weight(&item);
                        open = open
                            && held.admit(weight, ahead)
                            && sender.send(Some((weight, item))).is_ok();
                        open
                    });
                    if !open || sender.send(None).is_err() {
                        return;
                    }
                }
            });
        }

        // The receivers are owned here, so that leaving early closes every channel,
        // and the guard wakes every waiting worker, which then stops.
        let receivers = receivers;
        let _stopping = Stopping(helds);
        for task in 0..tasks.count {
            loop {
                match receivers[task % workers].recv() {
                    Ok(Some((weight, item))) => {
                        helds[task % workers].release(weight);
                        if consume(task, item).is_break() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    // The task's worker panicked, and the panic propagates once the scope ends.
                    Err(_) => return,
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };

    /// Streams the tasks with `items` items each,
    /// and returns the consumed pairs of a task index and an item.
    fn streamed(tasks: Tasks, ahead: usize, items: usize) -> Vec<(usize, usize)> {
        let mut seen = Vec::new();
        stream_tasks(
            tasks,
            |_| ahead,
            |_| 1,
            |task, emit| {
                for item in 0..items {
                    if !emit(item) {
                        return;
                    }
                }
                assert!(task < tasks.count());
            },
            |task, item| {
                seen.push((task, item));
                ControlFlow::Continue(())
            },
        );
        seen
    }

    /// Items arrive in task order and in the order of emission,
    /// on the pool or not, and for any `ahead`.
    #[test]
    fn streamed_items_arrive_in_task_and_emission_order() {
        for count in [0, 1, 2, 7, 50] {
            let expected: Vec<(usize, usize)> = (0..count)
                .flat_map(|task| (0..20).map(move |item| (task, item)))
                .collect();
            for ahead in [0, 1, 4] {
                assert_eq!(streamed(Tasks::of(count), ahead, 20), expected);
                assert_eq!(streamed(Tasks::sequential(count), ahead, 20), expected);
            }
        }
    }

    /// No item is consumed after `consume` breaks.
    #[test]
    fn a_breaking_consumer_ends_the_stream() {
        let expected: Vec<(usize, usize)> = (0..3)
            .flat_map(|task| (0..10).map(move |item| (task, item)))
            .chain((0..=5).map(|item| (3, item)))
            .collect();
        for tasks in [Tasks::of(8), Tasks::sequential(8)] {
            let mut seen = Vec::new();
            stream_tasks(
                tasks,
                |_| 2,
                |_| 1,
                |_, emit| {
                    for item in 0..10 {
                        if !emit(item) {
                            return;
                        }
                    }
                },
                |task, item| {
                    seen.push((task, item));
                    if (task, item) == (3, 5) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            );
            assert_eq!(seen, expected);
        }
    }

    /// Returns the largest weight that was made and not yet consumed
    /// while streaming `count` tasks of `items` items each.
    ///
    /// An item weighs `weigh(item)`, and the consumer yields its thread after every item.
    fn most_waiting(count: usize, items: usize, ahead: usize, weigh: fn(usize) -> usize) -> usize {
        let made = AtomicUsize::new(0);
        let mut consumed = 0;
        let mut taken = 0;
        let mut most = 0;
        stream_tasks(
            Tasks::of(count),
            |_| ahead,
            |&item| weigh(item),
            |_, emit| {
                for item in 0..items {
                    made.fetch_add(weigh(item), Ordering::SeqCst);
                    if !emit(item) {
                        return;
                    }
                }
            },
            |_, item| {
                consumed += weigh(item);
                taken += 1;
                most = most.max(made.load(Ordering::SeqCst) - consumed);
                thread::yield_now();
                ControlFlow::Continue(())
            },
        );
        assert_eq!(taken, count * items);
        most
    }

    /// The items made and not yet consumed stay within `ahead` plus two items per worker,
    /// for long tasks and for short ones.
    #[test]
    fn tasks_run_at_most_ahead_items_in_front_of_the_consumer() {
        let ahead = 2;
        let bound = Workers::get().threads() * (ahead + 2);
        // Long tasks wait while their worker holds `ahead` items.
        // Short tasks finish at once, and their worker's next task waits behind them.
        for (count, items) in [(16, 200), (2000, 1)] {
            let most = most_waiting(count, items, ahead, |_| 1);
            assert!(most <= bound, "{most} items waited, at most {bound} may");
        }
    }

    /// With items of different weights, a worker holds at most `ahead` in weight.
    #[test]
    fn a_worker_holds_at_most_ahead_in_weight_or_one_item() {
        // Per worker, the bound allows `ahead` in weight, one more item that waits,
        // and the item that the consumer took last.
        let ahead = 16;
        let bound = Workers::get().threads() * (ahead + 3 * 10);
        let most = most_waiting(16, 200, ahead, |item| 1 + item % 10);
        assert!(
            most <= bound,
            "{most} waited in weight, at most {bound} may"
        );
    }

    /// An item heavier than `ahead` is still passed on, one at a time.
    #[test]
    fn an_item_heavier_than_ahead_passes_alone() {
        let bound = Workers::get().threads() * 3 * 100;
        let most = most_waiting(8, 50, 4, |_| 100);
        assert!(
            most <= bound,
            "{most} waited in weight, at most {bound} may"
        );
    }

    /// A panic in `consume` propagates, and the waiting workers are released.
    #[test]
    fn a_panicking_consumer_releases_the_waiting_workers() {
        let outcome = std::panic::catch_unwind(|| {
            stream_tasks(
                Tasks::of(16),
                |_| 1,
                |_| 1,
                |_, emit| {
                    for item in 0..100 {
                        if !emit(item) {
                            return;
                        }
                    }
                },
                |task, _| {
                    if task == 1 {
                        panic!("the consumer fails");
                    }
                    ControlFlow::Continue(())
                },
            )
        });
        assert!(outcome.is_err());
    }

    /// A panic in a task propagates, and the stream does not wait forever.
    #[test]
    fn a_panicking_task_propagates_without_stalling_the_stream() {
        for failing in [0, 3] {
            let outcome = std::panic::catch_unwind(|| {
                stream_tasks(
                    Tasks::of(6),
                    |_| 1,
                    |_| 1,
                    |task, emit| {
                        if task >= failing {
                            panic!("task {task} fails");
                        }
                        emit(task);
                    },
                    |_, _| ControlFlow::Continue(()),
                )
            });
            assert!(outcome.is_err());
        }
    }
}
