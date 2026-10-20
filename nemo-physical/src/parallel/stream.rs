//! Tasks whose results are handed to the caller as they are made.

use std::{
    ops::ControlFlow,
    sync::{Condvar, Mutex, PoisonError, mpsc},
};

use crate::parallel::{Tasks, Workers};

/// The items one worker has made that the consumer has not taken yet, by weight.
#[derive(Debug, Default)]
struct Held {
    /// The weight held, and whether the consumer has stopped taking items.
    state: Mutex<(usize, bool)>,
    /// Signalled when the consumer takes an item or stops.
    taken: Condvar,
}

impl Held {
    /// Waits until an item of the given weight may join the held ones, and adds it:
    /// at once if nothing is held, else once the held weight leaves room for it within `ahead`.
    ///
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

    /// Releases the weight of an item the consumer took.
    fn release(&self, weight: usize) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).0 -= weight;
        self.taken.notify_one();
    }

    /// Marks the consumer stopped, waking a waiting worker.
    fn stop(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).1 = true;
        self.taken.notify_all();
    }
}

/// Stops every worker's [Held] when dropped,
/// so that no worker waits for a consumer that is gone, however it left.
struct Stopping<'a>(&'a [Held]);

impl Drop for Stopping<'_> {
    fn drop(&mut self) {
        for held in self.0 {
            held.stop();
        }
    }
}

/// Runs the tasks and hands what they emit to `consume` on the calling thread:
/// task by task in task order, and each task's items in the order it emitted them.
///
/// `produce` runs the task with the given index,
/// passing each item it makes to the emitter it is given.
/// The emitter answers whether the items are still wanted,
/// and a task stops at the first `false`.
/// `produce` may be called from several worker threads at once,
/// so it reads its input through shared references.
/// `consume` receives each item with the index of its task,
/// and ends the phase early by breaking:
/// no item is consumed after that.
///
/// When the tasks may use the pool,
/// they run there while the calling thread consumes,
/// so even a single task overlaps with the consumption of its items.
/// Of `n` workers, worker `w` runs the tasks `w`, `w + n`, `w + 2n`, … in order,
/// passing their items through a channel of its own.
/// A worker holds the items it made and the consumer did not take
/// up to a total `weight` of `ahead(task)` for the task it runs, or a single item of any weight,
/// and waits before an item that would exceed that.
/// So the task whose items the consumer waits for has always started,
/// and the items made and not yet consumed weigh at most the largest `ahead`
/// plus two items per worker, however long or short the tasks are.
/// A later task whose items weigh at most its `ahead` is made whole
/// while the consumer still takes the items of earlier ones.
/// Otherwise each task runs on the calling thread,
/// and every item is consumed as soon as it is emitted.
///
/// A panic in a task or in `consume` propagates to the caller
/// once every task has stopped.
///
/// # Example
///
/// ```
/// use std::ops::ControlFlow;
///
/// use nemo_physical::parallel::{stream_tasks, Tasks};
///
/// // Each task emits its index three times; the consumer sees them in order.
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
    // A task's items go through its worker's channel with their weights, `None` ending the task.
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

        // Owned here, so that leaving early closes every channel
        // and wakes every waiting worker, which then stops.
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
                    // Its worker panicked; the panic propagates once the scope ends.
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

    /// Streams `count` tasks of `items` items each, returning the `(task, item)` pairs consumed.
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

    /// The most weight made and not yet consumed while streaming `count` tasks
    /// of `items` items each, an item weighing `weigh(item)`,
    /// the consumer pausing on every item.
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

    #[test]
    fn tasks_run_at_most_ahead_items_in_front_of_the_consumer() {
        let ahead = 2;
        let bound = Workers::get().threads() * (ahead + 2);
        // Long tasks wait on their worker's held items; short ones finish at once
        // and their worker's next task waits behind them.
        for (count, items) in [(16, 200), (2000, 1)] {
            let most = most_waiting(count, items, ahead, |_| 1);
            assert!(most <= bound, "{most} items waited, at most {bound} may");
        }
    }

    #[test]
    fn a_worker_holds_at_most_ahead_in_weight_or_one_item() {
        // A worker holds `ahead`, waits with one more item,
        // and the consumer counts the item it took last.
        let ahead = 16;
        let bound = Workers::get().threads() * (ahead + 3 * 10);
        let most = most_waiting(16, 200, ahead, |item| 1 + item % 10);
        assert!(
            most <= bound,
            "{most} waited in weight, at most {bound} may"
        );
    }

    #[test]
    fn an_item_heavier_than_ahead_passes_alone() {
        let bound = Workers::get().threads() * 3 * 100;
        let most = most_waiting(8, 50, 4, |_| 100);
        assert!(
            most <= bound,
            "{most} waited in weight, at most {bound} may"
        );
    }

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
