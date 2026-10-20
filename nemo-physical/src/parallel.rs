//! The worker pool that runs the parallel phases of this crate.
//!
//! The parallel phases
//! are all shaped the same way:
//! a fixed number of independent tasks
//! whose results are wanted in task order.
//! A phase estimates its work as the number of values it will visit or write,
//! and asks [Tasks] how it should be cut
//! by the thresholds of the [tuning](crate::tuning).
//! It cuts its input accordingly
//! and hands the tasks to [map_tasks] or [for_each_piece],
//! which run them on a persistent work-stealing pool.
//! A phase whose results are too large to hold at once
//! hands them to [stream_tasks] instead,
//! which passes them to the caller in task order as they are made.

mod map;
mod pieces;
mod stream;
mod tasks;
mod workers;

pub use map::map_tasks;
pub use pieces::for_each_piece;
pub use stream::stream_tasks;
pub use tasks::Tasks;
pub use workers::Workers;
