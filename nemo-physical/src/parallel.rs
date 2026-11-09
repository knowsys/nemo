//! The worker pool that runs the parallel phases of this crate.
//!
//! A *phase* is a step of work that is cut into a fixed number of independent *tasks*,
//! whose results are needed in task order.
//! A phase estimates its *work* as the number of values that it will visit or write,
//! and asks [Tasks] how to cut it, by the thresholds of the [tuning](crate::tuning).
//! It cuts its input accordingly and passes the tasks to [map_tasks] or [for_each_piece],
//! which run them on a persistent work-stealing pool.
//! A phase whose results are too large to hold at once uses [stream_tasks] instead,
//! which passes the results to a consumer in task order as they are made.

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
