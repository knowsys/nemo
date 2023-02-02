//! Code for timing blocks of code

use ascii_tree::{write_tree, Tree};
use howlong::*;
use linked_hash_map::LinkedHashMap;
use once_cell::sync::Lazy;
use std::fmt;
use std::{
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

/// Global instance of the [`TimedCode`]
static TIMECODE_INSTANCE: Lazy<Mutex<TimedCode>> = Lazy::new(|| {
    let instance = TimedCode::new();
    Mutex::new(instance)
});

/// Represents a block of code that is timed
#[derive(Default, Copy, Clone)]
pub struct TimedCodeInfo {
    total_system_time: Duration,
    total_process_time: Duration,
    total_thread_time: Duration,
    start_system: Option<TimePoint>,
    start_process: Option<TimePoint>,
    start_thread: Option<TimePoint>,
    runs: u64,
}

impl TimedCodeInfo {
    /// Create new [`TimedCodeInfo`] object
    pub fn new() -> Self {
        Self {
            total_system_time: Duration::from_secs(0),
            total_process_time: Duration::from_secs(0),
            total_thread_time: Duration::from_secs(0),
            start_system: None,
            start_process: None,
            start_thread: None,
            runs: 0,
        }
    }
}

impl fmt::Debug for TimedCodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let run_msg;
        if self.start_system.is_some() {
            run_msg = "currently running";
        } else {
            run_msg = "currently not running";
        }
        write!(
            f,
            "TimedCodeInfo [totals (msec): {}/{}/{}, {:?} completed runs, {}]",
            self.total_system_time.as_millis(),
            self.total_process_time.as_millis(),
            self.total_thread_time.as_millis(),
            self.runs,
            run_msg
        )
    }
}

/// How to sort the elements of a [`TimedCode`] object
#[derive(Debug, Copy, Clone)]
pub enum TimedSorting {
    /// The order the code got called in
    Default,
    /// Alphabetical by the title of the block
    Alphabetical,
    /// Show the blocks which took longest first
    LongestThreadTime,
}

impl Default for TimedSorting {
    fn default() -> Self {
        Self::Default
    }
}

/// How to display a layer of a [`TimedCode`] object
#[derive(Debug, Default, Copy, Clone)]
pub struct TimedDisplay {
    sorting: TimedSorting,
    num_elements: usize,
}

impl TimedDisplay {
    /// Create new [`TimedDisplay`]
    pub fn new(sorting: TimedSorting, num_elements: usize) -> Self {
        Self {
            sorting,
            num_elements,
        }
    }
}

/// Represents a block of code that is timed
#[derive(Debug, Default, Clone)]
pub struct TimedCode {
    info: TimedCodeInfo,
    subblocks: LinkedHashMap<String, TimedCode>,
}

impl TimedCode {
    /// Create new [`TimedCode`] object
    pub fn new() -> Self {
        Self {
            info: TimedCodeInfo::new(),
            subblocks: LinkedHashMap::new(),
        }
    }

    /// Return the global instance
    pub fn instance() -> MutexGuard<'static, TimedCode> {
        TIMECODE_INSTANCE.lock().unwrap()
    }

    /// Navigate to a subblock (use forward slash to go multiple layers at once)
    pub fn sub(&mut self, name: &str) -> &mut TimedCode {
        if cfg!(test) {
            return self;
        }

        let name_parts: Vec<&str> = name.split('/').collect();

        let mut current_block = self;
        for part in name_parts {
            let part_string = String::from_str(part).expect("We expect well formed strings");
            current_block = current_block
                .subblocks
                .entry(part_string)
                .or_insert_with(TimedCode::new)
        }

        current_block
    }

    /// Start the next measurement
    pub fn start(&mut self) {
        if cfg!(test) {
            return;
        }

        debug_assert!(self.info.start_thread.is_none());

        self.info.start_system = Some(HighResolutionClock::now());
        self.info.start_process = Some(ProcessRealCPUClock::now());
        self.info.start_thread = Some(ThreadClock::now());
    }

    /// Stop the current measurement and save the times
    pub fn stop(&mut self) -> Duration {
        if cfg!(test) {
            return Duration::ZERO;
        }

        debug_assert!(self.info.start_system.is_some());

        let start_system = self
            .info
            .start_system
            .expect("start() must be called before calling stop()");
        let start_process = self
            .info
            .start_process
            .expect("start() must be called before calling stop()");
        let start_thread = self
            .info
            .start_thread
            .expect("start() must be called before calling stop()");

        let duration_system = HighResolutionClock::now() - start_system;
        let duration_process = ProcessRealCPUClock::now() - start_process;
        let duration_thread = ThreadClock::now() - start_thread;

        self.info.total_system_time += duration_system;
        self.info.total_process_time += duration_process;
        self.info.total_thread_time += duration_thread;
        self.info.start_system = None;
        self.info.start_process = None;
        self.info.start_thread = None;
        self.info.runs += 1;

        duration_thread
    }

    fn apply_display_option<'a>(
        code: &'a TimedCode,
        option: &TimedDisplay,
    ) -> Vec<(&'a String, &'a TimedCode)> {
        let mut blocks: Vec<(&'a String, &'a TimedCode)> = code.subblocks.iter().collect();

        match option.sorting {
            TimedSorting::Default => {}
            TimedSorting::Alphabetical => blocks.sort_by(|a, b| a.0.partial_cmp(b.0).unwrap()),
            TimedSorting::LongestThreadTime => blocks.sort_by(|a, b| {
                b.1.info
                    .total_thread_time
                    .partial_cmp(&a.1.info.total_thread_time)
                    .unwrap()
            }),
        };

        if option.num_elements > 0 {
            blocks.truncate(option.num_elements);
        }

        blocks
    }

    /// Turns e.g. (Test, 0.642355,1234,56) into "Test [64.2%, 1234ms, 56x]"
    fn format_title(title: &String, percentage: f64, msecs: u128, runs: u64) -> String {
        let result = format!("{title} [{percentage:.1}%, {msecs}ms, {runs}x]");
        result
    }

    /// Create ASCII tree recursively
    fn create_tree_recursive(
        current_layer: usize,
        current_node: &TimedCode,
        title: String,
        options: &[TimedDisplay],
    ) -> Tree {
        const DEFAULT_OPTION: TimedDisplay = TimedDisplay {
            sorting: TimedSorting::Default,
            num_elements: 0,
        };

        let current_option = if current_layer < options.len() {
            &options[current_layer]
        } else {
            &DEFAULT_OPTION
        };

        let mut subnodes = Vec::<Tree>::new();

        let filtered_blocks = TimedCode::apply_display_option(current_node, current_option);
        for block in filtered_blocks {
            let percentage = if current_node.info.total_thread_time > Duration::new(0, 0) {
                100.0
                    * (block.1.info.total_thread_time.as_secs_f64()
                        / current_node.info.total_thread_time.as_secs_f64())
            } else {
                0.0
            };

            subnodes.push(TimedCode::create_tree_recursive(
                current_layer + 1,
                block.1,
                TimedCode::format_title(
                    block.0,
                    percentage,
                    block.1.info.total_thread_time.as_millis(),
                    block.1.info.runs,
                ),
                options,
            ));
        }

        if subnodes.is_empty() {
            Tree::Leaf(vec![title])
        } else {
            Tree::Node(title, subnodes)
        }
    }

    /// Creates an ASCII tree
    pub fn create_tree(&self, title: &str, options: &[TimedDisplay]) -> Tree {
        let title_string: String = format!(
            "{title} [system/process/thread (ms): {}/{}/{}]",
            self.info.total_system_time.as_millis(),
            self.info.total_process_time.as_millis(),
            self.info.total_thread_time.as_millis()
        );
        TimedCode::create_tree_recursive(0, self, title_string, options)
    }

    /// Creates an ASCII tree and converts it to a string representation
    pub fn create_tree_string(&self, title: &str, options: &[TimedDisplay]) -> String {
        let tree = self.create_tree(title, options);

        let mut output = String::new();
        write_tree(&mut output, &tree).expect("Should be fine");

        output
    }
}
