//! Code for timing blocks of code

use ascii_tree::{write_tree, Tree};
use linked_hash_map::LinkedHashMap;
use once_cell::sync::Lazy;
use std::{
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

/// Global instance of the [`TimeCode`]
pub static TIMECODE_INSTANCE: Lazy<Mutex<TimedCode>> = Lazy::new(|| {
    let instance = TimedCode::new();
    Mutex::new(instance)
});

/// Represents a block of code that is timed
#[derive(Debug, Clone)]
pub struct TimedCodeInfo {
    accumulated_time: Duration,
    individual_times: Vec<Duration>,

    start_time: Option<Instant>,
}

impl TimedCodeInfo {
    /// Create new [`TimedCodeInfo`] object
    pub fn new() -> Self {
        Self {
            accumulated_time: Duration::from_secs(0),
            individual_times: Vec::new(),
            start_time: None,
        }
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
    LongestTime,
}

/// How to display a layer of a [`TimedCode`] object
#[derive(Debug, Copy, Clone)]
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

    /// Create default display option
    pub fn default() -> Self {
        Self {
            sorting: TimedSorting::Default,
            num_elements: 0,
        }
    }
}

/// Represents a block of code that is timed
#[derive(Debug, Clone)]
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

        let name_parts: Vec<&str> = name.split("/").collect();

        let mut current_block = self;
        for part in name_parts {
            let part_string = String::from_str(part).expect("We expect well formed strings");
            current_block = current_block
                .subblocks
                .entry(part_string)
                .or_insert_with(|| TimedCode::new())
        }

        current_block
    }

    /// Start the next measurement
    pub fn start(&mut self) {
        if cfg!(test) {
            return;
        }

        debug_assert!(self.info.start_time.is_none());

        self.info.start_time = Some(Instant::now());
    }

    /// Stop the current measurement and save the times
    pub fn stop(&mut self) {
        if cfg!(test) {
            return;
        }

        debug_assert!(self.info.start_time.is_some());

        let start = self
            .info
            .start_time
            .expect("When calling stop it is assumed that start was called before");
        let now = Instant::now();

        let duration = now - start;

        self.info.accumulated_time += duration;
        self.info.individual_times.push(duration);
        self.info.start_time = None;
    }

    fn apply_display_option<'a>(
        code: &'a TimedCode,
        option: &TimedDisplay,
    ) -> Vec<(&'a String, &'a TimedCode)> {
        let mut blocks: Vec<(&'a String, &'a TimedCode)> = code.subblocks.iter().collect();

        match option.sorting {
            TimedSorting::Default => {}
            TimedSorting::Alphabetical => blocks.sort_by(|a, b| a.0.partial_cmp(b.0).unwrap()),
            TimedSorting::LongestTime => blocks.sort_by(|a, b| {
                b.1.info
                    .accumulated_time
                    .partial_cmp(&a.1.info.accumulated_time)
                    .unwrap()
            }),
        };

        if option.num_elements > 0 {
            blocks.truncate(option.num_elements);
        }

        blocks
    }

    /// Turns e.g. (Test, 0.642355) into "Test [64.2%]"
    fn format_title(title: &String, percentage: f64) -> String {
        let percentage_string = format!("{:.1}", percentage);

        let mut result = String::clone(title);
        result.push_str(" [");
        result.push_str(&percentage_string);
        result.push_str("%]");

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
            let percentage = if current_node.info.accumulated_time > Duration::new(0, 0) {
                100.0
                    * (block.1.info.accumulated_time.as_secs_f64()
                        / current_node.info.accumulated_time.as_secs_f64())
                // block.1.info.accumulated_time.as_secs_f64()
            } else {
                0.0
            };

            subnodes.push(TimedCode::create_tree_recursive(
                current_layer + 1,
                block.1,
                TimedCode::format_title(block.0, percentage),
                options,
            ));
        }

        if subnodes.len() == 0 {
            Tree::Leaf(vec![title])
        } else {
            Tree::Node(title, subnodes)
        }
    }

    /// Creates an ASCII tree
    pub fn create_tree(&self, title: &str, options: &[TimedDisplay]) -> Tree {
        let mut title_string = String::from_str(title).unwrap();
        title_string.push_str(" [");
        title_string.push_str(&self.info.accumulated_time.as_millis().to_string());
        title_string.push_str(" ms]");

        TimedCode::create_tree_recursive(0, &self, title_string, options)
    }

    /// Creates an ASCII tree and converts it to a string representation
    pub fn create_tree_string(&self, title: &str, options: &[TimedDisplay]) -> String {
        let tree = self.create_tree(title, options);

        let mut output = String::new();
        write_tree(&mut output, &tree).expect("Should be fine");

        output
    }
}
