//! Code for timing blocks of code

use ascii_tree::{Tree, write_tree};
#[cfg(not(target_family = "wasm"))]
use cpu_time::ProcessTime;

#[cfg(all(not(test), not(target_family = "wasm")))]
use cpu_time::ThreadTime;
use linked_hash_map::LinkedHashMap;
use once_cell::sync::Lazy;
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;
use std::{
    cmp::Reverse,
    fmt::{self, Display},
    ops::AddAssign,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::Duration,
};
use tracing::{
    Level, Subscriber, event,
    span::{self, Id},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;

/// Global instance of the [TimedCode]
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
    start_system: Option<Instant>,
    #[cfg(not(target_family = "wasm"))]
    #[allow(dead_code)]
    start_process: Option<ProcessTime>,
    #[allow(dead_code)]
    #[cfg(not(target_family = "wasm"))]
    start_thread: Option<Duration>,
    runs: u64,
}

impl TimedCodeInfo {
    /// Create new [TimedCodeInfo] object
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total system time for this node.
    pub fn system_time(&self) -> Duration {
        self.total_system_time
    }

    /// Returns the total process time for this node
    pub fn process_time(&self) -> Duration {
        self.total_process_time
    }

    /// Returns the total thread time for this node
    pub fn thread_time(&self) -> Duration {
        self.total_thread_time
    }
}

impl fmt::Debug for TimedCodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let run_msg = if self.start_system.is_some() {
            "currently running"
        } else {
            "currently not running"
        };
        write!(
            f,
            "TimedCodeInfo [totals (msec): {}/{}/{}, {:?} completed runs, {}]",
            self.total_system_time.as_millis(),
            self.total_process_time.as_millis(),
            self.total_thread_time.as_millis(),
            self.runs,
            run_msg,
        )
    }
}

/// How to sort the elements of a [TimedCode] object
#[derive(Debug, Copy, Clone, Default)]
pub enum TimedSorting {
    /// The order the code got called in
    #[default]
    Default,
    /// Alphabetical by the title of the block
    Alphabetical,
    /// Show the blocks which took longest first
    LongestThreadTime,
}

/// How to display a layer of a [TimedCode] object
#[derive(Debug, Default, Copy, Clone)]
pub struct TimedDisplay {
    sorting: TimedSorting,
    num_elements: usize,
}

impl TimedDisplay {
    /// Create new [TimedDisplay]
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
    /// Create new [TimedCode] object
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

    /// Return an iterator through the sub-nodes
    pub fn sub_nodes(&self) -> impl Iterator<Item = (&str, &TimedCode)> {
        self.subblocks.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Return the recorded timings for this block
    pub fn timings(&self) -> &TimedCodeInfo {
        &self.info
    }

    /// Reset the current node, remove all subnodes
    pub fn reset(&mut self) {
        self.info = Default::default();
        self.subblocks.clear();
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
            current_block = current_block.subblocks.entry(part_string).or_default()
        }

        current_block
    }

    /// Return the total system time this block took.
    pub fn total_system_time(&self) -> Duration {
        self.info.total_system_time
    }

    /// No-op because time measurement is disabled
    #[cfg(test)]
    pub fn start(&mut self) {}

    /// Start the next measurement
    #[cfg(all(not(test), not(target_family = "wasm")))]
    pub fn start(&mut self) {
        debug_assert!(self.info.start_thread.is_none());

        self.info.start_system = Some(Instant::now());
        self.info.start_process = Some(ProcessTime::now());
        self.info.start_thread = Some(ThreadTime::now().as_duration());
    }

    /// Start the next measurement
    #[cfg(all(not(test), target_family = "wasm"))]
    pub fn start(&mut self) {
        debug_assert!(self.info.start_system.is_none());

        self.info.start_system = Some(Instant::now());
    }

    /// No-op because time measurement is disabled
    #[cfg(test)]
    pub fn stop(&mut self) -> Duration {
        Duration::ZERO
    }

    /// Stop the current measurement and save the times
    #[cfg(all(not(test), not(target_family = "wasm")))]
    pub fn stop(&mut self) -> Duration {
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

        let duration_system = Instant::now() - start_system;
        let duration_process = ProcessTime::now().duration_since(start_process);
        let duration_thread = ThreadTime::now().as_duration() - start_thread;
        self.info.total_system_time += duration_system;
        self.info.total_process_time += duration_process;
        self.info.total_thread_time += duration_thread;
        self.info.start_system = None;
        self.info.start_process = None;
        self.info.start_thread = None;
        self.info.runs += 1;

        duration_thread
    }

    /// Stop the current measurement and save the times
    #[cfg(all(not(test), target_family = "wasm"))]
    pub fn stop(&mut self) -> Duration {
        debug_assert!(self.info.start_system.is_some());

        let start_system = self
            .info
            .start_system
            .expect("start() must be called before calling stop()");

        let duration_system = Instant::now() - start_system;
        self.info.total_system_time += duration_system;
        self.info.start_system = None;
        self.info.runs += 1;

        duration_system
    }

    fn apply_display_option<'a>(
        code: &'a TimedCode,
        option: &TimedDisplay,
    ) -> Vec<(&'a String, &'a TimedCode)> {
        let mut blocks: Vec<(&'a String, &'a TimedCode)> = code.subblocks.iter().collect();

        fn name<'a>(entry: &(&'a String, &TimedCode)) -> &'a String {
            entry.0
        }

        #[cfg(not(target_family = "wasm"))]
        fn longest_time(entry: &(&String, &TimedCode)) -> Reverse<Duration> {
            Reverse(entry.1.info.total_thread_time)
        }
        #[cfg(target_family = "wasm")]
        fn longest_time(entry: &(&String, &TimedCode)) -> Reverse<Duration> {
            Reverse(entry.1.info.total_system_time)
        }

        match option.sorting {
            TimedSorting::Default => {}
            TimedSorting::Alphabetical => blocks.sort_by_key(name),
            TimedSorting::LongestThreadTime => blocks.sort_by_key(longest_time),
        };

        if option.num_elements > 0 {
            blocks.truncate(option.num_elements);
        }

        blocks
    }

    /// Turns e.g. (Test, 0.642355,1234,56) into "Test [64.2%, 1234ms, 56x]"
    fn format_title(title: &str, percentage: f64, msecs: u128, runs: u64) -> String {
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
            let percentage = if current_node.info.total_system_time > Duration::new(0, 0) {
                100.0
                    * (block.1.info.total_system_time.as_secs_f64()
                        / current_node.info.total_system_time.as_secs_f64())
            } else {
                0.0
            };

            subnodes.push(TimedCode::create_tree_recursive(
                current_layer + 1,
                block.1,
                TimedCode::format_title(
                    block.0,
                    percentage,
                    block.1.info.total_system_time.as_millis(),
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

#[derive(Debug, Copy, Clone)]
pub struct TimingLayer {}

#[derive(Debug, Clone, Default)]
struct TimingRecord {
    system: Duration,
    process: Duration,
    thread: Duration,
}

impl Display for TimingRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[system/process/thread (ms): {}/{}/{}]",
            self.system.as_millis(),
            self.process.as_millis(),
            self.thread.as_millis()
        )
    }
}

impl AddAssign for TimingRecord {
    fn add_assign(&mut self, rhs: Self) {
        self.system += rhs.system;
        self.process += rhs.process;
        self.thread += rhs.thread;
    }
}

#[derive(Debug, Clone)]
struct IdleTime(TimingRecord);

#[derive(Debug, Clone)]
struct BusyTime(TimingRecord);

#[derive(Debug, Clone)]
struct Timings {
    entered: usize,
    last_system: Instant,
    last_process: ProcessTime,
    last_thread: Duration,
    idle: TimingRecord,
    busy: TimingRecord,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            entered: 0,
            last_system: Instant::now(),
            last_process: ProcessTime::now(),
            //last_thread: cpu_time::ThreadTime::now().as_duration(),
            last_thread: Default::default(),
            idle: Default::default(),
            busy: Default::default(),
        }
    }
}

impl Timings {
    fn new() -> Self {
        Default::default()
    }

    fn since_last(&mut self) -> TimingRecord {
        let system = Instant::now();
        let process = ProcessTime::now();
        let thread = cpu_time::ThreadTime::now().as_duration();

        let record = TimingRecord {
            system: system.duration_since(self.last_system),
            process: process.duration_since(self.last_process),
            thread: thread - self.last_thread,
        };

        self.last_system = system;
        self.last_process = process;
        self.last_thread = thread;

        record
    }

    fn enter(&mut self) {
        if self.entered == 0 {
            let last = self.since_last();
            self.idle += last;
        }
        self.entered += 1;
    }

    fn exit(&mut self) {
        self.entered -= 1;
        if self.entered == 0 {
            let last = self.since_last();
            self.busy += last;
        }
    }

    fn close(&mut self) -> (IdleTime, BusyTime) {
        let last = self.since_last();
        self.idle += last;

        (IdleTime(self.idle.clone()), BusyTime(self.busy.clone()))
    }
}

impl<S> Layer<S> for TimingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span exists");
        let mut extensions = span.extensions_mut();
        extensions.insert(Timings::new());
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span exists");

        for span in span.scope() {
            if let Some(timings) = span.extensions_mut().get_mut::<Timings>() {
                timings.enter()
            }
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span exists");

        for span in span.scope() {
            if let Some(timings) = span.extensions_mut().get_mut::<Timings>() {
                timings.exit()
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span exists");
        if let Some(timings) = span.extensions_mut().get_mut::<Timings>() {
            let (idle, busy) = timings.close();
            if let Some(parent) = span.parent() {
                event!(parent: parent.id(), Level::INFO, name = span.name(), time.idle = %idle.0, time.busy = %busy.0);
            } else {
                event!(Level::INFO, name = span.name(), time.idle = %idle.0, time.busy = %busy.0);
            }
        }
    }
}
