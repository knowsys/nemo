//! The command line arguments of `nmo`.

use std::path::{Path, PathBuf};

use clap::{ArgGroup, Parser, ValueEnum};

/// Runs a Nemo rule program and writes what it derives.
#[derive(Debug, Parser)]
#[command(
    name = "nmo",
    version,
    about,
    long_about = None,
    after_help = "Exit status: 0 on success, 3 if warnings were reported, 1 on an error, \
                  2 for invalid arguments, and 101 on an internal error.",
    // The options that trace something, of which `--trace-output` requires one.
    group = ArgGroup::new("traced")
        .multiple(true)
        .args(["trace", "trace_input", "trace_all", "trace_tree", "trace_node"])
)]
pub struct Arguments {
    /// The rule programs to run, combined into one program in the given order.
    #[arg(required_unless_present = "explain", value_name = "PROGRAM")]
    pub programs: Vec<PathBuf>,

    /// Which predicates to write to files. A predicate without an
    /// `@export` is written as comma-separated values.
    #[arg(short, long, value_enum, default_value_t = Mode::Keep, value_name = "MODE")]
    pub export: Mode,

    /// Print the results to standard output as facts, instead of
    /// writing export files. Without a mode, or with `keep`, it prints
    /// what `--export` selects. If that is `keep`, it also prints the
    /// `@output` predicates, or every derived predicate if the program
    /// declares no outputs and no exports. Another mode, written as
    /// `-P=idb`, selects the predicates by role.
    #[arg(
        short = 'P',
        long,
        alias = "print-facts",
        value_enum,
        value_name = "MODE",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "keep"
    )]
    pub print: Option<Mode>,

    /// The directory exports are written to.
    #[arg(short = 'D', long, default_value = "results", value_name = "DIR")]
    pub export_dir: PathBuf,

    /// Replace export files, the HTML report and the trace file, if they
    /// already exist.
    #[arg(short, long, alias = "overwrite-results")]
    pub overwrite: bool,

    /// Compress every export with gzip.
    #[arg(short, long)]
    pub gzip: bool,

    /// The directory relative imports are resolved against. By default,
    /// this is the directory of the program file that contains the
    /// import.
    #[arg(short = 'I', long, value_name = "DIR")]
    pub import_dir: Option<PathBuf>,

    /// Declare a module root, as `name=directory`. A `@use` then refers
    /// to the module `tc.rls` in that directory as `name::tc`.
    #[arg(long, value_name = "NAME=DIR")]
    pub module_root: Vec<String>,

    /// Give a declared parameter its value, as `name=value`.
    #[arg(short, long, value_name = "NAME=VALUE")]
    pub param: Vec<String>,

    /// The number of worker threads, by default one per CPU core.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..))]
    pub threads: Option<u16>,

    /// How much to report about the run.
    #[arg(long, value_enum, default_value_t = Report::Summary, value_name = "LEVEL")]
    pub report: Report,

    /// Report nothing but warnings and errors (same as `--report none`),
    /// and log errors only.
    #[arg(short, long, conflicts_with = "report")]
    pub quiet: bool,

    /// Hide warnings, and exit with 0 instead of 3 if there are any.
    #[arg(long)]
    pub no_warnings: bool,

    /// Log more to standard error: `-v` for what the run does, `-vv`
    /// for the engine's decisions, and `-vvv` for everything, including
    /// the telemetry stream.
    #[arg(short, long, action = clap::ArgAction::Count, conflicts_with = "log")]
    pub verbose: u8,

    /// Log by filter directives, as `debug` or `nemo::io=debug,warn`.
    /// The `NMO_LOG` environment variable sets the default.
    #[arg(long, value_name = "FILTER")]
    pub log: Option<String>,

    /// Write the engine's telemetry stream to this file as JSON lines,
    /// one event per line.
    #[arg(long, value_name = "FILE")]
    pub events: Option<PathBuf>,

    /// Serve a live dashboard of the run on this port, written as
    /// `--dashboard=PORT` (0 picks a free port, and no port means 8280).
    /// Nemo keeps serving it until interrupted.
    #[arg(
        long,
        value_name = "PORT",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "8280"
    )]
    pub dashboard: Option<u16>,

    /// Explain why facts hold after reasoning. Separate several facts
    /// with `;`, as in `--trace "reach(a, b);reach(a, c)"`. Proof trees
    /// are printed to standard output unless `--trace-output` is given.
    #[arg(long, value_name = "FACTS")]
    pub trace: Vec<String>,

    /// Read facts to trace from a file (separated by `;`, as for
    /// `--trace`). Repeatable.
    #[arg(long, alias = "trace-input-file", value_name = "FILE")]
    pub trace_input: Vec<PathBuf>,

    /// Trace every fact of every derived predicate. Each fact is printed
    /// once, after its premises: a derived fact as the rule instance that
    /// derives it, a loaded one as a fact statement. With `--trace-output`,
    /// the trace goes into that file as one trace response.
    #[arg(long, alias = "trace-all-idb-facts")]
    pub trace_all: bool,

    /// Answer a `treeForTable` query of the explanation view, given as
    /// a JSON file or as inline JSON. The response is printed to
    /// standard output unless `--trace-output` is given. Cannot be
    /// combined with the other trace options.
    #[arg(
        long,
        value_name = "FILE|JSON",
        conflicts_with_all = ["trace", "trace_input", "trace_all", "trace_node"]
    )]
    pub trace_tree: Option<String>,

    /// Answer a `tableEntriesForTreeNodes` query of the explanation
    /// view, given and printed as for `--trace-tree`.
    #[arg(
        long,
        value_name = "FILE|JSON",
        conflicts_with_all = ["trace", "trace_input", "trace_all"]
    )]
    pub trace_node: Option<String>,

    /// Write the trace as JSON to this file instead of printing it. For
    /// `--trace` and `--trace-all`, the file contains the derivations as
    /// one graph of facts. For `--trace-tree` and `--trace-node`, it
    /// contains the query's response.
    #[arg(long, value_name = "FILE", requires = "traced")]
    pub trace_output: Option<PathBuf>,

    /// Explain a diagnostic code, as `--explain N0204`, and do nothing
    /// else.
    #[arg(long, value_name = "CODE")]
    pub explain: Option<String>,
}

impl Arguments {
    /// Returns the report level, after applying `--quiet` and resolving `auto`.
    ///
    /// `auto` becomes the summary, or nothing if the results or a trace are printed,
    /// so that the terminal shows only the data.
    pub fn reporting(&self) -> Report {
        let prints = self.print.is_some_and(|mode| mode != Mode::None) || self.traces_to_stdout();
        match self.report {
            _ if self.quiet => Report::None,
            Report::Auto if prints => Report::None,
            Report::Auto => Report::Summary,
            level => level,
        }
    }

    /// Returns whether the run traces anything.
    pub fn traces(&self) -> bool {
        !self.trace.is_empty()
            || !self.trace_input.is_empty()
            || self.trace_all
            || self.trace_tree.is_some()
            || self.trace_node.is_some()
    }

    /// Returns whether the run prints a trace to standard output,
    /// which it does for any trace without `--trace-output`.
    pub fn traces_to_stdout(&self) -> bool {
        self.traces() && self.trace_output.is_none()
    }

    /// Returns whether the run exports or prints the extensional predicates.
    ///
    /// Their imported tables are then written as they were read,
    /// so the imports must be loaded whole.
    pub fn selects_extensional(&self) -> bool {
        matches!(self.export, Mode::All | Mode::Extensional)
            || matches!(self.print, Some(Mode::All | Mode::Extensional))
    }

    /// Returns the print mode, or `None` if the run does not print.
    pub fn printing(&self) -> Option<Mode> {
        self.print.filter(|mode| *mode != Mode::None)
    }

    /// Returns whether a program path looks like the value of an option written without `=`.
    ///
    /// Such a value is a print mode, as in `-P idb`, or a port, as in `--dashboard 8080`.
    /// Both options need `=` before their value, so clap takes the value for a program path.
    pub fn is_option_value(path: &Path) -> bool {
        path.to_str()
            .is_some_and(|text| Mode::from_str(text, true).is_ok() || text.parse::<u16>().is_ok())
    }
}

/// Which predicates a run writes out or prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// What the program declares, or, when printing, what `--export` selects.
    Keep,
    /// Every predicate that rules derive facts for.
    #[value(name = "idb")]
    Derived,
    /// Every predicate with input facts, from facts, imports or `@input`.
    #[value(name = "edb")]
    Extensional,
    /// Every predicate.
    All,
    /// Nothing.
    None,
}

/// How much a run reports about itself.
///
/// Everything but the JSON document goes to the error stream,
/// so that standard output is left for the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Report {
    /// Warnings and errors only.
    None,
    /// The summary, or nothing if the results or a trace are printed.
    Auto,
    /// The headline, and how many facts each output predicate holds.
    #[value(alias = "short")]
    Summary,
    /// The stage times, a breakdown of the reasoning time, and the size
    /// of every predicate. It records no telemetry, which suits
    /// benchmarks.
    #[value(alias = "time", alias = "mem")]
    Timing,
    /// The timing report plus the rules that took the most time. It
    /// records the run's telemetry.
    #[value(alias = "all")]
    Full,
    /// The whole run as one JSON document on standard output.
    /// Diagnostics still go to the error stream.
    Json,
    /// The summary, plus a visual `report.html` written into the
    /// export directory.
    Html,
}

impl Report {
    /// Returns whether this level reads the engine's telemetry stream.
    ///
    /// `auto` must be resolved through [Arguments::reporting] first.
    pub fn observes(self) -> bool {
        matches!(self, Report::Full | Report::Json | Report::Html)
    }

    /// Returns whether this level prints the stage times and phases.
    pub fn times(self) -> bool {
        matches!(self, Report::Timing | Report::Full)
    }
}
