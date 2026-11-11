//! The text summary of a run, printed to the error stream in as much detail as the report
//! level selects.

use std::{collections::HashSet, time::Duration};

use nemo::{
    codes::NOT_STORED_WHOLE,
    diagnostics::render::TerminalStyle,
    engine::{Engine, ExportSpec},
    telemetry::{PhaseTable, Recorder},
};

use crate::{
    arguments::Report,
    run::{Part, Run, Times},
};

/// The number of rules in the summary's table of rule times, which lists the longest-running
/// rules first.
const RULES_LISTED: usize = 20;

impl Run<'_> {
    /// Prints the summary of the run to the error stream,
    /// in as much detail as the report level selects.
    ///
    /// It ends with the export directory, if the run wrote one of `exports` to a file.
    /// Standard output is left for the data.
    pub(super) fn summarize(
        &self,
        engine: &Engine,
        exports: &[ExportSpec],
        times: Times,
        recorder: Option<&Recorder>,
    ) {
        let level = self.arguments.reporting();
        // At the JSON level, the JSON document is the only report.
        if level == Report::None || level == Report::Json {
            return;
        }

        self.begin(Part::Summary);

        let TerminalStyle { strong, reset, .. } = TerminalStyle::detect();
        let derived = engine.derived_facts();
        eprintln!(
            "Finished in {strong}{}{reset} and derived {strong}{}{reset} fact{}.",
            readable(times.total),
            separated(derived as u64),
            if derived == 1 { "" } else { "s" },
        );

        if level.times() {
            let milliseconds = |duration: Duration| duration.as_secs_f64() * 1e3;
            eprintln!();
            eprintln!("parse   {:>12.3} ms", milliseconds(times.parse));
            eprintln!("load    {:>12.3} ms", milliseconds(times.load));
            eprintln!("reason  {:>12.3} ms", milliseconds(times.reason));
            eprintln!("export  {:>12.3} ms", milliseconds(times.export));
            eprintln!("total   {:>12.3} ms", milliseconds(times.total));
            eprintln!(
                "memory  {:>12.1} MB",
                engine.memory_bytes() as f64 / (1024.0 * 1024.0)
            );
            eprintln!("applied {:>12}", engine.applications());
            eprintln!("loaded  {:>12}", engine.loaded_facts());
            Self::phases(engine);
            if let Some(recorder) = recorder {
                Self::rules(recorder);
            }
        }

        self.counts(engine, level);

        if exports.iter().any(|export| !export.is_stdout()) {
            // The directory is shown with a separator at its end.
            let directory = self.arguments.export_dir.join("");
            eprintln!("exports: {}", directory.display());
        }
    }

    /// Prints the [RULES_LISTED] applied rules that took the most time,
    /// each with its time, applications, derived facts and dropped rows.
    fn rules(recorder: &Recorder) {
        let mut rules = recorder.rules();
        rules.retain(|rule| rule.applications > 0);
        if rules.is_empty() {
            return;
        }
        rules.sort_by_key(|rule| std::cmp::Reverse(rule.duration));
        rules.truncate(RULES_LISTED);

        eprintln!();
        eprintln!(
            "{:>10}  {:>8}  {:>12}  {:>8}  rule",
            "time", "applied", "derived", "dropped"
        );
        for rule in rules {
            // Each rule takes one line, so its whitespace is collapsed to single spaces,
            // and a long rule is truncated.
            let text: String = rule.text.split_whitespace().collect::<Vec<_>>().join(" ");
            let text: String = if text.chars().count() > 72 {
                text.chars().take(69).chain("...".chars()).collect()
            } else {
                text
            };
            eprintln!(
                "{:>7.1} ms  {:>8}  {:>12}  {:>8}  {text}",
                rule.duration as f64 / 1e6,
                rule.applications,
                separated(rule.derived),
                rule.dropped,
            );
        }
    }

    /// Prints the number of facts of each of the program's [outputs](Engine::outputs).
    ///
    /// It lists every predicate instead if the report level prints times,
    /// or if the run writes or prints the extensional predicates.
    fn counts(&self, engine: &Engine, level: Report) {
        let mut predicates = engine.predicates();
        if !level.times() && !self.arguments.selects_extensional() {
            let outputs: HashSet<String> = engine.outputs().into_iter().collect();
            predicates.retain(|info| outputs.contains(&info.name));
        }
        if predicates.is_empty() {
            return;
        }

        // IRI predicates can be long, so the column of counts starts after the longest name
        // of this run.
        let width = predicates
            .iter()
            .map(|info| info.name.len())
            .max()
            .expect("a non-empty list has a longest name")
            + 2;
        eprintln!();
        for info in &predicates {
            let predicate = &info.name;
            // The run has already succeeded, so a predicate whose rows cannot be counted is
            // reported without failing the run. A predicate that the engine does not store
            // whole has no count, and is left out.
            match engine.count(predicate) {
                Ok(count) => eprintln!("{predicate:<width$}{:>12}", separated(count as u64)),
                Err(problem) if problem.code() == NOT_STORED_WHOLE => {}
                Err(problem) => self.show(&problem),
            }
        }
    }

    /// Prints how the reasoning time splits into phases, and counts of the work done.
    fn phases(engine: &Engine) {
        let timings = engine.timings();
        let milliseconds = |nanoseconds: u64| nanoseconds as f64 / 1e6;
        eprintln!();
        for (phase, nanoseconds) in PhaseTable::of_run(&timings).rows() {
            eprintln!("{phase:<12}{:>10.1} ms", milliseconds(*nanoseconds));
        }
        eprintln!();
        for (work, count) in [
            ("joins", timings.counts.joins),
            ("blocks", timings.counts.blocks),
            ("rows", timings.counts.rows),
            ("arrangements", timings.counts.arrangements),
            ("arranged", timings.counts.arranged_rows),
            ("consolidated", timings.consolidated_rows),
            ("orders", timings.stored_orders),
        ] {
            eprintln!("{work:<14}{count:>12}");
        }
        eprintln!(
            "{:<14}{:>12.1} MB",
            "buffer peak",
            timings.counts.buffer_bytes as f64 / (1024.0 * 1024.0)
        );
    }
}

/// Returns a count with thousands separators, as `21,458`.
fn separated(count: u64) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// Returns a duration as text in a unit that suits its size, as `412 ms` or `1.24 s`.
fn readable(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds < 0.01 {
        format!("{:.1} ms", seconds * 1e3)
    } else if seconds < 1.0 {
        format!("{:.0} ms", seconds * 1e3)
    } else if seconds < 10.0 {
        format!("{seconds:.2} s")
    } else if seconds < 60.0 {
        format!("{seconds:.1} s")
    } else {
        let whole = duration.as_secs();
        format!("{} min {} s", whole / 60, whole % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A count is written with a comma between groups of three digits.
    #[test]
    fn counts_group_their_digits_in_threes() {
        assert_eq!(separated(0), "0");
        assert_eq!(separated(999), "999");
        assert_eq!(separated(21_458), "21,458");
        assert_eq!(separated(1_234_567), "1,234,567");
    }

    /// A duration is written in milliseconds, in seconds, or in minutes and seconds,
    /// with fewer decimals the larger it is.
    #[test]
    fn durations_take_the_unit_that_reads_best() {
        assert_eq!(readable(Duration::from_micros(1_700)), "1.7 ms");
        assert_eq!(readable(Duration::from_micros(412_300)), "412 ms");
        assert_eq!(readable(Duration::from_millis(1_243)), "1.24 s");
        assert_eq!(readable(Duration::from_millis(12_400)), "12.4 s");
        assert_eq!(readable(Duration::from_secs(124)), "2 min 4 s");
    }
}
