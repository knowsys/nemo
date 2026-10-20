//! The kinds of problems the command line reports.

use nemo_physical::{diagnostic_kinds, diagnostics::Count};

diagnostic_kinds! {
    /// A problem with a run's arguments or with what the run writes.
    pub enum CliError {
        InvalidSetting { setting: String }
            => INVALID_SETTING = 900, error, "invalid parameter setting `{setting}`",
               explain "A `--param` setting is not of the form `name=value`, or its value is \
                        not one term.",
               hint "write a setting as `name=value` with one term as the value, as in \
                     `--param threshold=5`";
        StdoutConflict { claimant: String }
            => STDOUT_CONFLICT = 901, error,
               "{claimant} and `--report json` cannot both write to standard output",
               explain "`--report json` writes its document to standard output, and \
                        something else in the run writes there too: the printed results, \
                        a trace that is printed instead of written to `--trace-output`, \
                        or an export with an empty resource.";
        DashboardUnavailable { port: u16, reason: String }
            => DASHBOARD_UNAVAILABLE = 902, error,
               "could not start the dashboard on port {port}: {reason}",
               explain "The dashboard could not listen on its port.",
               hint "choose another port with `--dashboard=PORT`, or `--dashboard=0` for any \
                     free port";
        ReportUnwritable { path: String, reason: String }
            => REPORT_UNWRITABLE = 903, error,
               "could not write the report to `{path}`: {reason}",
               explain "The HTML report could not be written.";
        TraceInputUnreadable { path: String, reason: String }
            => TRACE_INPUT_UNREADABLE = 904, error,
               "could not read the trace input `{path}`: {reason}",
               explain "A `--trace-input` file could not be read.";
        NotAFact { text: String, reason: String }
            => NOT_A_FACT = 905, error, "`{text}` is not a fact: {reason}",
               explain "A text given to `--trace`, or one part of a `--trace-input` file \
                        between two `;`, is not a fact.",
               hint "separate several facts with `;`, as `reach(a, b); reach(a, c)`";
        TraceUnwritable { path: String, reason: String }
            => TRACE_UNWRITABLE = 906, error,
               "could not write the trace to `{path}`: {reason}",
               explain "The `--trace-output` file could not be written.";
        UndeclaredParameter { name: String }
            => UNDECLARED_PARAMETER = 907, error, "the program has no parameter `${name}`",
               explain "`--param` sets a parameter that the program does not declare.",
               hint "declare the parameter with `@parameter` before setting it";
        ExportCollision {
            /// What would write the file, as `` `p` and `q` ``.
            claimants: String,
            /// The file, as the export directory names it.
            file: String,
        } => EXPORT_COLLISION = 908, error, "{claimants} would both be written to `{file}`",
               explain "Two exports, or an export and the HTML report, would write the same \
                        file.";
        ExportExists { path: String }
            => EXPORT_EXISTS = 909, error, "file `{path}` already exists",
               explain "An export, or the HTML report, would replace a file that already \
                        exists.",
               hint "pass `--overwrite` to replace existing files";
        DroppedDerivations { count: Count, kind: String }
            => DROPPED_DERIVATIONS = 910, warning,
               "dropped {count} derivation{count:#}: {kind}",
               explain "Rule evaluation dropped derivations whose values it could not \
                        compute, for example because of a division by zero. The message \
                        names the reason.";
        PrintFailed { reason: String }
            => PRINT_FAILED = 911, error, "could not print the results: {reason}",
               explain "The results could not be written to standard output.";
        InvalidLogFilter { directives: String, reason: String }
            => INVALID_LOG_FILTER = 912, error,
               "invalid log filter `{directives}`: {reason}",
               explain "The `--log` option or the `NMO_LOG` variable is not a valid log \
                        filter.",
               hint "write a level, as `debug`, or a list of `target=level` directives, as \
                     `nemo::io=debug,warn`";
        EventsUnwritable { path: String, reason: String }
            => EVENTS_UNWRITABLE = 913, error,
               "could not create the event log `{path}`: {reason}",
               explain "The `--events` file could not be created.";
        UnknownCode { code: String }
            => UNKNOWN_CODE = 914, error, "unknown diagnostic code `{code}`",
               explain "The code given to `--explain` is not one that Nemo reports.";
        TraceQueryUnreadable { path: String, reason: String }
            => TRACE_QUERY_UNREADABLE = 915, error,
               "could not read the trace query `{path}`: {reason}",
               explain "The query given to `--trace-tree` or `--trace-node` could not be \
                        read, or it is not a query of the explanation view's protocol.";
        SkippedPredicates {
            /// How many predicates were skipped.
            count: Count,
            /// The skipped predicates, each in backticks.
            predicates: String,
        } => SKIPPED_PREDICATES = 916, warning,
               "skipped {count} predicate{count:#} not stored whole: {predicates}",
               explain "A selection by role (`all`, `idb`, or `edb`) includes predicates \
                        whose facts are not stored whole, because the rules read them \
                        directly at their source. The run left them out.",
               hint "declare a predicate `@output` to store it whole";
        InvalidModuleRoot { setting: String }
            => INVALID_MODULE_ROOT = 917, error, "invalid module root `{setting}`",
               explain "A `--module-root` setting is not of the form `name=directory`.",
               hint "write a module root as `name=directory`, as in \
                     `--module-root shared=../lib`";
    }
}
