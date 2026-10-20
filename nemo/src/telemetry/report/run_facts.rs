//! The account of a finished run beyond the engine's event stream.

use serde::{Serialize, Serializer};

use crate::telemetry::{
    EngineTimings,
    report::tables::{NamedCounts, PhaseTable, WorkTable},
};

/// The facts about a finished run that the engine's event stream does not carry:
/// stage wall times, the final sizes, and per-predicate row counts.
///
/// This is the part of the [document](crate::telemetry::ReportDocument)
/// that exists only once the run is over.
/// Times are in nanoseconds and sizes in bytes.
#[derive(Debug, Clone, Default)]
pub struct RunFacts {
    /// Reading and parsing the programs.
    pub parse: u64,
    /// Loading the imported data.
    pub load: u64,
    /// Reasoning to the fixpoint.
    pub reason: u64,
    /// Writing the exports.
    pub export: u64,
    /// The whole run.
    pub total: u64,
    /// Facts stored by loading.
    pub loaded: u64,
    /// New facts derived beyond the loaded ones.
    pub derived: u64,
    /// The rule applications performed,
    /// as the engine [counts them](crate::engine::Engine::applications).
    pub applications: u64,
    /// Bytes across stored tables and the dictionary.
    pub memory: u64,
    /// The engine's cumulative wall-clock accounting.
    pub timings: EngineTimings,
    /// Evaluation errors by kind, nonzero kinds only.
    pub errors: Vec<ErrorCount>,
    /// The stored predicates.
    pub predicates: Vec<PredicateSize>,
    /// The program as loaded and after every stage that rewrote it, in order:
    /// what the engine ran, and how it got there.
    pub programs: Vec<ProgramText>,
}

/// The evaluation errors of one kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorCount {
    /// The kind's name.
    pub kind: String,
    /// How many rows errors of this kind dropped.
    pub count: u64,
}

/// The size of one stored predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PredicateSize {
    /// The predicate's name.
    pub name: String,
    /// Its number of columns.
    pub columns: usize,
    /// Its number of rows.
    pub rows: u64,
}

/// The program after one pipeline stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProgramText {
    /// The stage's name, `"loaded"` for the program as loaded.
    pub stage: String,
    /// The program's text.
    pub text: String,
}

/// The stage wall times, as the document nests them.
#[derive(Serialize)]
struct StageTimes {
    /// Reading and parsing the programs.
    parse: u64,
    /// Loading the imported data.
    load: u64,
    /// Reasoning to the fixpoint.
    reason: u64,
    /// Writing the exports.
    export: u64,
    /// The whole run.
    total: u64,
}

/// The facts as the document lays them out.
#[derive(Serialize)]
struct FactsView<'a> {
    /// The stage wall times.
    stages: StageTimes,
    /// Facts stored by loading.
    loaded: u64,
    /// New facts derived beyond the loaded ones.
    derived: u64,
    /// The rule applications performed.
    applications: u64,
    /// Bytes across stored tables and the dictionary.
    memory: u64,
    /// The phase table of the run.
    phases: NamedCounts<'a>,
    /// The work table of the run.
    counts: NamedCounts<'a>,
    /// Evaluation errors by kind.
    errors: &'a [ErrorCount],
    /// The stored predicates.
    predicates: &'a [PredicateSize],
    /// The program after every stage.
    programs: &'a [ProgramText],
}

impl Serialize for RunFacts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let phases = PhaseTable::of_run(&self.timings);
        let counts = WorkTable::of_run(&self.timings);
        FactsView {
            stages: StageTimes {
                parse: self.parse,
                load: self.load,
                reason: self.reason,
                export: self.export,
                total: self.total,
            },
            loaded: self.loaded,
            derived: self.derived,
            applications: self.applications,
            memory: self.memory,
            phases: NamedCounts(phases.rows()),
            counts: NamedCounts(counts.rows()),
            errors: &self.errors,
            predicates: &self.predicates,
            programs: &self.programs,
        }
        .serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::plan::{PhaseTimings, WorkCounts};

    use super::*;

    /// Creates an error count.
    fn error(kind: &str, count: u64) -> ErrorCount {
        ErrorCount {
            kind: kind.to_owned(),
            count,
        }
    }

    /// Creates a predicate size.
    fn size(name: &str, columns: usize, rows: u64) -> PredicateSize {
        PredicateSize {
            name: name.to_owned(),
            columns,
            rows,
        }
    }

    /// Creates a program text.
    fn text(stage: &str, text: &str) -> ProgramText {
        ProgramText {
            stage: stage.to_owned(),
            text: text.to_owned(),
        }
    }

    #[test]
    fn facts_serialize_their_sections() {
        let mut facts = RunFacts {
            parse: 1,
            total: 5,
            derived: 7,
            ..RunFacts::default()
        };
        facts.errors.push(error("division by zero", 2));
        facts.predicates.push(size("p", 2, 9));
        facts.programs.push(text("loaded", "p(1) .\n"));
        let out = serde_json::to_string(&facts).expect("the facts serialize");
        assert!(
            out.contains("\"programs\":[{\"stage\":\"loaded\",\"text\":\"p(1) .\\n\"}]"),
            "{out}"
        );
        assert!(out.starts_with("{\"stages\":{\"parse\":1,"), "{out}");
        assert!(out.contains("\"derived\":7"), "{out}");
        assert!(
            out.contains("{\"kind\":\"division by zero\",\"count\":2}"),
            "{out}"
        );
        assert!(
            out.contains("{\"name\":\"p\",\"columns\":2,\"rows\":9}"),
            "{out}"
        );
    }

    /// The document's shape is what the report page and the command
    /// line's JSON readers rely on: every key, in order, byte for byte.
    #[test]
    fn facts_keep_their_shape() {
        let facts = RunFacts {
            parse: 1,
            load: 2,
            reason: 3,
            export: 4,
            total: 10,
            loaded: 5,
            derived: 6,
            applications: 7,
            memory: 8,
            timings: EngineTimings {
                execution: PhaseTimings {
                    order: 11,
                    join: 12,
                    setup: 13,
                    merge: 14,
                    build: 15,
                    encode: 16,
                    total: 100,
                },
                apply: 150,
                store: 17,
                consolidate: 18,
                consolidated_rows: 19,
                stored_orders: 22,
                counts: WorkCounts {
                    joins: 31,
                    combinations: 32,
                    rebound: 33,
                    blocks: 34,
                    rows: 35,
                    arrangements: 20,
                    arranged_rows: 21,
                    buffer_bytes: 36,
                },
            },
            errors: vec![error("division by zero", 2)],
            predicates: vec![size("p", 2, 9), size("q", 1, 0)],
            programs: vec![
                text("loaded", "p(1) .\n"),
                text("sparql", "p(1) .\nq(1) .\n"),
            ],
        };
        let out = serde_json::to_string(&facts).expect("the facts serialize");
        assert_eq!(
            out,
            "{\"stages\":{\"parse\":1,\"load\":2,\"reason\":3,\"export\":4,\"total\":10},\
             \"loaded\":5,\"derived\":6,\"applications\":7,\"memory\":8,\
             \"phases\":{\"order\":11,\"join\":12,\"setup\":13,\"merge\":14,\"build\":15,\
             \"encode\":16,\"glue\":32,\"plan\":50,\"store\":17,\"consolidate\":18},\
             \"counts\":{\"joins\":31,\"combinations\":32,\"rebound\":33,\"blocks\":34,\
             \"rows\":35,\"arrangements\":20,\"arrangedRows\":21,\"consolidated\":19,\
             \"storedOrders\":22,\"buffer\":36},\
             \"errors\":[{\"kind\":\"division by zero\",\"count\":2}],\
             \"predicates\":[{\"name\":\"p\",\"columns\":2,\"rows\":9},\
             {\"name\":\"q\",\"columns\":1,\"rows\":0}],\
             \"programs\":[{\"stage\":\"loaded\",\"text\":\"p(1) .\\n\"},\
             {\"stage\":\"sparql\",\"text\":\"p(1) .\\nq(1) .\\n\"}]}"
        );
    }
}
