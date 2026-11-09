//! Semi-naive evaluation to the fixpoint.
//!
//! Each stratum is evaluated in iterations.
//! An iteration applies every *active* rule of the stratum once,
//! which is every rule that can fire in the stratum.
//! The stratum is done after an iteration that derives nothing.
//! Between iterations, the tables' revision blocks are consolidated.
//!
//! The rule of a bound SPARQL import is a *fetch rule*:
//! its application queries the endpoint instead of joining.
//! A run is suspended if a fetch rule needs data from the network,
//! and continues at the rule application where it was suspended.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    function::EvaluationContext,
    management::{ColumnOrder, MergePolicy, Revision, TableId},
    stopwatch::Stopwatch,
    trie::Trie,
};

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::compiled::{CompiledProgram, CompiledRule},
        evaluate::{
            application::{self, Applied},
            body,
        },
        registered::Registered,
        remote::{PendingRequest, Progress, Response, Suspension},
    },
    model::Span,
    telemetry::EngineEvent,
};
#[cfg(feature = "sparql")]
use crate::{
    diagnostics::{AboutPredicate, AboutResource, TranslationError},
    engine::{
        compile::compiled::CompiledFetch,
        evaluate::fetch::{self, FetchOutcome, Fetched, PendingFetch},
    },
};

/// The type of a suspended fetch in a build without the `sparql` feature.
///
/// The type is uninhabited, since no application is suspended without the feature.
#[cfg(not(feature = "sparql"))]
#[derive(Debug)]
pub(in crate::engine) enum PendingFetch {}

/// A run suspended until the requests of a fetch have responses:
/// the suspended fetch, and the position from which the run continues.
#[derive(Debug)]
pub(in crate::engine) struct SuspendedReasoning {
    /// The stratum the run was in.
    stratum: usize,
    /// The stratum's active rules.
    active: Vec<usize>,
    /// The position of the suspended application among the active rules.
    next: usize,
    /// The step at which the iteration began.
    iteration_start: u64,
    /// The number of rows that the iteration stored before the run was suspended.
    derived: usize,
    /// The run's evaluation context.
    context: EvaluationContext,
    /// The suspended fetch.
    fetch: Box<PendingFetch>,
}

impl SuspendedReasoning {
    /// Returns the requests without a response yet.
    pub(in crate::engine) fn pending(&self) -> Vec<PendingRequest> {
        #[cfg(feature = "sparql")]
        return self.fetch.slots.pending().collect();
        #[cfg(not(feature = "sparql"))]
        match *self.fetch {}
    }

    /// Returns whether some request has no response yet.
    pub(in crate::engine) fn has_pending(&self) -> bool {
        #[cfg(feature = "sparql")]
        return self.fetch.slots.has_pending();
        #[cfg(not(feature = "sparql"))]
        match *self.fetch {}
    }

    /// Records the response to the request with the given id, if such a request is pending.
    pub(in crate::engine) fn respond(&mut self, id: u64, response: Response) {
        #[cfg(feature = "sparql")]
        return self.fetch.slots.respond(id, response);
        #[cfg(not(feature = "sparql"))]
        {
            let _ = (id, response);
            match *self.fetch {}
        }
    }
}

/// The tally of one fetch rule's requests during a run.
///
/// It counts the requests, those that failed and were skipped under `on_error = warn`,
/// and the rows of the responses that were not valid solutions.
/// They are reported when the run ends, with one warning of each kind per import.
#[derive(Debug)]
#[cfg_attr(not(feature = "sparql"), expect(dead_code))]
pub(in crate::engine) struct FetchTally {
    /// The endpoint queried.
    endpoint: String,
    /// The import's predicate.
    predicate: String,
    /// The directive's source position.
    span: Option<Span>,
    /// The number of requests sent.
    sent: u64,
    /// The number of requests that failed.
    failed: u64,
    /// The number of binding combinations that the failed requests carried.
    unasked: u64,
    /// Why the first failed request failed.
    first: Option<String>,
    /// The number of rows in the responses that were not valid solutions.
    malformed: u64,
    /// The number of binding rows left out because they contain a blank node that an endpoint
    /// returned.
    blank_bindings: u64,
}

/// How a rule application ended.
// The outcome is returned once per application, unboxed,
// since boxing it would allocate for every application.
#[allow(clippy::large_enum_variant)]
enum ApplicationOutcome {
    /// The rule was applied, with this result.
    Applied(Applied),
    /// The application is suspended until the requests of this fetch have responses.
    #[cfg_attr(not(feature = "sparql"), expect(dead_code))]
    Suspended(Box<PendingFetch>),
}

/// How an application, and the storing of its rows, ended within an iteration.
enum StoreOutcome {
    /// It stored this many rows.
    Stored(usize),
    /// It is suspended until its requests have responses.
    Suspended(Box<PendingFetch>),
}

/// How an iteration over a stratum's active rules ended.
enum IterationOutcome {
    /// Every rule was applied.
    Completed,
    /// The run was cancelled before the iteration's last rule.
    Cancelled,
    /// The application at this position among the active rules is suspended.
    Suspended {
        /// The position of the suspended application among the active rules.
        next: usize,
        /// The suspended fetch.
        fetch: Box<PendingFetch>,
    },
}

/// How the iterations of a run ended.
enum RunOutcome {
    /// The run reached the fixpoint.
    Fixpoint,
    /// The run was cancelled before the fixpoint.
    Cancelled,
    /// The run is suspended until the requests of a fetch have responses.
    Suspended(SuspendedReasoning),
}

/// Returns an application's nonempty outputs grouped by table.
///
/// The outputs of the heads that derive into one table are merged before they are stored,
/// since a table accepts one batch per revision.
fn group_per_table(outputs: Vec<(TableId, Trie)>) -> HashMap<TableId, Vec<Trie>> {
    let mut per_table: HashMap<TableId, Vec<Trie>> = HashMap::new();
    for (table, trie) in outputs {
        if !trie.is_empty() {
            per_table.entry(table).or_default().push(trie);
        }
    }
    per_table
}

impl Engine {
    /// Applies one rule at a step:
    /// a join for an ordinary rule, and a fetch from the endpoint for a fetch rule.
    ///
    /// A fetch is suspended until its requests have responses.
    /// `suspended` is the suspended fetch to continue, if any.
    /// `observe` asks for observations of the semi-naive variants, for telemetry.
    fn apply_rule(
        &mut self,
        rule: &mut CompiledRule,
        index: usize,
        step: u64,
        context: EvaluationContext,
        observe: bool,
        suspended: Option<Box<PendingFetch>>,
    ) -> Result<ApplicationOutcome, Diagnostic> {
        #[cfg(feature = "sparql")]
        if let Some(fetch) = &rule.fetch {
            let outcome = fetch::apply(
                rule,
                index,
                step,
                &mut self.manager,
                &mut self.dictionary,
                context,
                suspended.map(|suspended| *suspended),
                &mut self.request_ids,
            )?;
            let (applied, fetched) = match outcome {
                FetchOutcome::Suspended(pending) => {
                    return Ok(ApplicationOutcome::Suspended(Box::new(pending)));
                }
                FetchOutcome::Applied(applied, fetched) => (*applied, fetched),
            };
            if fetched.requests > 0 || fetched.malformed > 0 || fetched.blank_bindings > 0 {
                self.tally_fetch(index, fetch, &fetched);
            }
            if fetched.requests > 0 {
                self.telemetry.record(&EngineEvent::Fetched {
                    rule: index,
                    endpoint: fetch.endpoint.clone(),
                    requests: fetched.requests,
                    rows: fetched.rows,
                });
            }
            return Ok(ApplicationOutcome::Applied(applied));
        }
        #[cfg(not(feature = "sparql"))]
        if let Some(suspended) = suspended {
            match *suspended {}
        }
        #[cfg(feature = "sparql")]
        debug_assert!(suspended.is_none(), "only a fetch is suspended");
        let skeleton = self.skeletons.of(index, rule);
        Ok(ApplicationOutcome::Applied(application::apply(
            rule,
            skeleton,
            step,
            &mut self.manager,
            &mut self.dictionary,
            context,
            observe,
        )))
    }

    /// Adds a fetch application's requests, failures, and malformed rows to its rule's
    /// tally for the run.
    #[cfg(feature = "sparql")]
    fn tally_fetch(&mut self, index: usize, fetch: &CompiledFetch, fetched: &Fetched) {
        let tally = self
            .fetch_tallies
            .entry(index)
            .or_insert_with(|| FetchTally {
                endpoint: fetch.endpoint.clone(),
                predicate: fetch.predicate.clone(),
                span: fetch.span,
                sent: 0,
                failed: 0,
                unasked: 0,
                first: None,
                malformed: 0,
                blank_bindings: 0,
            });
        tally.sent += fetched.requests;
        tally.malformed += fetched.malformed;
        tally.blank_bindings += fetched.blank_bindings;
        tally.failed += fetched.failed;
        tally.unasked = tally.unasked.saturating_add(fetched.unasked);
        if tally.first.is_none() {
            tally.first.clone_from(&fetched.first_failure);
        }
    }

    /// Reports the malformed rows, the blank-node bindings and the failed requests that
    /// fetches skipped during the run, one warning of each kind per import,
    /// and resets the tallies.
    fn report_fetch_tallies(&mut self) {
        let mut tallies: Vec<(usize, FetchTally)> = std::mem::take(&mut self.fetch_tallies)
            .into_iter()
            .collect();
        tallies.sort_unstable_by_key(|(rule, _)| *rule);
        #[cfg(feature = "sparql")]
        for (_, tally) in tallies {
            if tally.malformed > 0 {
                self.pipeline.report(
                    TranslationError::MalformedRows {
                        count: tally.malformed.into(),
                        resource: tally.endpoint.clone(),
                    }
                    .at(tally.span)
                    .with_resource(tally.endpoint.clone())
                    .with_predicate(tally.predicate.clone()),
                );
            }
            if tally.blank_bindings > 0 {
                self.pipeline.report(
                    TranslationError::BlankNodeBindings {
                        count: tally.blank_bindings.into(),
                        resource: tally.endpoint.clone(),
                    }
                    .at(tally.span)
                    .with_resource(tally.endpoint.clone())
                    .with_predicate(tally.predicate.clone()),
                );
            }
            if tally.failed == 0 {
                continue;
            }
            let mut warning = TranslationError::RequestsFailed {
                failed: tally.failed.into(),
                sent: tally.sent.into(),
                resource: tally.endpoint.clone(),
                unasked: tally.unasked.into(),
            }
            .at(tally.span)
            .with_resource(tally.endpoint)
            .with_predicate(tally.predicate);
            if let Some(first) = tally.first {
                warning = warning.with_note(format!("the first failure was: {first}"));
            }
            self.pipeline.report(warning);
        }
        #[cfg(not(feature = "sparql"))]
        debug_assert!(tallies.is_empty(), "only a fetch tallies");
    }

    /// Ends a run that was discarded while it was suspended, as a cancellation ends one.
    ///
    /// It reports what the run's fetches skipped, against the written program.
    pub(in crate::engine) fn end_discarded_run(&mut self) {
        let already = self.pipeline.reports().len();
        self.report_fetch_tallies();
        if let Some(written) = &self.written {
            self.pipeline
                .map_reports(already, |report| written.present(report));
        }
    }

    /// Starts or continues a run, until the fixpoint, a cancellation or a suspension
    /// (see [reason](Self::reason)).
    pub(in crate::engine) fn reason_to_fixpoint(&mut self) -> Result<Progress, Diagnostic> {
        let Some(mut program) = self.program.take() else {
            return Err(EngineError::NoProgram.diagnostic());
        };
        // A suspended run continues where it was suspended. A suspended
        // loading method refused the run before it got here.
        let resumed = match self.suspension.take() {
            Some(Suspension::Reason(suspended)) => Some(suspended),
            other => {
                debug_assert!(other.is_none(), "a suspended loading method refuses a run");
                self.suspension = other;
                None
            }
        };
        self.fixpoint = false;
        // A new run evaluates in the context that a loading method chose since the last run,
        // if one did.
        let context = match &resumed {
            Some(suspended) => suspended.context,
            None => self
                .next_context
                .take()
                .unwrap_or_else(EvaluationContext::now),
        };
        // New input facts first invalidate the derived facts that depend
        // on them through negation or aggregation (see `recover`). The
        // strata are moved out for the iterations and put back afterwards,
        // so the engine keeps its program even if an application fails.
        let strata = std::mem::take(&mut program.strata);
        if resumed.is_none() {
            self.recover(&mut program);
            self.contexts.begin(self.step, context);
        }
        let outcome = self.iterate_strata(&mut program, &strata, context, resumed);
        program.strata = strata;
        self.program = Some(program);
        let ended = match outcome {
            Ok(RunOutcome::Suspended(suspended)) => {
                return Ok(self.suspend(Suspension::Reason(suspended)));
            }
            Ok(ended) => ended,
            Err(problem) => {
                self.report_fetch_tallies();
                return Err(problem);
            }
        };
        self.report_fetch_tallies();
        let cancelled = matches!(ended, RunOutcome::Cancelled);
        if self.telemetry.enabled() {
            self.telemetry.record(&EngineEvent::Finished {
                fixpoint: !cancelled,
                derived: self.derived as u64,
                memory: self.memory_bytes() as u64,
            });
        }
        tracing::info!(
            derived = self.derived,
            steps = self.step,
            fixpoint = !cancelled,
            "reasoning finished"
        );
        if cancelled {
            return Err(EngineError::Cancelled.diagnostic());
        }
        self.fixpoint = true;
        Ok(Progress::Done)
    }

    /// Evaluates the strata in order, each to its fixpoint.
    ///
    /// A stratum is evaluated in iterations over its active rules until one derives nothing,
    /// and the store is consolidated between iterations.
    /// A continued run begins at the application where it was suspended.
    /// Returns how the run ended.
    fn iterate_strata(
        &mut self,
        program: &mut CompiledProgram,
        strata: &[Vec<usize>],
        context: EvaluationContext,
        resumed: Option<SuspendedReasoning>,
    ) -> Result<RunOutcome, Diagnostic> {
        // The token is cleared where a public method starts or continues a run, not here.
        // One `reason()` continues its run after every fetch, and a cancellation that arrives
        // in between must stop it.
        tracing::info!(strata = strata.len(), step = self.step, "reasoning");
        let finished = Self::finished_tables(program, strata);
        let first = resumed.as_ref().map_or(0, |suspended| suspended.stratum);
        let mut resumed = resumed;
        for (stratum_index, stratum) in strata.iter().enumerate().skip(first) {
            // A continued stratum keeps its active rules, and continues its
            // iteration at the suspended application.
            let (active, mut entry) = match resumed.take() {
                Some(suspended) => {
                    let entry = (
                        suspended.next,
                        suspended.iteration_start,
                        suspended.derived,
                        suspended.fetch,
                    );
                    (suspended.active, Some(entry))
                }
                None => {
                    let active = self.activate(program, stratum, context);
                    self.telemetry.record(&EngineEvent::StratumStarted {
                        stratum: stratum_index,
                        rules: active.len(),
                    });
                    tracing::debug!(
                        stratum = stratum_index,
                        rules = active.len(),
                        "stratum started"
                    );
                    (active, None)
                }
            };

            // Iterate until an iteration stores no new rows.
            loop {
                let (next, iteration_start, before, suspended) = match entry.take() {
                    Some((next, iteration_start, before, fetch)) => {
                        (next, iteration_start, before, Some(fetch))
                    }
                    None => (0, self.step, 0, None),
                };
                let (stored, outcome) =
                    self.run_iteration(program, &active, next, context, suspended)?;
                self.derived += stored;
                let derived = before + stored;
                match outcome {
                    IterationOutcome::Completed => {}
                    IterationOutcome::Cancelled => return Ok(RunOutcome::Cancelled),
                    IterationOutcome::Suspended { next, fetch } => {
                        return Ok(RunOutcome::Suspended(SuspendedReasoning {
                            stratum: stratum_index,
                            active,
                            next,
                            iteration_start,
                            derived,
                            context,
                            fetch,
                        }));
                    }
                }
                tracing::debug!(stratum = stratum_index, derived, "iteration finished");
                self.report_iteration(stratum_index, derived);
                if derived == 0 {
                    break;
                }
                self.consolidate(program, iteration_start);
            }
            self.finish(program, &finished[stratum_index]);
        }
        Ok(RunOutcome::Fixpoint)
    }

    /// Returns, per stratum, the tables that it leaves final for the later strata:
    /// the tables that it writes and a later stratum reads, but no later stratum writes.
    fn finished_tables(program: &CompiledProgram, strata: &[Vec<usize>]) -> Vec<Vec<TableId>> {
        let mut last_writer: HashMap<TableId, usize> = HashMap::new();
        let mut last_reader: HashMap<TableId, usize> = HashMap::new();
        for (stratum, rules) in strata.iter().enumerate() {
            for &index in rules {
                let rule = &program.rules[index];
                for table in Self::head_tables(rule) {
                    last_writer.insert(table, stratum);
                }
                let read = rule.body.iter().map(|atom| atom.table);
                for table in read.chain(rule.negated.iter().map(|atom| atom.table)) {
                    last_reader.insert(table, stratum);
                }
            }
        }

        let mut finished = vec![Vec::new(); strata.len()];
        for (table, stratum) in last_writer {
            if last_reader
                .get(&table)
                .is_some_and(|&reader| reader > stratum)
            {
                finished[stratum].push(table);
            }
        }
        finished
    }

    /// Returns the active rules of a stratum, which are the rules that can fire in it.
    ///
    /// Guard atoms, which are the negated atoms that share no variable with their rule,
    /// are checked once here, since their predicates do not change within the stratum.
    fn activate(
        &mut self,
        program: &CompiledProgram,
        stratum: &[usize],
        context: EvaluationContext,
    ) -> Vec<usize> {
        let mut active = Vec::with_capacity(stratum.len());
        for &index in stratum {
            let rule = &program.rules[index];
            if rule.never_fires {
                continue;
            }
            let mut blocked = false;
            for atom in rule.negated.iter().filter(|atom| atom.is_guard()) {
                if body::guard_matches(
                    atom,
                    self.step,
                    &mut self.manager,
                    &mut self.dictionary,
                    context,
                ) {
                    blocked = true;
                    break;
                }
            }
            if !blocked {
                active.push(index);
            }
        }
        active
    }

    /// Runs one iteration over a stratum's active rules from the given position on.
    ///
    /// The first application continues the suspended fetch in `suspended`, if given.
    /// Returns the number of rows that the iteration stored, and how it ended.
    fn run_iteration(
        &mut self,
        program: &mut CompiledProgram,
        active: &[usize],
        from: usize,
        context: EvaluationContext,
        mut suspended: Option<Box<PendingFetch>>,
    ) -> Result<(usize, IterationOutcome), Diagnostic> {
        let mut stored = 0usize;
        for (position, &index) in active.iter().enumerate().skip(from) {
            if self.cancel.is_cancelled() {
                return Ok((stored, IterationOutcome::Cancelled));
            }
            let rule = &mut program.rules[index];
            match self.apply_and_store(rule, index, context, suspended.take())? {
                StoreOutcome::Stored(rows) => stored += rows,
                StoreOutcome::Suspended(fetch) => {
                    let outcome = IterationOutcome::Suspended {
                        next: position,
                        fetch,
                    };
                    return Ok((stored, outcome));
                }
            }
        }
        Ok((stored, IterationOutcome::Completed))
    }

    /// Returns whether a rule's body has new rows since its last application.
    ///
    /// The tables' last nonempty revisions give the exact answer,
    /// however their blocks have merged.
    fn has_fresh_input(&self, rule: &CompiledRule) -> bool {
        rule.body.iter().any(|atom| {
            self.manager
                .last_added(atom.table)
                .is_some_and(|added| added >= Revision(rule.last_applied))
        })
    }

    /// Applies one rule at the current step and stores what it derived.
    ///
    /// Returns the number of rows stored,
    /// or the suspended fetch whose requests need responses.
    /// A suspended application does not advance the step.
    /// A body without new rows since the rule's last application derives nothing,
    /// so the rule's delta window advances without building a plan.
    fn apply_and_store(
        &mut self,
        rule: &mut CompiledRule,
        index: usize,
        context: EvaluationContext,
        suspended: Option<Box<PendingFetch>>,
    ) -> Result<StoreOutcome, Diagnostic> {
        if suspended.is_none() && !self.has_fresh_input(rule) {
            rule.last_applied = self.step;
            return Ok(StoreOutcome::Stored(0));
        }

        let step = self.step;
        let begun = Stopwatch::start();
        let observe = self.telemetry.enabled();
        let Applied {
            outputs,
            errors,
            timings,
            counts,
            observations,
        } = match self.apply_rule(rule, index, step, context, observe, suspended)? {
            ApplicationOutcome::Applied(applied) => applied,
            ApplicationOutcome::Suspended(fetch) => return Ok(StoreOutcome::Suspended(fetch)),
        };
        self.step += 1;
        self.applied += 1;
        for observation in observations {
            self.telemetry.record(&EngineEvent::VariantObserved {
                rule: index,
                variant: observation.variant,
                delta_rows: observation.delta_rows,
                leading_rows: observation.leading_rows,
                delta_leads: observation.delta_leads,
            });
        }
        let applied = begun.nanos();
        self.timings.apply += applied;
        self.timings.execution.absorb(&timings);
        self.timings.counts.absorb(&counts);
        rule.last_applied = step;
        let dropped = errors.total();
        self.errors.absorb(&errors);

        let per_table = group_per_table(outputs);
        let begun = Stopwatch::start();
        let stored = self.store_grouped(step, per_table, applied);
        let storing = begun.nanos();
        self.timings.store += storing;
        if stored > 0 {
            // Log which rule wrote this revision, for tracing.
            self.applications.push(application::LoggedApplication {
                rule: u32::try_from(index).unwrap_or(u32::MAX),
                step,
            });
        }
        self.telemetry.record(&EngineEvent::RuleApplied {
            rule: index,
            step,
            duration: applied + storing,
            derived: stored as u64,
            dropped,
            execution: timings,
            counts,
        });
        tracing::trace!(
            rule = index,
            step,
            derived = stored,
            dropped,
            nanoseconds = applied + storing,
            "rule applied"
        );
        Ok(StoreOutcome::Stored(stored))
    }

    /// Stores an application's outputs at its step, one batch per table,
    /// and returns the number of rows stored.
    ///
    /// `applied` is the time the application took, in nanoseconds.
    /// A table that gains rows is charged this time, plus the time spent storing into it.
    fn store_grouped(
        &mut self,
        step: u64,
        per_table: HashMap<TableId, Vec<Trie>>,
        applied: u64,
    ) -> usize {
        let mut stored = 0;
        for (table, mut tries) in per_table {
            let begun = Stopwatch::start();
            let trie = match tries.len() {
                1 => tries.pop().expect("one trie"),
                _ => Trie::merge(&tries.iter().collect::<Vec<_>>()),
            };
            #[cfg(debug_assertions)]
            if let Some(program) = &self.program {
                self.assert_stored_trie(&program.column_types, table, &trie);
            }
            let arity = trie.arity();
            let added =
                self.manager
                    .add_trie(table, Revision(step), ColumnOrder::identity(arity), trie);
            if added > 0 {
                *self.derivation_time.entry(table).or_default() += applied + begun.nanos();
            }
            stored += added;
        }
        stored
    }

    /// Reports a finished iteration to telemetry.
    ///
    /// Does nothing unless telemetry is enabled, since counting the stored rows reads every table.
    fn report_iteration(&mut self, stratum: usize, derived: usize) {
        if !self.telemetry.enabled() {
            return;
        }
        let mut total = 0;
        for &Registered { table, .. } in self.predicates.values() {
            total += self
                .manager
                .row_count(table, Revision(0)..Revision(self.step)) as u64;
        }
        self.telemetry.record(&EngineEvent::IterationFinished {
            stratum,
            derived: derived as u64,
            total,
            memory: self.memory_bytes() as u64,
        });
    }

    /// Consolidates the tables after an iteration,
    /// merging the revision blocks that the two-level policy proposes.
    ///
    /// A [wall](nemo_physical::management::MergePolicy) is a revision that no merge crosses.
    /// Delta windows start at block boundaries,
    /// so besides the walls of [exact_walls](Self::exact_walls),
    /// only the start of the iteration that just ran must stay a wall.
    /// Every rule's next delta begins at or after it,
    /// so at most the rows of one iteration are derived again,
    /// and the blocks that the iteration added may merge freely.
    ///
    /// The [two-level policy](nemo_physical::management::MergePolicy::TwoLevel) is chosen
    /// since it keeps few blocks between walls,
    /// and a join reads every block of its inputs, each through a cursor of its own.
    /// The price is merges that rewrite more rows than doubling merges do.
    fn consolidate(&mut self, program: &CompiledProgram, iteration_start: u64) {
        let begun = Stopwatch::start();
        let mut walls = Self::exact_walls(program);
        walls.push(Revision(iteration_start));
        let tables: Vec<TableId> = self
            .predicates
            .values()
            .map(|&Registered { table, .. }| table)
            .collect();
        for table in tables {
            self.consolidate_table(table, &walls, MergePolicy::TwoLevel);
        }
        self.timings.consolidate += begun.nanos();
    }

    /// Consolidates after a stratum:
    /// merges each table that the stratum leaves final into one block between each pair of
    /// adjacent walls.
    ///
    /// During the stratum, the two-level policy keeps a growing table several blocks wide,
    /// so that its merges stay cheap.
    /// A table that no later stratum writes is not merged again,
    /// and every later read of it pays for each of its blocks.
    fn finish(&mut self, program: &CompiledProgram, tables: &[TableId]) {
        let begun = Stopwatch::start();
        let walls = Self::exact_walls(program);
        for &table in tables {
            self.consolidate_table(table, &walls, MergePolicy::Whole);
        }
        self.timings.consolidate += begun.nanos();
    }

    /// Returns the steps of the last applications of the rules that read exact windows,
    /// which consolidation keeps as walls.
    ///
    /// Fetch rules, and rules that call a nondeterministic function (such as `RAND()`),
    /// must not read a row twice.
    /// So the block boundary at their last application must survive every merge in every
    /// stratum, since a later stratum's rule reads this stratum's tables from its own last
    /// application on.
    fn exact_walls(program: &CompiledProgram) -> Vec<Revision> {
        program
            .rules
            .iter()
            .filter(|rule| rule.exact_windows())
            .map(|rule| Revision(rule.last_applied))
            .collect()
    }

    /// Consolidates one table under a policy until the policy proposes no further merge,
    /// never merging across the given walls or the boundaries of the table's extensional blocks.
    ///
    /// The boundaries of extensional blocks are always walls,
    /// so that input facts never merge with derived rows,
    /// and the derived rows can be dropped alone.
    fn consolidate_table(&mut self, table: TableId, walls: &[Revision], policy: MergePolicy) {
        let mut walls = walls.to_vec();
        for block in self.extensional.get(&table).into_iter().flatten() {
            walls.push(block.range.start);
            walls.push(block.range.end);
        }

        let rewritten = self.manager.consolidate_with(table, &walls, policy);
        self.timings.consolidated_rows += rewritten as u64;
    }
}
