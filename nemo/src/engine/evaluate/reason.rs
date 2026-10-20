//! Semi-naive evaluation to the fixpoint.
//!
//! Each stratum is evaluated in iterations.
//! An iteration applies every active rule of the stratum once,
//! and the stratum is done after an iteration that derives nothing.
//! Between iterations, the tables' revision blocks are consolidated.
//! A run suspends if a bound SPARQL import needs data from the network,
//! and resumes at the application it was suspended in.

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
        remote::{PendingRequest, RemoteResponse, Suspension},
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

/// The suspended fetch in a build without the `sparql` feature.
///
/// The type is uninhabited, since no application suspends without the feature.
#[cfg(not(feature = "sparql"))]
#[derive(Debug)]
pub(in crate::engine) enum PendingFetch {}

/// A reasoning run suspended while a fetch waits for responses:
/// the suspended fetch, and the position from which the run resumes.
#[derive(Debug)]
pub(in crate::engine) struct SuspendedReasoning {
    /// The stratum the run was in.
    stratum: usize,
    /// The stratum's active rules.
    active: Vec<usize>,
    /// The position of the suspended application among the active rules.
    next: usize,
    /// The step the iteration began at.
    iteration_start: u64,
    /// The rows the iteration stored before the run was suspended.
    derived: usize,
    /// The run's evaluation context.
    context: EvaluationContext,
    /// The suspended fetch.
    fetch: Box<PendingFetch>,
}

impl SuspendedReasoning {
    /// Returns the requests waiting for their responses.
    pub(in crate::engine) fn pending(&self) -> Vec<PendingRequest> {
        #[cfg(feature = "sparql")]
        return self.fetch.slots.pending().collect();
        #[cfg(not(feature = "sparql"))]
        match *self.fetch {}
    }

    /// Records the response to the request with the given id,
    /// returning whether such a request was waiting.
    pub(in crate::engine) fn respond(&mut self, id: u64, response: RemoteResponse) -> bool {
        #[cfg(feature = "sparql")]
        return self.fetch.slots.respond(id, response);
        #[cfg(not(feature = "sparql"))]
        {
            let _ = (id, response);
            match *self.fetch {}
        }
    }
}

/// The requests one fetch rule made during a run,
/// those that failed and were skipped under `on_error = warn`,
/// and the rows of the responses that were not valid solutions.
///
/// They are reported when the run ends, one warning of each kind per import.
#[derive(Debug)]
#[cfg_attr(not(feature = "sparql"), expect(dead_code))]
pub(in crate::engine) struct FetchTally {
    /// The endpoint queried.
    endpoint: String,
    /// The import's predicate.
    predicate: String,
    /// The directive's source position.
    span: Option<Span>,
    /// Requests made.
    sent: u64,
    /// Requests that failed.
    failed: u64,
    /// The binding combinations the failed requests carried.
    unasked: u64,
    /// Why the first failed request failed.
    first: Option<String>,
    /// Rows in the responses that were not valid solutions.
    malformed: u64,
}

/// How a rule application ended.
// The result passes once per application, unboxed, since boxing it would allocate for every
// application.
#[allow(clippy::large_enum_variant)]
enum ApplicationResult {
    /// Applied, with its result.
    Applied(Applied),
    /// Suspended, waiting for the responses to its requests.
    #[cfg_attr(not(feature = "sparql"), expect(dead_code))]
    Suspended(Box<PendingFetch>),
}

/// How an application ended within an iteration.
enum StoreOutcome {
    /// It stored this many rows.
    Stored(usize),
    /// It is suspended, waiting for the responses to its requests.
    Suspended(Box<PendingFetch>),
}

/// Why an iteration ended before its last rule.
enum IterationHalt {
    /// The run was cancelled.
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
enum RunEnd {
    /// At the fixpoint.
    Fixpoint,
    /// Cancelled before it.
    Cancelled,
    /// Suspended while a fetch waits for responses.
    Suspended(SuspendedReasoning),
}

/// Returns an application's nonempty outputs grouped by table.
///
/// The outputs of heads deriving into one table are merged before storing,
/// since a table takes one batch per revision.
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
    /// a join for an ordinary rule, a fetch from the endpoint for a bound import's rule.
    ///
    /// A fetch suspends until its requests have responses,
    /// and continues from the suspended fetch passed in `waiting`.
    fn apply_rule(
        &mut self,
        rule: &mut CompiledRule,
        index: usize,
        step: u64,
        context: EvaluationContext,
        observe: bool,
        waiting: Option<Box<PendingFetch>>,
    ) -> Result<ApplicationResult, Diagnostic> {
        #[cfg(feature = "sparql")]
        if let Some(fetch) = &rule.fetch {
            let outcome = fetch::apply(
                rule,
                index,
                step,
                &mut self.manager,
                &mut self.dictionary,
                context,
                waiting.map(|waiting| *waiting),
                &mut self.request_ids,
            )?;
            let (applied, fetched) = match outcome {
                FetchOutcome::Suspended(pending) => {
                    return Ok(ApplicationResult::Suspended(Box::new(pending)));
                }
                FetchOutcome::Applied(applied, fetched) => (*applied, fetched),
            };
            if fetched.requests > 0 || fetched.malformed > 0 {
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
            return Ok(ApplicationResult::Applied(applied));
        }
        #[cfg(not(feature = "sparql"))]
        if let Some(waiting) = waiting {
            match *waiting {}
        }
        #[cfg(feature = "sparql")]
        debug_assert!(waiting.is_none(), "only a fetch waits");
        let skeleton = self.skeletons.of(index, rule);
        Ok(ApplicationResult::Applied(application::apply(
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
            });
        tally.sent += fetched.requests;
        tally.malformed += fetched.malformed;
        tally.failed += fetched.failed;
        tally.unasked = tally.unasked.saturating_add(fetched.unasked);
        if tally.first.is_none() {
            tally.first.clone_from(&fetched.first_failure);
        }
    }

    /// Reports the malformed rows and the failed requests that fetches skipped during the
    /// run, one warning of each kind per import, and resets the tallies.
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

    /// Reasons to the fixpoint (see [reason](Self::reason)).
    pub(in crate::engine) fn reason_to_fixpoint(&mut self) -> Result<(), Diagnostic> {
        let Some(mut program) = self.program.take() else {
            return Err(EngineError::NoProgram.diagnostic());
        };
        // A suspended reasoning run resumes where it was suspended. Any
        // other suspended method (a loading method waiting for data from
        // the network) is abandoned.
        let resumed = match self.suspension.take() {
            Some(Suspension::Reason(suspended)) => Some(suspended),
            other => {
                self.suspension = other;
                self.abandon();
                None
            }
        };
        self.fixpoint = false;
        // A new run evaluates at the instant its facts were loaded at, if any were.
        let context = match &resumed {
            Some(suspended) => suspended.context,
            None => self.instant.take().unwrap_or_else(EvaluationContext::now),
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
            Ok(RunEnd::Suspended(suspended)) => {
                return Err(self.suspend(Suspension::Reason(suspended)));
            }
            Ok(ended) => ended,
            Err(problem) => {
                self.report_fetch_tallies();
                return Err(problem);
            }
        };
        self.report_fetch_tallies();
        let cancelled = matches!(ended, RunEnd::Cancelled);
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
        Ok(())
    }

    /// Runs the strata to the fixpoint: stratum by stratum,
    /// iterations over the stratum's active rules until one derives nothing,
    /// consolidating the store between iterations.
    /// A resumed run begins at the application it was suspended in.
    /// Returns how the run ended.
    fn iterate_strata(
        &mut self,
        program: &mut CompiledProgram,
        strata: &[Vec<usize>],
        context: EvaluationContext,
        resumed: Option<SuspendedReasoning>,
    ) -> Result<RunEnd, Diagnostic> {
        self.cancel.clear();
        tracing::info!(strata = strata.len(), step = self.step, "reasoning");
        let finished = Self::finished_tables(program, strata);
        let first = resumed.as_ref().map_or(0, |suspended| suspended.stratum);
        let mut resumed = resumed;
        for (stratum_index, stratum) in strata.iter().enumerate().skip(first) {
            // A resumed stratum keeps its active rules, and continues its
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
                let (next, iteration_start, before, waiting) = match entry.take() {
                    Some((next, iteration_start, before, fetch)) => {
                        (next, iteration_start, before, Some(fetch))
                    }
                    None => (0, self.step, 0, None),
                };
                let (stored, halt) =
                    self.run_iteration(program, &active, next, context, waiting)?;
                self.derived += stored;
                let derived = before + stored;
                match halt {
                    None => {}
                    Some(IterationHalt::Cancelled) => return Ok(RunEnd::Cancelled),
                    Some(IterationHalt::Suspended { next, fetch }) => {
                        return Ok(RunEnd::Suspended(SuspendedReasoning {
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
        Ok(RunEnd::Fixpoint)
    }

    /// Returns, per stratum, the tables it leaves final for the later strata:
    /// those it writes, no later stratum writes, and a later stratum reads.
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

    /// Returns the rules of a stratum that can fire in it.
    ///
    /// Guard atoms, the negated atoms sharing no variable with their rule, are checked once here,
    /// since their predicates do not change within the stratum.
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
    /// The first application continues the suspended fetch in `waiting`, if given.
    /// Returns the rows the iteration stored, and why it ended early, if it did.
    fn run_iteration(
        &mut self,
        program: &mut CompiledProgram,
        active: &[usize],
        from: usize,
        context: EvaluationContext,
        mut waiting: Option<Box<PendingFetch>>,
    ) -> Result<(usize, Option<IterationHalt>), Diagnostic> {
        let mut stored = 0usize;
        for (position, &index) in active.iter().enumerate().skip(from) {
            if self.cancel.is_cancelled() {
                return Ok((stored, Some(IterationHalt::Cancelled)));
            }
            let rule = &mut program.rules[index];
            match self.apply_and_store(rule, index, context, waiting.take())? {
                StoreOutcome::Stored(rows) => stored += rows,
                StoreOutcome::Suspended(fetch) => {
                    let halt = IterationHalt::Suspended {
                        next: position,
                        fetch,
                    };
                    return Ok((stored, Some(halt)));
                }
            }
        }
        Ok((stored, None))
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

    /// Applies one rule at the current step and stores what it derived,
    /// returning the number of rows stored,
    /// or the suspended application waiting for the responses to its requests,
    /// which does not advance the step.
    ///
    /// A body without new rows since the rule's last application derives nothing,
    /// so its delta window advances without building a plan.
    fn apply_and_store(
        &mut self,
        rule: &mut CompiledRule,
        index: usize,
        context: EvaluationContext,
        waiting: Option<Box<PendingFetch>>,
    ) -> Result<StoreOutcome, Diagnostic> {
        if waiting.is_none() && !self.has_fresh_input(rule) {
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
        } = match self.apply_rule(rule, index, step, context, observe, waiting)? {
            ApplicationResult::Applied(applied) => applied,
            ApplicationResult::Suspended(waiting) => return Ok(StoreOutcome::Suspended(waiting)),
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
            // The tracing log: which rule wrote this revision.
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
    /// returning the number of rows stored.
    ///
    /// A table that gains rows is charged the application's `applied` nanoseconds
    /// plus the time spent storing into it.
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

    /// Consolidates after an iteration, merging the revision blocks the two-level policy proposes.
    ///
    /// Since delta windows start at block boundaries,
    /// only the start of the iteration that just ran must stay a wall (a revision no merge
    /// crosses).
    /// Every rule's next delta begins at or after it,
    /// so at most the rows of one iteration are derived again,
    /// and the blocks the iteration added may merge freely.
    ///
    /// The two-level policy keeps every growing table about two blocks wide,
    /// since a join's cost per application grows with the product of its inputs' block counts.
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

    /// Consolidates after a stratum,
    /// merging each table the stratum leaves final into one block between each pair of adjacent
    /// walls.
    ///
    /// During the stratum, the two-level policy keeps a growing table several blocks wide,
    /// so that its merges stay cheap.
    /// A table no later stratum writes is not merged again,
    /// and every later read of it costs per block.
    fn finish(&mut self, program: &CompiledProgram, tables: &[TableId]) {
        let begun = Stopwatch::start();
        let walls = Self::exact_walls(program);
        for &table in tables {
            self.consolidate_table(table, &walls, MergePolicy::Whole);
        }
        self.timings.consolidate += begun.nanos();
    }

    /// Returns the last applications of the rules that read exact windows,
    /// which consolidation keeps as walls.
    ///
    /// Rules that fetch, or call a nondeterministic function (such as `RAND()`),
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
    /// so input facts never merge with derived rows,
    /// which keeps them separable for recovery and reload.
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
