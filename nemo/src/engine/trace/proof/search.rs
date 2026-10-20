//! The proof search over the stored tables.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    rc::Rc,
};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    management::{Revision, TableId, TableManager},
    plan::{FilterSpec, Marker, OperationPlan},
    storage::StorageValue,
    trie::Trie,
    value::{DataValue, ValueRef},
};

use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::compiled::{
            AggregateColumn, CompiledProgram, CompiledRule, ExistentialAtom, ExistentialHead,
            ExistentialTerm, HeadAtom,
        },
        evaluate::{
            application::{LoggedApplication, scope_columns},
            body::{BodySpace, combined_positions, filtered, same_constant},
            contexts::RunContexts,
            extensional::{ExtensionalBlock, ExtensionalOrigin},
        },
        trace::proof::{Proof, ProofNode, targets},
    },
    model::AggregateKind,
};

/// The outcome of a search.
pub(super) enum Found {
    /// A proof exists.
    Proven(Rc<ProofNode>),
    /// No proof was found.
    Missing {
        /// Whether the failure involved the cycle guard.
        ///
        /// Such a result depends on its context and must not be memoized.
        blocked: bool,
    },
}

/// The outcome of one candidate application's attempt.
pub(super) enum Attempted {
    /// The candidate explains the fact.
    Proven(Rc<ProofNode>),
    /// A premise search hit the cycle guard.
    Blocked,
    /// No witness of this candidate explains the fact.
    NoWitness,
}

/// One candidate application of a rule, to explain one fact.
#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateApplication<'f> {
    /// The rule's index.
    pub(super) rule: usize,
    /// The engine step of the application,
    /// or the end of the fact's revision block if the application log has no entry for it.
    pub(super) step: u64,
    /// The fact's table.
    pub(super) table: TableId,
    /// The fact's encoded row.
    pub(super) row: &'f [StorageValue],
}

impl CandidateApplication<'_> {
    /// Returns the proof node deriving the fact by this application.
    pub(super) fn derived(
        &self,
        witness: Vec<StorageValue>,
        premises: Vec<Rc<ProofNode>>,
        matches: Option<usize>,
        nondeterministic: bool,
    ) -> Rc<ProofNode> {
        Rc::new(ProofNode {
            table: self.table,
            row: self.row.to_vec(),
            proof: Proof::Derived {
                rule: self.rule,
                step: self.step,
                witness,
                premises,
                matches,
                nondeterministic,
            },
        })
    }
}

/// The proof search over one engine's stores.
pub(super) struct ProofSearch<'a> {
    /// The table store.
    pub(super) manager: &'a mut TableManager,
    /// The dictionary.
    pub(super) dictionary: &'a mut Dictionary,
    /// The compiled program.
    pub(super) program: &'a CompiledProgram,
    /// The application log, ascending by step.
    applications: &'a [LoggedApplication],
    /// The context each reasoning run evaluated in.
    pub(super) contexts: &'a RunContexts,
    /// Per table, its extensional blocks.
    extensional: &'a HashMap<TableId, Vec<ExtensionalBlock>>,
    /// The engine's current step.
    ///
    /// Every stored row's revision lies below it.
    pub(super) now: u64,
    /// The proven and refuted facts.
    ///
    /// Refutations are recorded only if they are free of context (no cycle guard involved).
    memo: HashMap<(TableId, Vec<StorageValue>), Option<Rc<ProofNode>>>,
    /// The facts on the current search path, the cycle guard.
    path: HashSet<(TableId, Vec<StorageValue>)>,
}

impl<'a> ProofSearch<'a> {
    /// Returns a search over the engine's stores.
    pub(super) fn over(engine: &'a mut Engine) -> Result<Self, Diagnostic> {
        let program = engine
            .program
            .as_ref()
            .ok_or_else(|| EngineError::NoProgram.diagnostic())?;
        Ok(Self {
            manager: &mut engine.manager,
            dictionary: &mut engine.dictionary,
            program,
            applications: &engine.applications,
            contexts: &engine.contexts,
            extensional: &engine.extensional,
            now: engine.step.max(1),
            memo: HashMap::new(),
            path: HashSet::new(),
        })
    }

    /// Returns a proof of the fact given by its values, or `None` if it is not stored.
    ///
    /// A fact with a value the dictionary cannot encode is not stored either.
    pub(super) fn prove(
        &mut self,
        table: TableId,
        values: &[DataValue],
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
        let row: Option<Vec<StorageValue>> = values
            .iter()
            .map(|value| self.dictionary.encode(value.as_ref()))
            .collect();
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(match self.search(table, &row)? {
            Found::Proven(node) => Some(node),
            Found::Missing { .. } => None,
        })
    }

    /// Returns the row's values, decoded.
    pub(super) fn decode(&self, row: &[StorageValue]) -> Vec<DataValue> {
        row.iter()
            .map(|&value| {
                self.dictionary
                    .decode(value)
                    .expect("stored values decode against their dictionary")
                    .to_owned()
            })
            .collect()
    }

    /// Returns whether the table holds the row within the revision range (which
    /// must align with block boundaries).
    fn contains(&mut self, table: TableId, range: Range<Revision>, row: &[StorageValue]) -> bool {
        let values = self.decode(row);
        let mut plan = OperationPlan::new();
        let markers: Vec<Marker> = row.iter().map(|_| plan.marker()).collect();
        let load = plan.load(table, range, markers.clone());
        let filters = markers
            .iter()
            .zip(values)
            .map(|(&marker, value)| FilterSpec::from_program(vec![marker], same_constant(value)))
            .collect();
        let matching = filtered(&mut plan, load, filters);
        let node = plan.project(matching, Vec::new());
        let output = plan.output(node);
        let context = self.contexts.latest();
        let results = plan.execute(self.manager, self.dictionary, context);
        !results.outputs[output].is_empty()
    }

    /// Returns the smallest revision at or above the given step that aligns with the table's
    /// block boundaries.
    ///
    /// Reads cover whole blocks, so a read of the data before the step ends there.
    pub(super) fn aligned_end(&self, table: TableId, step: u64) -> Revision {
        let at = Revision(step);
        for block in self.manager.blocks(table) {
            if block.start < at && at < block.end {
                return block.end;
            }
        }
        at
    }

    /// Returns the first revision block of the table holding the row, if any.
    fn first_block(&mut self, table: TableId, row: &[StorageValue]) -> Option<Range<Revision>> {
        self.manager
            .blocks(table)
            .into_iter()
            .find(|block| self.contains(table, block.clone(), row))
    }

    /// Returns the origin of the extensional block covering a table's block, if
    /// the block is extensional.
    fn extensional_origin(
        &self,
        table: TableId,
        block: &Range<Revision>,
    ) -> Option<ExtensionalOrigin> {
        self.extensional
            .get(&table)
            .into_iter()
            .flatten()
            .find(|extensional| {
                extensional.range.start <= block.start && block.end <= extensional.range.end
            })
            .map(|extensional| extensional.origin)
    }

    /// Returns the candidate applications for a fact of a derived block:
    /// the logged applications within the block of rules writing the fact's table, ascending.
    ///
    /// The earliest is almost always the true producer.
    /// If the log holds no such application (it can be out of date after the engine recovers
    /// from a program update), every rule writing the table is a candidate,
    /// at the block's end.
    fn candidates(&self, table: TableId, block: &Range<Revision>) -> Vec<(usize, u64)> {
        let program = self.program;
        let candidates: Vec<(usize, u64)> = self
            .applications
            .iter()
            .filter(|logged| block.start.0 <= logged.step && logged.step < block.end.0)
            .map(|logged| (logged.rule as usize, logged.step))
            .filter(|&(rule, _)| {
                program
                    .rules
                    .get(rule)
                    .is_some_and(|rule| targets(rule, table))
            })
            .collect();
        if !candidates.is_empty() {
            return candidates;
        }
        (0..program.rules.len())
            .filter(|&rule| {
                !program.rules[rule].never_fires && targets(&program.rules[rule], table)
            })
            .map(|rule| (rule, block.end.0))
            .collect()
    }

    /// Searches a proof for the fact.
    pub(super) fn search(
        &mut self,
        table: TableId,
        row: &[StorageValue],
    ) -> Result<Found, Diagnostic> {
        let key = (table, row.to_vec());
        if let Some(memoized) = self.memo.get(&key) {
            return Ok(match memoized {
                Some(node) => Found::Proven(Rc::clone(node)),
                None => Found::Missing { blocked: false },
            });
        }
        if self.path.contains(&key) {
            return Ok(Found::Missing { blocked: true });
        }

        // The fact's first block decides the outcome: no block means the
        // fact is not stored, an extensional block means a leaf, and a
        // derived block bounds the candidate applications and the premise reads.
        let Some(block) = self.first_block(table, row) else {
            self.memo.insert(key, None);
            return Ok(Found::Missing { blocked: false });
        };
        if let Some(origin) = self.extensional_origin(table, &block) {
            let node = Rc::new(ProofNode {
                table,
                row: row.to_vec(),
                proof: Proof::Asserted { origin },
            });
            self.memo.insert(key, Some(Rc::clone(&node)));
            return Ok(Found::Proven(node));
        }

        self.path.insert(key.clone());
        let mut blocked = false;
        let mut proven = None;
        for (rule, step) in self.candidates(table, &block) {
            let candidate = CandidateApplication {
                rule,
                step,
                table,
                row,
            };
            match self.attempt(&candidate)? {
                Attempted::Proven(node) => {
                    proven = Some(node);
                    break;
                }
                Attempted::Blocked => blocked = true,
                Attempted::NoWitness => {}
            }
        }
        self.path.remove(&key);

        match proven {
            Some(node) => {
                self.memo.insert(key, Some(Rc::clone(&node)));
                Ok(Found::Proven(node))
            }
            None => {
                if !blocked {
                    self.memo.insert(key, None);
                }
                Ok(Found::Missing { blocked })
            }
        }
    }

    /// Attempts to explain the fact by one candidate application.
    ///
    /// Each head of the rule that writes the fact's table (an ordinary head or an existential
    /// insert) grounds the body by the fact's values.
    /// The first grounding with a witness whose premises are all proven wins.
    fn attempt(&mut self, candidate: &CandidateApplication<'_>) -> Result<Attempted, Diagnostic> {
        let program = self.program;
        let rule = &program.rules[candidate.rule];
        let values = self.decode(candidate.row);

        // A fetched fact is proven by the binding rows its request was built from.
        #[cfg(feature = "sparql")]
        if rule.fetch.is_some() {
            return self.attempt_fetch(candidate);
        }

        // A fact of an aggregate head is proven by its group.
        if let Some(aggregate) = &rule.aggregate
            && aggregate.table == candidate.table
        {
            return self.attempt_aggregate(candidate, &values);
        }

        // An ordinary head grounds the body through its columns. An
        // existential insert grounds it through its frontier columns,
        // if the fact's nulls and constants match the insert.
        let mut groundings: Vec<(Vec<(usize, DataValue)>, bool)> = Vec::new();
        for head in rule
            .heads
            .iter()
            .filter(|head| head.table == candidate.table)
        {
            groundings.push(head_grounding(rule, head, &values));
        }
        if let Some(existential) = &rule.existential {
            for insert in existential
                .inserts
                .iter()
                .filter(|insert| insert.table == candidate.table)
            {
                if let Some(grounding) =
                    insert_grounding(existential, insert, &values, candidate.row)
                {
                    groundings.push((grounding, false));
                }
            }
        }

        let mut blocked = false;
        for (grounding, nondeterministic) in groundings {
            let witnesses = self.witnesses(candidate.rule, &grounding, candidate.step);
            match self.prove_witnesses(candidate, &witnesses, nondeterministic, None, None)? {
                Attempted::NoWitness => {}
                Attempted::Blocked => blocked = true,
                proven => return Ok(proven),
            }
        }
        Ok(if blocked {
            Attempted::Blocked
        } else {
            Attempted::NoWitness
        })
    }

    /// Attempts to explain a fact of an aggregate head.
    ///
    /// The group columns ground the body, and the group's contributions are counted.
    /// A min or max result also carries the premises of the witness row achieving it,
    /// if they are all proven.
    fn attempt_aggregate(
        &mut self,
        candidate: &CandidateApplication<'_>,
        values: &[DataValue],
    ) -> Result<Attempted, Diagnostic> {
        let program = self.program;
        let rule = &program.rules[candidate.rule];
        let aggregate = rule
            .aggregate
            .as_ref()
            .expect("an aggregate candidate's rule carries its aggregate");

        let mut grounding: Vec<(usize, DataValue)> = Vec::new();
        let mut results: Vec<(usize, StorageValue)> = Vec::new();
        for ((reference, value), &encoded) in
            aggregate.columns.iter().zip(values).zip(candidate.row)
        {
            match *reference {
                AggregateColumn::Group(index) => {
                    grounding.push((aggregate.group[index], value.clone()))
                }
                AggregateColumn::Result(index) => results.push((index, encoded)),
                AggregateColumn::Post(_) => {}
            }
        }

        let witnesses = self.witnesses(candidate.rule, &grounding, candidate.step);
        if witnesses.is_empty() {
            return Ok(Attempted::NoWitness);
        }
        let positions = combined_positions(rule);

        // The evidence summary: the number of distinct contributions
        // the first aggregate ranged over within this group.
        let matches = match aggregate.functions.first() {
            Some(spec) => {
                let scope = scope_columns(rule, spec);
                let mut seen = HashSet::new();
                witnesses.for_each_row(|witness| {
                    let projected: Vec<StorageValue> = scope
                        .iter()
                        .map(|&column| witness[positions[column]])
                        .collect();
                    seen.insert(projected);
                });
                seen.len()
            }
            None => witnesses.row_count(),
        };

        // A min/max result names a witness row: the one achieving it.
        let keep = results.iter().find_map(|&(spec_index, target)| {
            let spec = &aggregate.functions[spec_index];
            matches!(spec.kind, AggregateKind::Min | AggregateKind::Max)
                .then_some(spec.input)
                .flatten()
                .map(|input| (positions[input], target))
        });

        match self.prove_witnesses(candidate, &witnesses, false, keep, Some(matches))? {
            Attempted::Proven(node) => Ok(Attempted::Proven(node)),
            // The group exists even if no single witness is fully proven
            // (or none is called for): the summary explains it.
            _ => Ok(Attempted::Proven(candidate.derived(
                Vec::new(),
                Vec::new(),
                Some(matches),
                false,
            ))),
        }
    }

    /// Tries the witnesses in order, proving each one's premises,
    /// until one witness is fully proven.
    ///
    /// `keep` admits only the rows holding the given value at the given position,
    /// and `matches` is attached as the aggregate summary.
    fn prove_witnesses(
        &mut self,
        candidate: &CandidateApplication<'_>,
        witnesses: &Trie,
        nondeterministic: bool,
        keep: Option<(usize, StorageValue)>,
        matches: Option<usize>,
    ) -> Result<Attempted, Diagnostic> {
        let program = self.program;
        let rule = &program.rules[candidate.rule];
        let positions = combined_positions(rule);
        let mut blocked = false;

        let mut rows = Vec::with_capacity(witnesses.row_count());
        witnesses.for_each_row(|row| rows.push(row.to_vec()));
        'witnesses: for witness in &rows {
            if let Some((column, value)) = keep
                && witness[column] != value
            {
                continue;
            }
            let mut premises = Vec::with_capacity(rule.body.len());
            for atom in &rule.body {
                let premise: Vec<StorageValue> = atom
                    .variables
                    .iter()
                    .map(|&variable| witness[positions[variable]])
                    .collect();
                match self.search(atom.table, &premise)? {
                    Found::Proven(node) => premises.push(node),
                    Found::Missing { blocked: hit } => {
                        blocked |= hit;
                        continue 'witnesses;
                    }
                }
            }
            return Ok(Attempted::Proven(candidate.derived(
                witness.to_vec(),
                premises,
                matches,
                nondeterministic,
            )));
        }
        Ok(if blocked {
            Attempted::Blocked
        } else {
            Attempted::NoWitness
        })
    }

    /// Runs the rule's body grounded by the head fact, restricted to
    /// the data before the application step, returning every witness:
    /// distinct combined rows (the variables in the rule's order, then
    /// computed columns).
    fn witnesses(
        &mut self,
        rule_index: usize,
        grounding: &[(usize, DataValue)],
        step: u64,
    ) -> Trie {
        let program = self.program;
        let rule = &program.rules[rule_index];
        let mut plan = OperationPlan::new();
        let space = BodySpace::new(&mut plan, rule);

        // One join over all body atoms, each restricted to the data
        // before the application (rounded up to its table's blocks);
        // the grounding holds the head fact's values.
        let mut inputs = Vec::with_capacity(rule.body.len());
        for atom in &rule.body {
            let end = self.aligned_end(atom.table, step);
            inputs.push(plan.load(atom.table, Revision(0)..end, space.atom_markers(atom)));
        }
        let node = space.join(&mut plan, inputs, space.order.clone(), grounding);
        // Negations hold against the saturated store, exactly as at
        // application time.
        let (node, _) = space.subtract_negations(&mut plan, node, Revision(0)..Revision(self.now));

        // The body evaluates as it did at the application: `NOW()` reads
        // the instant its run began at.
        let output = plan.output(node);
        let context = self.contexts.at(step);
        let mut results = plan.execute(self.manager, self.dictionary, context);
        std::mem::take(&mut results.outputs[output])
    }
}

/// Returns the grounding of a rule's body by one ordinary head writing the
/// fact, and whether a column was left out of it for calling a nondeterministic function
/// (such as `RAND()` or `STRUUID()`).
///
/// Such a value cannot be reproduced,
/// so the premises explain the fact except for that column.
fn head_grounding(
    rule: &CompiledRule,
    head: &HeadAtom,
    values: &[DataValue],
) -> (Vec<(usize, DataValue)>, bool) {
    let mut grounding: Vec<(usize, DataValue)> = Vec::new();
    let mut nondeterministic = false;
    for (&column, value) in head.columns.iter().zip(values) {
        if column >= rule.variables
            && rule.computed[column - rule.variables]
                .program
                .is_nondeterministic()
        {
            nondeterministic = true;
            continue;
        }
        grounding.push((column, value.clone()));
    }
    (grounding, nondeterministic)
}

/// Returns the grounding of a rule's body by one existential insert writing the fact.
///
/// Returns `None` if the insert cannot have written the fact.
/// Frontier columns ground the body, existential columns must hold consistent nulls,
/// and constants must match.
fn insert_grounding(
    existential: &ExistentialHead,
    insert: &ExistentialAtom,
    values: &[DataValue],
    row: &[StorageValue],
) -> Option<Vec<(usize, DataValue)>> {
    let mut grounding: Vec<(usize, DataValue)> = Vec::new();
    let mut nulls: HashMap<usize, StorageValue> = HashMap::new();
    for ((term, value), &encoded) in insert.columns.iter().zip(values).zip(row) {
        match term {
            ExistentialTerm::Frontier(slot) => {
                let column = existential.frontier[*slot];
                match grounding.iter().find(|&&(bound, _)| bound == column) {
                    Some((_, bound)) if bound != value => return None,
                    Some(_) => {}
                    None => grounding.push((column, value.clone())),
                }
            }
            ExistentialTerm::Existential(slot) => {
                if !matches!(value.as_ref(), ValueRef::Null(_)) {
                    return None;
                }
                match nulls.get(slot) {
                    Some(&seen) if seen != encoded => return None,
                    _ => {
                        nulls.insert(*slot, encoded);
                    }
                }
            }
            ExistentialTerm::Constant(constant) => {
                if constant != value {
                    return None;
                }
            }
        }
    }
    Some(grounding)
}
