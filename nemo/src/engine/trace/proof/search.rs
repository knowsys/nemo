//! The proof search over the stored tables.

use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    management::{Revision, TableId, TableManager},
    plan::OperationPlan,
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
            body::{BodySpace, combined_positions},
            contexts::RunContexts,
            extensional::ExtensionalBlock,
        },
        trace::proof::{Explanation, ProofNode, targets},
    },
    model::AggregateKind,
};

/// One candidate application of a rule, to explain one fact.
#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateApplication<'f> {
    /// The index of the rule in the compiled program.
    pub(super) rule: usize,
    /// The engine step of the application,
    /// or the revision at which the fact was added if the application log has no entry for it.
    pub(super) step: u64,
    /// The fact's table.
    pub(super) table: TableId,
    /// The fact's encoded row.
    pub(super) row: &'f [StorageValue],
}

impl CandidateApplication<'_> {
    /// Returns the proof node that derives the fact by this application.
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
            proof: Explanation::Derived {
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

/// The outcome of every fact that a search met, by table and row,
/// which is the fact's proof, or `None` if it has none.
pub(super) type Outcomes = HashMap<(TableId, Vec<StorageValue>), Option<Rc<ProofNode>>>;

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
    /// The evaluation context of each reasoning run.
    pub(super) contexts: &'a RunContexts,
    /// The extensional blocks of each table, which contain loaded data.
    extensional: &'a HashMap<TableId, Vec<ExtensionalBlock>>,
    /// The engine's current step.
    ///
    /// Every stored row's revision lies below it.
    pub(super) now: u64,
    /// The outcomes of the facts met so far, which outlive the search.
    outcomes: &'a mut Outcomes,
}

impl<'a> ProofSearch<'a> {
    /// Returns a search over the engine's stores,
    /// which adds its outcomes to `outcomes`, beside those of earlier searches.
    ///
    /// Fails if no program is loaded.
    pub(super) fn over(
        engine: &'a mut Engine,
        outcomes: &'a mut Outcomes,
    ) -> Result<Self, Diagnostic> {
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
            outcomes,
        })
    }

    /// Returns a proof of the fact given by its values,
    /// or `None` if it is not stored or no candidate application derives it.
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
        self.search(table, &row)
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

    /// Returns whether the table's row was added before the step.
    pub(super) fn added_before(&self, table: TableId, row: &[StorageValue], step: u64) -> bool {
        self.manager
            .revision_of(table, row)
            .is_some_and(|added| added.0 < step)
    }

    /// Returns whether a revision lies in one of the table's extensional blocks.
    fn is_extensional(&self, table: TableId, revision: Revision) -> bool {
        self.extensional
            .get(&table)
            .into_iter()
            .flatten()
            .any(|extensional| extensional.range.contains(&revision))
    }

    /// Returns the candidate applications for a fact added at a derived revision,
    /// each as a rule index and a step.
    ///
    /// The candidate is the logged application of that step, which wrote the fact.
    /// The log can lack that application after the engine recovers from a program update.
    /// Then every rule that writes the table and can fire is a candidate, at the revision.
    fn candidates(&self, table: TableId, revision: Revision) -> Vec<(usize, u64)> {
        let program = self.program;
        let at = self
            .applications
            .partition_point(|logged| logged.step < revision.0);
        if let Some(logged) = self.applications.get(at)
            && logged.step == revision.0
            && program
                .rules
                .get(logged.rule as usize)
                .is_some_and(|rule| targets(rule, table))
        {
            return vec![(logged.rule as usize, logged.step)];
        }
        (0..program.rules.len())
            .filter(|&rule| {
                !program.rules[rule].never_fires && targets(&program.rules[rule], table)
            })
            .map(|rule| (rule, revision.0))
            .collect()
    }

    /// Returns a proof of the fact, remembered for later searches,
    /// or `None` if it is not stored or no candidate application derives it.
    pub(super) fn search(
        &mut self,
        table: TableId,
        row: &[StorageValue],
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
        let key = (table, row.to_vec());
        if let Some(known) = self.outcomes.get(&key) {
            return Ok(known.clone());
        }

        // The revision at which the fact was added decides the outcome.
        // Without one, the fact is not stored. An extensional revision makes the fact a leaf,
        // and a derived one names the application.
        let mut proven = None;
        if let Some(revision) = self.manager.revision_of(table, row) {
            if self.is_extensional(table, revision) {
                proven = Some(Rc::new(ProofNode {
                    table,
                    row: row.to_vec(),
                    proof: Explanation::Loaded,
                }));
            } else {
                for (rule, step) in self.candidates(table, revision) {
                    let candidate = CandidateApplication {
                        rule,
                        step,
                        table,
                        row,
                    };
                    proven = self.attempt(&candidate)?;
                    if proven.is_some() {
                        break;
                    }
                }
            }
        }

        self.outcomes.insert(key, proven.clone());
        Ok(proven)
    }

    /// Attempts to explain the fact by one candidate application.
    ///
    /// Each head of the rule that writes the fact's table, an ordinary head or an existential
    /// insert, grounds the body by the fact's values.
    /// The first grounding with a witness whose premises are all proven wins.
    fn attempt(
        &mut self,
        candidate: &CandidateApplication<'_>,
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
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

        for (grounding, nondeterministic) in groundings {
            let witnesses = self.witnesses(candidate.rule, &grounding, candidate.step);
            let proven =
                self.prove_witnesses(candidate, &witnesses, nondeterministic, None, None)?;
            if proven.is_some() {
                return Ok(proven);
            }
        }
        Ok(None)
    }

    /// Attempts to explain a fact of an aggregate head.
    ///
    /// The group columns ground the body, and the group's contributions are counted.
    /// A min or max result also has the premises of the witness row that achieves it,
    /// if they are all proven.
    fn attempt_aggregate(
        &mut self,
        candidate: &CandidateApplication<'_>,
        values: &[DataValue],
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
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
            return Ok(None);
        }
        let positions = combined_positions(rule);

        // The summary of the group is the number of distinct contributions
        // that the first aggregate ranged over within it.
        let matches = match aggregate.functions.first() {
            Some(spec) => {
                let scope = scope_columns(rule, spec);
                let mut seen = HashSet::new();
                for witness in &witnesses {
                    let projected: Vec<StorageValue> = scope
                        .iter()
                        .map(|&column| witness[positions[column]])
                        .collect();
                    seen.insert(projected);
                }
                seen.len()
            }
            None => witnesses.len(),
        };

        // A min or max result names a witness row, the one that achieves it.
        let keep = results.iter().find_map(|&(spec_index, target)| {
            let spec = &aggregate.functions[spec_index];
            matches!(spec.kind, AggregateKind::Min | AggregateKind::Max)
                .then_some(spec.input)
                .flatten()
                .map(|input| (positions[input], target))
        });

        // The group exists even if no witness is fully proven, or none is needed,
        // and the summary explains it.
        let proven = self.prove_witnesses(candidate, &witnesses, false, keep, Some(matches))?;
        Ok(Some(proven.unwrap_or_else(|| {
            candidate.derived(Vec::new(), Vec::new(), Some(matches), false)
        })))
    }

    /// Tries the witnesses in order, proving each one's premises,
    /// until one witness is fully proven.
    ///
    /// `keep`, if given, keeps only the witnesses that have the given value at the given
    /// position.
    /// `matches` becomes the aggregate summary of the proof.
    fn prove_witnesses(
        &mut self,
        candidate: &CandidateApplication<'_>,
        witnesses: &[Vec<StorageValue>],
        nondeterministic: bool,
        keep: Option<(usize, StorageValue)>,
        matches: Option<usize>,
    ) -> Result<Option<Rc<ProofNode>>, Diagnostic> {
        let program = self.program;
        let rule = &program.rules[candidate.rule];
        let positions = combined_positions(rule);

        'witnesses: for witness in witnesses {
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
                    Some(node) => premises.push(node),
                    None => continue 'witnesses,
                }
            }
            return Ok(Some(candidate.derived(
                witness.to_vec(),
                premises,
                matches,
                nondeterministic,
            )));
        }
        Ok(None)
    }

    /// Runs the rule's body grounded by the head fact over the data before the application step,
    /// and returns every witness.
    ///
    /// A witness is a distinct combined row,
    /// with the variables in the rule's order and then the computed columns.
    /// Each premise of a witness was added before the step,
    /// so a premise is always older than the fact it supports.
    fn witnesses(
        &mut self,
        rule_index: usize,
        grounding: &[(usize, DataValue)],
        step: u64,
    ) -> Vec<Vec<StorageValue>> {
        let joined = self.joined(rule_index, grounding, step);

        // A read that ends inside a block also returns rows added from the step on,
        // so the premises from such a read are checked.
        // A body table read up to exactly the step returns no such rows.
        let rule = &self.program.rules[rule_index];
        let positions = combined_positions(rule);
        let checked: Vec<_> = rule
            .body
            .iter()
            .filter(|atom| self.aligned_end(atom.table, step) > Revision(step))
            .collect();
        let mut witnesses = Vec::with_capacity(joined.row_count());
        let mut premise = Vec::new();
        joined.for_each_row(|witness| {
            let before = checked.iter().all(|atom| {
                premise.clear();
                premise.extend(
                    atom.variables
                        .iter()
                        .map(|&variable| witness[positions[variable]]),
                );
                self.added_before(atom.table, &premise, step)
            });
            if before {
                witnesses.push(witness.to_vec());
            }
        });
        witnesses
    }

    /// Runs the rule's body grounded by the head fact, and returns the distinct combined rows.
    ///
    /// Each body table is read up to the end of the block that contains the step.
    fn joined(&mut self, rule_index: usize, grounding: &[(usize, DataValue)], step: u64) -> Trie {
        let program = self.program;
        let rule = &program.rules[rule_index];
        let mut plan = OperationPlan::new();
        let space = BodySpace::new(&mut plan, rule);

        // One join over all body atoms, each restricted to the data before the application,
        // rounded up to its table's blocks. The grounding contains the head fact's values.
        let mut inputs = Vec::with_capacity(rule.body.len());
        for atom in &rule.body {
            let end = self.aligned_end(atom.table, step);
            inputs.push(plan.load(atom.table, Revision(0)..end, space.atom_markers(atom)));
        }
        let node = space.join(&mut plan, inputs, space.order.clone(), grounding);
        // Negations hold against the saturated store, exactly as at
        // application time.
        let (node, _) = space.subtract_negations(&mut plan, node, Revision(0)..Revision(self.now));

        // The body is evaluated as it was at the application,
        // so `NOW()` returns the instant at which that run began.
        let output = plan.output(node);
        let context = self.contexts.at(step);
        let mut results = plan.execute(self.manager, self.dictionary, context);
        std::mem::take(&mut results.outputs[output])
    }
}

/// Returns the grounding of a rule's body by one ordinary head that writes the fact,
/// and whether a column was left out of it because it calls a nondeterministic function,
/// such as `RAND()`.
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

/// Returns the grounding of a rule's body by one existential insert that writes the fact.
///
/// Frontier columns ground the body, existential columns must contain consistent nulls,
/// and constants must match.
/// Returns `None` if the insert cannot have written the fact.
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
