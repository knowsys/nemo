//! Rule application: one semi-naive step as an operation plan.

use nemo_physical::{
    dictionary::Dictionary,
    function::{EvaluationContext, OpCode},
    management::{Revision, TableId, TableManager},
    plan::{
        AggregateColumnSpec, AggregateFunction, ComputedColumnSpec, Expression, FilterSpec,
        JoinArena, Marker, NodeId, OperationPlan, PhaseTimings, PlanLayout, PlanResults,
        WorkCounts,
    },
    storage::StorageValue,
    trie::{RowBuffer, Trie},
    value::ErrorCounts,
};

use crate::{
    engine::{
        compile::compiled::{
            AggregateColumn, CompiledAggregate, CompiledAggregateFunction, CompiledRule,
            ExistentialAtom, ExistentialHead, ExistentialTerm,
        },
        evaluate::body::{BodySpace, filtered, unpatched},
    },
    model::AggregateKind,
};

/// One entry of the application log: a rule application that stored rows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LoggedApplication {
    /// The rule's index in the compiled program.
    pub(crate) rule: u32,
    /// The engine step of the application.
    pub(crate) step: u64,
}

/// The result of one rule application: the derived rows per head table and the execution's
/// statistics.
#[derive(Debug, Default)]
pub(crate) struct Applied {
    /// The fresh rows per head table, as tries.
    pub(crate) outputs: Vec<(TableId, Trie)>,
    /// The evaluation errors, counted per kind.
    pub(crate) errors: ErrorCounts,
    /// The execution's phase accounting.
    pub(crate) timings: PhaseTimings,
    /// The execution's work counters.
    pub(crate) counts: WorkCounts,
    /// One observation per semi-naive variant, if the application was asked to observe.
    pub(crate) observations: Vec<VariantObservation>,
}

/// The input sizes of one semi-naive variant relative to the rule's variable order,
/// reported as telemetry.
///
/// It records whether the delta atom binds the order's first variable,
/// and how its block compares with the smallest block among the atoms that do.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VariantObservation {
    /// The variant: the body position of its delta atom.
    pub variant: usize,
    /// Rows of the delta atom's block.
    pub delta_rows: u64,
    /// Rows of the smallest block among the atoms binding the order's
    /// first variable.
    pub leading_rows: u64,
    /// Whether the delta atom binds the order's first variable.
    pub delta_leads: bool,
}

/// The loads of one semi-naive variant.
#[derive(Debug)]
struct VariantLoads {
    /// The variant's loads, as a range of indices into the skeleton's
    /// loads.
    loads: std::ops::Range<usize>,
    /// Index into the skeleton's loads of the delta atom's load.
    delta: usize,
    /// Indices of the loads of atoms binding the order's first variable.
    leading: Vec<usize>,
}

/// How a skeleton load's revision range derives from the rule's delta
/// window at application time.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RangeRole {
    /// Everything before the rule's last application.
    Before,
    /// The delta since the rule's last application.
    Delta,
    /// Everything up to the present.
    Full,
}

/// A load node of a skeleton, with its table and range derivation.
type LoadNode = (NodeId, TableId, RangeRole);

/// The plan skeletons of a program's rules, by rule index,
/// each built on the rule's first application.
///
/// A program compiled anew starts without skeletons,
/// since its join orders are chosen for the whole program and may have changed.
#[derive(Debug, Default)]
pub(crate) struct PlanSkeletons(Vec<Option<PlanSkeleton>>);

impl PlanSkeletons {
    /// Forgets every skeleton, for a program compiled anew.
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    /// Returns the skeleton of the rule at an index, building it on first use.
    pub(crate) fn of(&mut self, position: usize, rule: &CompiledRule) -> &mut PlanSkeleton {
        if self.0.len() <= position {
            self.0.resize_with(position + 1, || None);
        }
        self.0[position].get_or_insert_with(|| PlanSkeletonBuilder::new(rule).build())
    }

    /// Returns the skeleton of the rule at an index, if it was built.
    #[cfg(test)]
    pub(crate) fn get(&self, position: usize) -> Option<&PlanSkeleton> {
        self.0.get(position)?.as_ref()
    }
}

/// A rule's reusable execution plan.
///
/// Its structure and layout are built once.
/// Only the loads' revision ranges change between applications.
#[derive(Debug)]
pub(crate) struct PlanSkeleton {
    /// The operation plan.
    plan: OperationPlan,
    /// Its cached layout.
    layout: PlanLayout,
    /// The load nodes to retarget, with their tables and range
    /// derivations.
    loads: Vec<LoadNode>,
    /// Per semi-naive variant, the loads an observation compares.
    variants: Vec<VariantLoads>,
    /// The scratch its joins build in, reused across applications.
    arena: JoinArena,
    /// Per head, its table and plan output index.
    targets: Vec<(TableId, usize)>,
    /// The existential outputs, if the rule has existential variables.
    probe: Option<ExistentialProbe>,
}

/// Applies one rule at the given engine step,
/// evaluating its filters and computed columns in `context`.
///
/// Returns the derived rows per head table, without the rows already stored,
/// and the evaluation-error counts.
///
/// Each application sets the loads' revision ranges to the rule's delta window
/// and executes the [skeleton](PlanSkeleton) again.
/// On the first application the delta window covers everything,
/// so every variant except the first reads an empty "before" range and does no work.
pub(crate) fn apply(
    rule: &CompiledRule,
    skeleton: &mut PlanSkeleton,
    step: u64,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
    observe: bool,
) -> Applied {
    // An aggregate rule always reads everything: its groups recompute
    // whole, and the head dedup drops what is already stored.
    let last = if rule.aggregate.is_some() {
        Revision(0)
    } else {
        Revision(rule.last_applied)
    };
    let now = Revision(step);
    // A delta window starts at the table's block boundary at or before
    // the rule's last application. The rows this adds are derived again
    // and removed by the head deduplication, and consolidation need not
    // keep a block boundary at every rule's last application. A rule
    // that calls a nondeterministic function (such as `RAND()`) would
    // compute new values for the rows it reads again, so it reads exact
    // windows, and its last application stays a wall (a revision
    // consolidation does not merge across).
    let snapping = !rule.exact_windows();
    let aggregating = rule.aggregate.is_some();

    // Clear the previous application's join scratch, keeping its memory.
    skeleton.arena.reset();
    let ranges = skeleton.retarget(last, now, snapping, aggregating, manager);

    // The observation reads block sizes only, before the run; an
    // aggregate rule has one degenerate variant and nothing to compare.
    let observations = if observe && !aggregating {
        skeleton.observe(&ranges, manager)
    } else {
        Vec::new()
    };

    let results = skeleton.plan.execute_with_layout(
        &skeleton.layout,
        manager,
        dictionary,
        context,
        &skeleton.arena,
    );

    Applied {
        observations,
        ..deliver(rule, skeleton, results, dictionary)
    }
}

impl PlanSkeleton {
    /// Returns the plan's layout.
    #[cfg(test)]
    pub(crate) fn layout(&self) -> &PlanLayout {
        &self.layout
    }

    /// Sets every load's revision range for the rule's delta window `last..now`,
    /// returning the ranges by load.
    ///
    /// A variant with an empty input derives nothing.
    /// If any of its loads reads no rows in its range, every one of its loads reads an empty
    /// range, so the plan neither prepares the column orders its atoms would need nor runs its
    /// join.
    fn retarget(
        &mut self,
        last: Revision,
        now: Revision,
        snapping: bool,
        aggregating: bool,
        manager: &mut TableManager,
    ) -> Vec<std::ops::Range<Revision>> {
        let mut ranges = Vec::with_capacity(self.loads.len());
        for &(_, table, role) in &self.loads {
            let start = if snapping {
                manager.aligned_floor(table, last)
            } else {
                last
            };
            ranges.push(match role {
                RangeRole::Full => Revision(0)..now,
                RangeRole::Before => Revision(0)..start,
                RangeRole::Delta => start..now,
            });
        }
        if !aggregating {
            for variant in &self.variants {
                let mut empty = false;
                for load in variant.loads.clone() {
                    let (_, table, _) = self.loads[load];
                    if manager.row_count(table, ranges[load].clone()) == 0 {
                        empty = true;
                        break;
                    }
                }
                if empty {
                    for load in variant.loads.clone() {
                        let start = ranges[load].start;
                        ranges[load] = start..start;
                    }
                }
            }
        }
        for (&(node, _, _), range) in self.loads.iter().zip(&ranges) {
            self.plan.set_load_range(node, range.clone());
        }
        ranges
    }

    /// Returns one observation per semi-naive variant,
    /// from the row counts of the ranges [retarget](Self::retarget) set.
    fn observe(
        &self,
        ranges: &[std::ops::Range<Revision>],
        manager: &TableManager,
    ) -> Vec<VariantObservation> {
        let rows = |load: usize| {
            let (_, table, _) = self.loads[load];
            manager.row_count(table, ranges[load].clone()) as u64
        };
        let mut observations = Vec::with_capacity(self.variants.len());
        for (variant, loads) in self.variants.iter().enumerate() {
            let mut leading_rows = u64::MAX;
            for &load in &loads.leading {
                leading_rows = leading_rows.min(rows(load));
            }
            observations.push(VariantObservation {
                variant,
                delta_rows: rows(loads.delta),
                leading_rows,
                delta_leads: loads.leading.contains(&loads.delta),
            });
        }
        observations
    }
}

/// Returns the prefix of the join order covering the markers an aggregation reads,
/// if its leading `groups` markers are exactly the join order's first markers.
///
/// Returns `None` if they are not, or if a marker read is not in the order.
fn join_order_prefix(order: &[Marker], read: &[Marker], groups: usize) -> Option<Vec<Marker>> {
    if !order.starts_with(&read[..groups]) {
        return None;
    }

    let mut deepest = groups;
    for marker in read {
        deepest = deepest.max(order.iter().position(|candidate| candidate == marker)? + 1);
    }

    Some(order[..deepest].to_vec())
}

/// Returns the physical function of one aggregate, over the markers of the rule's columns.
fn aggregate_function(spec: &CompiledAggregateFunction, markers: &[Marker]) -> AggregateFunction {
    let value = spec.input.map(|column| markers[column]);
    let value = || value.expect("every aggregate but a count reads a value");
    match spec.kind {
        AggregateKind::Count => AggregateFunction::Count,
        AggregateKind::Sum => AggregateFunction::Sum(value()),
        AggregateKind::Min => AggregateFunction::Min(value()),
        AggregateKind::Max => AggregateFunction::Max(value()),
        AggregateKind::Average => AggregateFunction::Average(value()),
        AggregateKind::Tuple => AggregateFunction::Tuple(value()),
        AggregateKind::Map => AggregateFunction::Map {
            keys: value(),
            values: markers[spec.value.expect("a map aggregate reads its values")],
        },
    }
}

/// Returns the scope columns of one aggregate: its input column, a map aggregate's value column,
/// and its scope variables, or every body variable if it has no scope.
///
/// Each distinct row over these columns is one contribution to the aggregate.
pub(crate) fn scope_columns(rule: &CompiledRule, spec: &CompiledAggregateFunction) -> Vec<usize> {
    let mut columns: Vec<usize> = spec.input.into_iter().chain(spec.value).collect();
    match &spec.scope {
        Some(scope) => columns.extend(scope.iter().copied()),
        None => {
            for variable in 0..rule.variables {
                if !columns.contains(&variable) {
                    columns.push(variable);
                }
            }
        }
    }
    columns
}

/// A skeleton under construction (see [PlanSkeletonBuilder::build]).
struct PlanSkeletonBuilder<'r> {
    /// The rule.
    rule: &'r CompiledRule,
    /// The rule's columns in the plan.
    space: BodySpace<'r>,
    /// The plan under construction.
    plan: OperationPlan,
    /// The loads recorded so far.
    loads: Vec<LoadNode>,
    /// The head targets recorded so far.
    targets: Vec<(TableId, usize)>,
}

impl<'r> PlanSkeletonBuilder<'r> {
    /// Opens a plan with the rule's markers.
    fn new(rule: &'r CompiledRule) -> Self {
        let mut plan = OperationPlan::new();
        let space = BodySpace::new(&mut plan, rule);
        Self {
            rule,
            space,
            plan,
            loads: Vec::new(),
            targets: Vec::with_capacity(rule.heads.len() + 1),
        }
    }

    /// Builds the rule's plan skeleton (see [apply]).
    ///
    /// The evaluation is semi-naive.
    /// For each body position, one variant joins that atom's delta since the rule's last
    /// application with the atoms before it as of that application and the atoms after it as of
    /// now.
    /// Every variant is an ordinary join node,
    /// and the plan layer reads the tables' revision blocks itself.
    /// The body's operators apply once to the union of the variants,
    /// the negated atoms are subtracted from the result,
    /// and each head projects it to the head's columns and subtracts the stored rows.
    /// The plan layout runs these operators inside the variants' joins wherever it can,
    /// and has the joins emit only the columns the operators above them read.
    fn build(mut self) -> PlanSkeleton {
        let (united, variants) = self.variants();
        let order = self.space.order.clone();
        let body = self.space.operators(&mut self.plan, united, &order, &[]);
        let (derived, negated_loads) =
            self.space
                .subtract_negations(&mut self.plan, body, unpatched());
        self.loads.extend(
            negated_loads
                .into_iter()
                .map(|(node, table)| (node, table, RangeRole::Full)),
        );
        self.heads(derived);
        self.aggregate_head(derived);
        let probe = self.existential_probe(derived);

        let layout = self.plan.layout();
        PlanSkeleton {
            plan: self.plan,
            layout,
            loads: self.loads,
            variants,
            arena: JoinArena::new(),
            targets: self.targets,
            probe,
        }
    }

    /// Returns the union of the semi-naive variants, each a bare join in the rule's order,
    /// and the loads of each variant.
    ///
    /// An aggregate rule has a single variant, which reads everything,
    /// since [apply] sets its delta window to the full range.
    fn variants(&mut self) -> (NodeId, Vec<VariantLoads>) {
        let rule = self.rule;
        let first_variable = rule.order.first().copied();
        let mut variants: Vec<NodeId> = Vec::new();
        let mut variant_loads: Vec<VariantLoads> = Vec::new();
        for pivot in 0..rule.body.len() {
            if pivot > 0 && rule.aggregate.is_some() {
                break;
            }
            let mut inputs = Vec::with_capacity(rule.body.len());
            let mut leading = Vec::new();
            let mut delta = 0;
            let first_load = self.loads.len();
            for (position, atom) in rule.body.iter().enumerate() {
                let role = match position.cmp(&pivot) {
                    std::cmp::Ordering::Less => RangeRole::Before,
                    std::cmp::Ordering::Equal => RangeRole::Delta,
                    std::cmp::Ordering::Greater => RangeRole::Full,
                };
                let node = self
                    .plan
                    .load(atom.table, unpatched(), self.space.atom_markers(atom));
                if position == pivot {
                    delta = self.loads.len();
                }
                if first_variable.is_some_and(|id| atom.variables.contains(&id)) {
                    leading.push(self.loads.len());
                }
                self.loads.push((node, atom.table, role));
                inputs.push(node);
            }
            variant_loads.push(VariantLoads {
                loads: first_load..self.loads.len(),
                delta,
                leading,
            });
            variants.push(self.plan.join(inputs, self.space.order.clone()));
        }
        (self.plan.union(variants), variant_loads)
    }

    /// Adds one head target: the node projected to the head's columns, in
    /// the head's order, without the rows the head table stores.
    fn target(&mut self, node: NodeId, table: TableId, head_markers: Vec<Marker>) {
        let existing = self.plan.load(table, unpatched(), head_markers.clone());
        self.loads.push((existing, table, RangeRole::Full));
        let projected = self.plan.project(node, head_markers);
        let fresh = self.plan.subtract(projected, existing);
        self.targets.push((table, self.plan.output(fresh)));
    }

    /// Adds the ordinary heads, one target each.
    fn heads(&mut self, derived: NodeId) {
        for head in &self.rule.heads {
            let head_markers = self.space.columns(&head.columns);
            self.target(derived, head.table, head_markers);
        }
    }

    /// Adds the aggregate head.
    ///
    /// The body result is projected to the group columns and every aggregate's scope columns,
    /// aggregated per group in one pass, extended by the post expressions,
    /// and deduplicated against the stored head like any other head.
    /// Each aggregate takes one contribution per distinct row of its own scope columns
    /// in the shared projection.
    fn aggregate_head(&mut self, derived: NodeId) {
        let rule = self.rule;
        let Some(aggregate) = &rule.aggregate else {
            return;
        };
        let projected = self.aggregate_scope(derived, aggregate);
        let results: Vec<Marker> = aggregate
            .functions
            .iter()
            .map(|_| self.plan.marker())
            .collect();
        let columns = aggregate
            .functions
            .iter()
            .zip(&results)
            .map(|(spec, &result)| {
                let function = aggregate_function(spec, &self.space.markers);
                // The scope columns start with the function's own value columns,
                // which `per` leaves out. A count reads no value, so `per` keeps all
                // of its scope columns.
                let scope = scope_columns(rule, spec);
                let (value, pair) = (spec.input.is_some(), spec.value.is_some());
                let own = match function {
                    AggregateFunction::Count => 0,
                    _ => usize::from(value) + usize::from(pair),
                };
                AggregateColumnSpec::new(function, result).per(self.space.columns(&scope[own..]))
            })
            .collect();
        let group = self.space.columns(&aggregate.group);
        let aggregated = self.plan.aggregate(projected, group, columns);

        let mut output_markers = self.space.columns(&aggregate.group);
        output_markers.extend(&results);
        let node = self.aggregate_post(aggregated, aggregate, &mut output_markers);
        let head_markers: Vec<Marker> = aggregate
            .columns
            .iter()
            .map(|reference| match *reference {
                AggregateColumn::Group(index) => output_markers[index],
                AggregateColumn::Result(index) => results[index],
                AggregateColumn::Post(index) => {
                    output_markers[aggregate.group.len() + results.len() + index]
                }
            })
            .collect();
        self.target(node, aggregate.table, head_markers);
    }

    /// Returns the projection an aggregation ranges over: the group columns,
    /// then every aggregate's scope columns.
    ///
    /// If the group columns lead the join order,
    /// the projection keeps the join order up to the deepest column read instead.
    /// A prefix of the join's rows stays sorted, so no re-sort is needed,
    /// and the aggregation's set deduplicates the contributions
    /// whose scope columns do not directly follow the group columns.
    ///
    /// A body of one atom without filters, bindings, computed columns or negations,
    /// whose projection is the atom itself, aggregates directly over the stored blocks.
    /// The stored rows are already the distinct projection,
    /// so a project node would only derive them again.
    fn aggregate_scope(&mut self, derived: NodeId, aggregate: &CompiledAggregate) -> NodeId {
        let rule = self.rule;
        let mut list = self.space.columns(&aggregate.group);
        for spec in &aggregate.functions {
            for column in scope_columns(rule, spec) {
                let marker = self.space.markers[column];
                if !list.contains(&marker) {
                    list.push(marker);
                }
            }
        }
        if let Some(prefix) = join_order_prefix(&self.space.order, &list, aggregate.group.len()) {
            list = prefix;
        }
        let direct = rule.body.len() == 1
            && rule.filters.is_empty()
            && rule.bindings.is_empty()
            && rule.computed.is_empty()
            && rule.negated.is_empty()
            && list == self.space.atom_markers(&rule.body[0]);
        if direct {
            let atom = &rule.body[0];
            let node = self.plan.load(atom.table, unpatched(), list);
            self.loads.push((node, atom.table, RangeRole::Full));
            node
        } else {
            self.plan.project(derived, list)
        }
    }

    /// Returns the post expressions over an aggregation's output,
    /// computed columns whose markers extend the output order,
    /// or the aggregation itself if there are none.
    fn aggregate_post(
        &mut self,
        aggregated: NodeId,
        aggregate: &CompiledAggregate,
        output_markers: &mut Vec<Marker>,
    ) -> NodeId {
        let mut node = aggregated;
        for slot in &aggregate.post {
            let inputs = slot
                .inputs
                .iter()
                .map(|&index| output_markers[index])
                .collect();
            let result = self.plan.marker();
            let spec = ComputedColumnSpec::from_program(inputs, slot.program.clone(), result);
            output_markers.push(result);
            node = self.plan.compute(node, spec);
        }
        node
    }

    /// Returns the plan outputs of the existential head, if the rule has one.
    ///
    /// The body result is projected to the frontier,
    /// and one existence-only join checks against the stored rows
    /// whether the whole head is already satisfied for each frontier tuple.
    fn existential_probe(&mut self, derived: NodeId) -> Option<ExistentialProbe> {
        let Some(head) = &self.rule.existential else {
            return None;
        };
        let existential_markers: Vec<Marker> =
            (0..head.existentials).map(|_| self.plan.marker()).collect();
        if head.frontier.is_empty() {
            // Without a frontier, the head asks two yes/no questions:
            // does any body match exist, and is the head already satisfied?
            let projected = self.plan.project(derived, vec![self.space.markers[0]]);
            let matches = self.plan.output(projected);
            let blocked = self.satisfaction(Vec::new(), &[], &existential_markers, 1);
            return Some(ExistentialProbe::Empty {
                matches,
                blocked: self.plan.output(blocked),
            });
        }
        let frontier_markers = self.space.columns(&head.frontier);
        let delta = self.plan.project(derived, frontier_markers.clone());
        let blocked = self.satisfaction(
            vec![delta],
            &frontier_markers,
            &existential_markers,
            frontier_markers.len(),
        );
        let unsatisfied = self.plan.subtract(delta, blocked);
        Some(ExistentialProbe::Frontier(self.plan.output(unsatisfied)))
    }

    /// Returns the satisfaction check of the existential head: the check atoms
    /// joined in the order of the frontier, then the existential markers
    /// (after any inputs given), projected to the leading `emitted` markers.
    fn satisfaction(
        &mut self,
        mut inputs: Vec<NodeId>,
        frontier_markers: &[Marker],
        existential_markers: &[Marker],
        emitted: usize,
    ) -> NodeId {
        let head = self
            .rule
            .existential
            .as_ref()
            .expect("satisfaction is planned for existential heads only");
        let mut order: Vec<Marker> = frontier_markers.to_vec();
        order.extend(existential_markers.iter().copied());
        let mut filters = Vec::new();
        for atom in &head.check {
            inputs.push(self.check_load(
                atom,
                frontier_markers,
                existential_markers,
                &mut order,
                &mut filters,
            ));
        }
        let joined = self.plan.join(inputs, order.clone());
        let checked = filtered(&mut self.plan, joined, filters);
        self.plan.project(checked, order[..emitted].to_vec())
    }

    /// Returns the load of one satisfaction-check atom.
    ///
    /// Frontier and existential columns bind their shared markers.
    /// Repetitions and constants take fresh markers, appended to the join order and constrained
    /// by filters.
    fn check_load(
        &mut self,
        atom: &ExistentialAtom,
        frontier_markers: &[Marker],
        existential_markers: &[Marker],
        order: &mut Vec<Marker>,
        filters: &mut Vec<FilterSpec>,
    ) -> NodeId {
        let mut column_markers: Vec<Marker> = Vec::with_capacity(atom.columns.len());
        for term in &atom.columns {
            let shared = match term {
                ExistentialTerm::Frontier(index) => Some(frontier_markers[*index]),
                ExistentialTerm::Existential(index) => Some(existential_markers[*index]),
                ExistentialTerm::Constant(_) => None,
            };
            let marker = match shared {
                Some(marker) if !column_markers.contains(&marker) => marker,
                binding => {
                    let fresh = self.plan.marker();
                    order.push(fresh);
                    // For a repeated variable, the fresh column must be the same term.
                    // For a constant, the fresh column must be that constant.
                    let other = match (binding, term) {
                        (Some(marker), _) => Expression::column(marker),
                        (None, ExistentialTerm::Constant(value)) => {
                            Expression::constant(value.clone())
                        }
                        _ => unreachable!("only constants have no shared marker"),
                    };
                    let same = Expression::apply(OpCode::Same, [Expression::column(fresh), other]);
                    filters.push(
                        FilterSpec::new(&same).expect("a comparison of two operands is small"),
                    );
                    fresh
                }
            };
            column_markers.push(marker);
        }
        let node = self.plan.load(atom.table, unpatched(), column_markers);
        self.loads.push((node, atom.table, RangeRole::Full));
        node
    }
}

/// Collects an execution's outputs into per-table rows,
/// creating fresh nulls for the unsatisfied frontier tuples of the existential head.
fn deliver(
    rule: &CompiledRule,
    skeleton: &PlanSkeleton,
    results: PlanResults,
    dictionary: &mut Dictionary,
) -> Applied {
    let mut outputs = results.outputs;
    let mut delivered = Vec::with_capacity(skeleton.targets.len());
    for &(table, output) in &skeleton.targets {
        delivered.push((table, std::mem::take(&mut outputs[output])));
    }
    if let (Some(head), Some(probe)) = (&rule.existential, skeleton.probe) {
        let unsatisfied: Vec<Vec<StorageValue>> = match probe {
            ExistentialProbe::Frontier(output) => {
                let mut rows = Vec::with_capacity(outputs[output].row_count());
                outputs[output].for_each_row(|row| rows.push(row.to_vec()));
                rows
            }
            ExistentialProbe::Empty { matches, blocked } => {
                if !outputs[matches].is_empty() && outputs[blocked].is_empty() {
                    vec![Vec::new()]
                } else {
                    Vec::new()
                }
            }
        };
        delivered.extend(mint(head, &unsatisfied, dictionary));
    }
    Applied {
        outputs: delivered,
        errors: results.errors,
        timings: results.timings,
        counts: results.counts,
        observations: Vec::new(),
    }
}

/// Creates one fresh null per existential variable per unsatisfied tuple,
/// and returns the rows of the insert atoms.
///
/// Fresh nulls cannot collide, so no deduplication is needed.
fn mint(
    head: &ExistentialHead,
    unsatisfied: &[Vec<StorageValue>],
    dictionary: &mut Dictionary,
) -> Vec<(TableId, Trie)> {
    let mut per_table: Vec<(TableId, RowBuffer)> = Vec::new();
    for row in unsatisfied {
        let nulls: Vec<_> = (0..head.existentials)
            .map(|_| {
                let id = dictionary.fresh_null();
                dictionary
                    .encode(nemo_physical::value::ValueRef::Null(id))
                    .expect("a fresh null encodes")
            })
            .collect();
        for insert in &head.inserts {
            let mut out = Vec::with_capacity(insert.columns.len());
            for term in &insert.columns {
                out.push(match term {
                    ExistentialTerm::Frontier(index) => row[*index],
                    ExistentialTerm::Existential(index) => nulls[*index],
                    ExistentialTerm::Constant(value) => dictionary
                        .encode(value.as_ref())
                        .expect("head constants were encoded when the rule compiled"),
                });
            }
            match per_table
                .iter_mut()
                .find(|(table, _)| *table == insert.table)
            {
                Some((_, buffer)) => buffer.push_row(&out),
                None => {
                    let mut buffer = RowBuffer::new(insert.columns.len());
                    buffer.push_row(&out);
                    per_table.push((insert.table, buffer));
                }
            }
        }
    }
    per_table
        .into_iter()
        .map(|(table, buffer)| (table, Trie::from_buffer(buffer)))
        .collect()
}

/// The plan outputs of an existential application.
#[derive(Debug, Clone, Copy)]
enum ExistentialProbe {
    /// The output of the unsatisfied frontier tuples, which receive fresh nulls.
    Frontier(usize),
    /// The outputs for an empty frontier.
    Empty {
        /// The output that is nonempty if any body match exists.
        matches: usize,
        /// The output that is nonempty if the head is already satisfied.
        blocked: usize,
    },
}
