use super::*;

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::{
    function::{OpCode, StackProgram},
    management::ColumnOrder,
    plan::layout::{AggregatePass, Delivery, MemberLayout, Step},
    storage::StorageValue,
    testing::{CELL_ROWS, assert_panics, int_buffer, int_rows},
    trie::RowBuffer,
    value::{DataValue, EvaluationError},
};

/// Registers a table with one subtable per given block of rows,
/// at revisions 1, 2, and so on.
fn add_table(manager: &mut TableManager, arity: usize, blocks: &[&[&[i64]]]) -> TableId {
    let table = manager.register("data", arity);
    for (index, rows) in blocks.iter().enumerate() {
        manager.add_rows(
            table,
            Revision(index as u64 + 1),
            ColumnOrder::identity(arity),
            int_buffer(arity, rows),
        );
    }
    table
}

/// Returns a range of revisions covering every block of the tests.
fn all() -> Range<Revision> {
    Revision(0)..Revision(100)
}

/// Returns a join of the inputs in the given order, projected to its leading `emitted` markers.
fn joined(
    plan: &mut OperationPlan,
    inputs: Vec<NodeId>,
    order: Vec<Marker>,
    emitted: usize,
) -> NodeId {
    let join = plan.join(inputs, order.clone());
    if emitted == order.len() {
        return join;
    }

    plan.project(join, order[..emitted].to_vec())
}

/// Returns random rows over a small integer domain.
fn random_rows(rng: &mut StdRng, arity: usize) -> Vec<Vec<i64>> {
    (0..rng.gen_range(0..15))
        .map(|_| (0..arity).map(|_| rng.gen_range(0..5)).collect())
        .collect()
}

/// Returns the join members of the units a layout executes, in order.
fn members(layout: &PlanLayout) -> Vec<&MemberLayout> {
    layout
        .steps
        .iter()
        .flat_map(|step| match step {
            Step::Unit(unit) => unit.members.as_slice(),
            Step::Aggregate(_) | Step::Concatenate { .. } => &[],
        })
        .collect()
}

/// Returns the one join member a layout executes.
fn the_member(layout: &PlanLayout) -> &MemberLayout {
    let members = members(layout);
    let [member] = members.as_slice() else {
        panic!("one join member was expected: {layout}");
    };

    member
}

/// Builds a plan that fuses, and returns it with its subtract-projecting join and its union.
///
/// Two joins R(x, z) ⋈ S(z, y), each over a different range of R's revisions, are united.
/// A join over the union projects to the columns `kept` lists
/// and subtracts the stored table.
fn subtract_projection_plan(
    r: TableId,
    s: TableId,
    stored: TableId,
    r_split: Revision,
    kept: &[usize],
) -> (OperationPlan, NodeId, NodeId) {
    let mut plan = OperationPlan::new();
    let [x, z, y] = plan.markers();
    let markers = [x, z, y];
    let mut joins = Vec::new();
    for (r_range, s_range) in [
        (Revision(0)..r_split, all()),
        (r_split..Revision(100), all()),
    ] {
        let r_load = plan.load(r, r_range, vec![x, z]);
        let s_load = plan.load(s, s_range, vec![z, y]);
        joins.push(joined(&mut plan, vec![r_load, s_load], vec![x, z, y], 3));
    }
    let union = plan.union(joins);
    let kept_markers: Vec<Marker> = kept.iter().map(|&column| markers[column]).collect();
    let existing = plan.load(stored, all(), kept_markers.clone());
    let projected = plan.project(union, kept_markers);
    let fresh = plan.subtract(projected, existing);
    plan.output(fresh);
    (plan, fresh, union)
}

#[test]
fn fused_subtract_projections_match_the_unfused_rows() {
    // The differential property of fusion: for every projection,
    // the fused execution computes exactly the rows of the unfused plan
    // (the same plan with a second consumer of the union),
    // which in turn match brute force.
    let mut rng = StdRng::seed_from_u64(23);
    for round in 0..120 {
        let r_rows = random_rows(&mut rng, 2);
        let s_rows = random_rows(&mut rng, 2);
        let projections: &[&[usize]] = &[&[0, 2], &[0, 1], &[2], &[0, 1, 2]];
        let kept = projections[round % projections.len()];
        let stored_rows: Vec<Vec<i64>> = random_rows(&mut rng, kept.len());

        let mut manager = TableManager::new();
        let r_halves = r_rows.split_at(r_rows.len() / 2);
        let r_refs: [Vec<&[i64]>; 2] = [
            r_halves.0.iter().map(Vec::as_slice).collect(),
            r_halves.1.iter().map(Vec::as_slice).collect(),
        ];
        let r = add_table(&mut manager, 2, &[&r_refs[0], &r_refs[1]]);
        let s_refs: Vec<&[i64]> = s_rows.iter().map(Vec::as_slice).collect();
        let s = add_table(&mut manager, 2, &[&s_refs]);
        let stored_refs: Vec<&[i64]> = stored_rows.iter().map(Vec::as_slice).collect();
        let stored = add_table(&mut manager, kept.len(), &[&stored_refs]);

        // Brute force over both joins' ranges.
        let mut expected = BTreeSet::new();
        for (r_side, s_side) in [(&r_halves.0, &s_rows), (&r_halves.1, &s_rows)] {
            for first in r_side.iter() {
                for second in s_side.iter() {
                    if first[1] != second[0] {
                        continue;
                    }
                    let full = [first[0], first[1], second[1]];
                    let projected: Vec<i64> = kept.iter().map(|&column| full[column]).collect();
                    if !stored_rows.contains(&projected) {
                        expected.insert(projected);
                    }
                }
            }
        }
        let expected: Vec<Vec<i64>> = expected.into_iter().collect();

        let (plan, fresh, _) = subtract_projection_plan(r, s, stored, Revision(2), kept);
        let layout = plan.layout();
        assert_eq!(
            layout.unit_members(fresh),
            Some(2),
            "round {round}: the subtraction runs inside both joins"
        );
        let mut dictionary = Dictionary::new();
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected, "round {round}");

        // The same plan with a second consumer must not fuse, and
        // must agree.
        let (mut defused, fresh, union) = subtract_projection_plan(r, s, stored, Revision(2), kept);
        defused.output(union);
        let layout = defused.layout();
        assert_eq!(
            layout.unit_members(union),
            Some(2),
            "round {round}: a second consumer materializes the union"
        );
        assert_eq!(
            layout.unit_members(fresh),
            Some(1),
            "round {round}: the subtraction reads the union's result"
        );
        let results = defused.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected, "round {round}");
    }
}

/// Returns a filter specification built from the parts of its program.
fn condition(
    inputs: Vec<Marker>,
    operations: Vec<OpCode>,
    constants: Vec<DataValue>,
) -> FilterSpec {
    let arity = inputs.len();
    let program = StackProgram::new(operations, constants, arity);
    FilterSpec::from_program(inputs, program)
}

#[test]
fn joins_derive_stored_orders() {
    // Converging edges: rows (x, y, z) with edge(x, y) and
    // edge(z, y) — the second load reads the table as (z, y), whose
    // derived order (y, z) exercises an on-demand reorder.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[2, 3], &[1, 3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let outgoing = plan.load(edge, all(), vec![x, y]);
    let incoming = plan.load(edge, all(), vec![z, y]);
    let converge = joined(&mut plan, vec![outgoing, incoming], vec![x, y, z], 3);
    plan.output(converge);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![
            vec![1, 2, 1],
            vec![1, 3, 1],
            vec![1, 3, 2],
            vec![2, 3, 1],
            vec![2, 3, 2],
        ]
    );
    assert_eq!(results.counts.arrangements, 1, "one arrangement in (y, z)");
    assert_eq!(results.counts.arranged_rows, 3);

    // The delivered trie feeds the manager back unchanged.
    let paths = manager.register("paths", 3);
    let [trie] = results.outputs.try_into().expect("one output");
    let stored = manager.add_trie(paths, Revision(1), ColumnOrder::identity(3), trie);
    assert_eq!(stored, 5);
}

/// Registers a unary table of the integers below `rows`.
fn large_table(manager: &mut TableManager, rows: i64) -> TableId {
    let table = manager.register("data", 1);
    let mut buffer = RowBuffer::new(1);
    for value in 0..rows {
        buffer.push_row(&[StorageValue::integer(value)]);
    }
    manager.add_rows(table, Revision(1), ColumnOrder::identity(1), buffer);
    table
}

#[test]
fn bindings_over_deeper_layers_stay_filters() {
    // a is the same term as c * 1, but c lies deeper than a,
    // so the layout checks the condition instead of looking a up.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[2], &[3], &[9]]]);
    let next = add_table(&mut manager, 2, &[&[&[10, 2], &[20, 3], &[30, 5]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b, c] = plan.markers();
    let left = plan.load(data, all(), vec![a]);
    let right = plan.load(next, all(), vec![b, c]);
    let joined = plan.join(vec![left, right], vec![a, b, c]);
    let bound = plan.filter(
        joined,
        condition(
            vec![a, c],
            vec![
                OpCode::Input(0),
                OpCode::Input(1),
                OpCode::Constant(0),
                OpCode::Multiply,
                OpCode::Same,
            ],
            vec![DataValue::integer(1)],
        ),
    );
    plan.output(bound);

    let layout = plan.layout();
    let member = the_member(&layout);
    assert_eq!(
        (member.filters.len(), member.bound_layers.len()),
        (1, 0),
        "{layout}"
    );
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![2, 10, 2], vec![3, 20, 3]]
    );
}

#[test]
fn partitioned_joins_match_the_sequential_row_sequence() {
    // Above the parallel threshold, the join runs in cells
    // and must deliver the same sorted, distinct rows the sequential run would.
    let mut manager = TableManager::new();
    let table = large_table(&mut manager, CELL_ROWS);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x] = plan.markers();
    let load = plan.load(table, all(), vec![x]);
    let join = joined(&mut plan, vec![load], vec![x], 1);
    plan.output(join);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    let rows = int_rows(&results.outputs[0]);
    assert!(rows.iter().map(|row| row[0]).eq(0..CELL_ROWS));
}

#[test]
fn partitioned_computed_columns_extend_every_row() {
    // A row stage in cells: each cell evaluates the programs on its own rows,
    // and the results are encoded once every cell is done.
    let mut manager = TableManager::new();
    let table = large_table(&mut manager, CELL_ROWS);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, next] = plan.markers();
    let load = plan.load(table, all(), vec![x]);
    let join = plan.join(vec![load], vec![x]);
    let successors = plan.compute(
        join,
        computation(
            vec![x],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            next,
        ),
    );
    plan.output(successors);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    let rows = int_rows(&results.outputs[0]);
    assert!(rows.iter().map(|row| row[0]).eq(0..CELL_ROWS));
    assert!(rows.iter().all(|row| row[1] == row[0] + 1));
}

#[test]
fn partitioned_projections_merge_across_cells() {
    // (x, x mod 10) projected to its second column: x lies above the residue and stays a layer,
    // so a sink projects the rows of each cell, and the build drops the repeats across cells.
    let mut manager = TableManager::new();
    let table = manager.register("data", 2);
    let rows: Vec<[i64; 2]> = (0..CELL_ROWS).map(|value| [value, value % 10]).collect();
    manager.add_rows(
        table,
        Revision(1),
        ColumnOrder::identity(2),
        int_buffer(2, &rows),
    );
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, residue] = plan.markers();
    let load = plan.load(table, all(), vec![x, residue]);
    let join = plan.join(vec![load], vec![x, residue]);
    let residues = plan.project(join, vec![residue]);
    plan.output(residues);

    let layout = plan.layout();
    let sink = matches!(the_member(&layout).delivery, Delivery::Sink(_));
    assert!(sink, "{layout}");
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    let expected: Vec<Vec<i64>> = (0..10).map(|residue| vec![residue]).collect();
    assert_eq!(int_rows(&results.outputs[0]), expected);
}

#[test]
fn partitioned_nullary_joins_emit_one_row() {
    // An existence check over a table large enough for cells:
    // exactly one empty row, however many cells the machine runs.
    let mut manager = TableManager::new();
    let table = large_table(&mut manager, 70_000);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x] = plan.markers();
    let load = plan.load(table, all(), vec![x]);
    let join = joined(&mut plan, vec![load], vec![x], 0);
    plan.output(join);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(results.outputs[0].row_count(), 1);
    assert_eq!(results.outputs[0].arity(), 0);
}

#[test]
fn joins_distribute_over_blocks() {
    // The chain join over a table split into three subtables (with a
    // repeated row) must match the join over the same rows stored as
    // one block.
    let blocks: &[&[&[i64]]] = &[&[&[1, 2], &[2, 3]], &[&[3, 1], &[1, 2]], &[&[2, 2]]];
    let merged: &[&[i64]] = &[&[1, 2], &[2, 3], &[3, 1], &[2, 2]];

    let mut results = Vec::new();
    for storage in [blocks, &[merged]] {
        let mut manager = TableManager::new();
        let edge = add_table(&mut manager, 2, storage);
        let mut dictionary = Dictionary::new();

        let mut plan = OperationPlan::new();
        let [x, y, z] = plan.markers();
        let first = plan.load(edge, all(), vec![x, y]);
        let second = plan.load(edge, all(), vec![y, z]);
        let chain = joined(&mut plan, vec![first, second], vec![x, y, z], 3);
        plan.output(chain);

        let outcome = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        results.push(int_rows(&outcome.outputs[0]));
    }
    assert_eq!(results[0], results[1]);
    assert!(!results[0].is_empty());
}

#[test]
fn unions_assemble_seminaive_variants() {
    // The semi-naive shape: delta ⋈ full united with old ⋈ delta
    // equals full ⋈ full minus old ⋈ old — all from one stored
    // table, split at the old/delta boundary.
    let old: &[&[i64]] = &[&[1, 2], &[2, 3]];
    let delta: &[&[i64]] = &[&[3, 1], &[2, 2]];
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[old, delta]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let delta_left = plan.load(edge, Revision(2)..Revision(3), vec![x, y]);
    let old_left = plan.load(edge, Revision(1)..Revision(2), vec![x, y]);
    let old_right = plan.load(edge, Revision(1)..Revision(2), vec![y, z]);
    let delta_right = plan.load(edge, Revision(2)..Revision(3), vec![y, z]);
    let full_right = plan.union(vec![old_right, delta_right]);

    let new_via_left = joined(&mut plan, vec![delta_left, full_right], vec![x, y, z], 3);
    let new_via_right = joined(&mut plan, vec![old_left, delta_right], vec![x, y, z], 3);
    let derived = plan.union(vec![new_via_left, new_via_right]);
    plan.output(derived);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![
            vec![1, 2, 2],
            vec![2, 2, 2],
            vec![2, 2, 3],
            vec![2, 3, 1],
            vec![3, 1, 2],
        ]
    );
}

#[test]
fn subtraction_covers_every_block() {
    // R(a, b) minus T(b), with T split into two subtables: a match
    // in either block drops the row.
    let mut manager = TableManager::new();
    let relation = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3], &[2, 2], &[2, 4]]]);
    let blocked = add_table(&mut manager, 1, &[&[&[2]], &[&[4]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b] = plan.markers();
    let rows = plan.load(relation, all(), vec![a, b]);
    let minus = plan.load(blocked, all(), vec![b]);
    let difference = plan.join(vec![rows], vec![a, b]);
    let difference = plan.subtract(difference, minus);
    plan.output(difference);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 3]]);
}

#[test]
fn projections_stay_sorted_and_deduplicate() {
    // Projecting edges to their targets: the input is read with the
    // kept marker leading, and duplicates collapse.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[3, 2], &[2, 7]], &[&[4, 7]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let edges = plan.load(edge, all(), vec![x, y]);
    let targets = plan.project(edges, vec![y]);
    plan.output(targets);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![2], vec![7]]);
}

#[test]
fn intermediates_materialize_every_consumed_order() {
    // One computed relation read three ways: canonically, reordered,
    // and through an alias whose translated order reuses the
    // reordered arrangement.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[2, 3], &[3, 4]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let edges = plan.load(edge, all(), vec![x, y]);
    let derived = joined(&mut plan, vec![edges], vec![x, y], 2);

    // The alias reads (x, y) as (y, z): composition with itself.
    let successors = plan.alias(derived, vec![y, z]);
    let composed = joined(&mut plan, vec![derived, successors], vec![x, y, z], 3);
    plan.output(composed);

    // A second consumer wants (y, x); a third reads the alias as
    // (z, y), translating to the same (y, x) arrangement.
    let reversed = joined(&mut plan, vec![derived], vec![y, x], 2);
    plan.output(reversed);
    let alias_reversed = joined(&mut plan, vec![successors], vec![z, y], 2);
    plan.output(alias_reversed);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![1, 2, 3], vec![2, 3, 4]]
    );
    assert_eq!(
        int_rows(&results.outputs[1]),
        vec![vec![2, 1], vec![3, 2], vec![4, 3]]
    );
    assert_eq!(int_rows(&results.outputs[1]), int_rows(&results.outputs[2]));
}

#[test]
fn aggregates_unite_blocks_before_grouping() {
    // The row (1, 4) recurs across subtables; the union collapses it
    // before counting.
    let mut manager = TableManager::new();
    let data = add_table(
        &mut manager,
        2,
        &[&[&[1, 2], &[1, 4]], &[&[1, 4], &[2, 10]]],
    );
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [key, value, count, sum] = plan.markers();
    let rows = plan.load(data, all(), vec![key, value]);
    let grouped = plan.aggregate(
        rows,
        vec![key],
        vec![
            AggregateColumnSpec::new(AggregateFunction::Count, count).per(vec![value]),
            AggregateColumnSpec::new(AggregateFunction::Sum(value), sum),
        ],
    );
    plan.output(grouped);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![1, 2, 6], vec![2, 1, 10]]
    );
}

#[test]
fn counts_tell_rows_apart_by_their_own_columns() {
    // Over (x, y) holding (1, 2) and (1, 3): one distinct x, two distinct rows,
    // and one distinct empty tuple. Without a group key, only the count over
    // every column may count the stored rows directly; the layout decides that.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3]]]);
    let mut dictionary = Dictionary::new();

    for (columns, expected, counts_rows) in [
        (vec![0], 1, false),
        (vec![0, 1], 2, true),
        (vec![], 1, false),
    ] {
        let mut plan = OperationPlan::new();
        let [x, y, count] = plan.markers();
        let rows = plan.load(data, all(), vec![x, y]);
        let per = columns.iter().map(|&column| [x, y][column]).collect();
        let spec = AggregateColumnSpec::new(AggregateFunction::Count, count).per(per);
        let counted = plan.aggregate(rows, Vec::new(), vec![spec]);
        plan.output(counted);

        let layout = plan.layout();
        let counting = layout.steps.iter().any(|step| {
            matches!(step, Step::Aggregate(aggregate) if matches!(aggregate.pass, AggregatePass::CountRows))
        });
        assert_eq!(counting, counts_rows, "{columns:?}: {layout}");

        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(
            int_rows(&results.outputs[0]),
            vec![vec![expected]],
            "{columns:?}"
        );
    }
}

#[test]
fn group_keys_need_not_lead_the_input() {
    // (a, b) grouped by b: per b, the distinct a, their sum, and the one empty tuple.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 5], &[2, 5], &[3, 6]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b, count, sum, groups] = plan.markers();
    let rows = plan.load(data, all(), vec![a, b]);
    let grouped = plan.aggregate(
        rows,
        vec![b],
        vec![
            AggregateColumnSpec::new(AggregateFunction::Count, count).per(vec![a]),
            AggregateColumnSpec::new(AggregateFunction::Sum(a), sum),
            AggregateColumnSpec::new(AggregateFunction::Count, groups),
        ],
    );
    plan.output(grouped);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![5, 2, 3, 1], vec![6, 1, 3, 1]]
    );
}

#[test]
fn expressions_compile_to_specs() {
    // next = a + 1, kept where next is b; a column read twice is one input.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3], &[2, 3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b, next] = plan.markers();
    let plus_one = Expression::apply(
        OpCode::Add,
        [
            Expression::column(a),
            Expression::constant(DataValue::integer(1)),
        ],
    );
    let computed = ComputedColumnSpec::new(&plus_one, next).expect("few operands");
    assert_eq!(computed.inputs, vec![a]);
    let matches = Expression::apply(
        OpCode::Same,
        [Expression::column(b), Expression::column(next)],
    );
    let matched = FilterSpec::new(&matches).expect("few operands");
    assert_eq!(matched.inputs, vec![b, next]);
    let reflexive = Expression::apply(OpCode::Same, [Expression::column(a), Expression::column(a)]);
    assert_eq!(
        FilterSpec::new(&reflexive).expect("few operands").inputs,
        vec![a]
    );

    let rows = plan.load(data, all(), vec![a, b]);
    let extended = plan.compute(rows, computed);
    let kept = plan.filter(extended, matched);
    plan.output(kept);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![1, 2, 2], vec![2, 3, 3]]
    );
}

#[test]
fn expressions_beyond_the_operand_limit_are_refused() {
    /// The largest of `count` distinct constants, as maxima over groups of them.
    fn largest(count: usize) -> Expression {
        let fold = |operands: Vec<Expression>| {
            let count = u16::try_from(operands.len()).expect("a group fits an instruction");
            Expression::apply(OpCode::Maximum(count), operands)
        };
        let constants: Vec<Expression> = (0i64..)
            .take(count)
            .map(|value| Expression::constant(DataValue::integer(value)))
            .collect();
        let groups = constants
            .chunks(usize::from(u16::MAX))
            .map(|group| fold(group.to_vec()))
            .collect();
        fold(groups)
    }

    let mut plan = OperationPlan::new();
    let [result] = plan.markers();
    let limit = StackProgram::OPERAND_LIMIT;
    assert!(ComputedColumnSpec::new(&largest(limit), result).is_ok());

    let refused = FilterSpec::new(&largest(limit + 1)).expect_err("one constant too many");
    assert_eq!(refused, PlanError::ExpressionTooLarge { limit });
    assert_eq!(refused.diagnostic().code(), codes::EXPRESSION_TOO_LARGE);
}

#[test]
fn filters_read_layers_by_marker() {
    // The chain join keeping rows with y != 3.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3], &[2, 3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let first = plan.load(edge, all(), vec![x, y]);
    let second = plan.load(edge, all(), vec![y, z]);
    let not_three = condition(
        vec![y],
        vec![
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Equal,
            OpCode::Not,
        ],
        vec![DataValue::integer(3)],
    );
    let chain = plan.join(vec![first, second], vec![x, y, z]);
    let chain = plan.filter(chain, not_three);
    plan.output(chain);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 2, 3]]);
}

#[test]
fn evaluation_errors_are_delivered() {
    // Comparing integers against a string errors once per row; the
    // rows vanish but the counts arrive with the results.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2], &[3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let rows = plan.load(data, all(), vec![a]);
    let broken = condition(
        vec![a],
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Less],
        vec![DataValue::string("x")],
    );
    let filtered = plan.join(vec![rows], vec![a]);
    let filtered = plan.filter(filtered, broken);
    plan.output(filtered);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.outputs[0].is_empty());
    assert_eq!(
        (
            results.errors.count(EvaluationError::Incomparable),
            results.errors.total()
        ),
        (3, 3)
    );
}

#[test]
fn existence_layers_keep_results_distinct() {
    // Sources with several matching targets appear once.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 5]]]);
    let target = add_table(&mut manager, 2, &[&[&[2, 7], &[2, 8], &[2, 9]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let first = plan.load(edge, all(), vec![x, y]);
    let second = plan.load(target, all(), vec![y, z]);
    let pairs = joined(&mut plan, vec![first, second], vec![x, y, z], 2);
    plan.output(pairs);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 2]]);
}

#[test]
fn nullary_outputs_answer_existence() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1]], &[&[2]]]);
    let empty = manager.register("empty", 1);
    let mut dictionary = Dictionary::new();

    // Existence across two blocks collapses to one empty row.
    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let rows = plan.load(data, all(), vec![a]);
    let exists = joined(&mut plan, vec![rows], vec![a], 0);
    plan.output(exists);
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(results.outputs[0].arity(), 0);
    assert_eq!(results.outputs[0].row_count(), 1);

    // Over an empty table, not even the empty row appears.
    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let rows = plan.load(empty, all(), vec![a]);
    let exists = joined(&mut plan, vec![rows], vec![a], 0);
    plan.output(exists);
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.outputs[0].is_empty());
}

/// Returns a computed column specification built from the parts of its program.
fn computation(
    inputs: Vec<Marker>,
    operations: Vec<OpCode>,
    constants: Vec<DataValue>,
    result: Marker,
) -> ComputedColumnSpec {
    let arity = inputs.len();
    let program = StackProgram::new(operations, constants, arity);
    ComputedColumnSpec::from_program(inputs, program, result)
}

#[test]
fn computed_columns_extend_rows() {
    // The chain join computes s = x + z (recomputed per row) and
    // d = x * 10 (reused while x is unchanged); a projection then
    // consumes the results in a reordered layout.
    let mut manager = TableManager::new();
    let left = add_table(&mut manager, 2, &[&[&[1, 2], &[2, 3]]]);
    let right = add_table(&mut manager, 2, &[&[&[2, 7], &[3, 8], &[3, 9]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z, s, d] = plan.markers();
    let first = plan.load(left, all(), vec![x, y]);
    let second = plan.load(right, all(), vec![y, z]);
    let chain = plan.join(vec![first, second], vec![x, y, z]);
    let chain = plan.compute(
        chain,
        computation(
            vec![x, z],
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Add],
            Vec::new(),
            s,
        ),
    );
    let chain = plan.compute(
        chain,
        computation(
            vec![x],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Multiply],
            vec![DataValue::integer(10)],
            d,
        ),
    );
    plan.output(chain);
    let swapped = plan.project(chain, vec![s, x]);
    plan.output(swapped);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![
            vec![1, 2, 7, 8, 10],
            vec![2, 3, 8, 10, 20],
            vec![2, 3, 9, 11, 20],
        ]
    );
    assert_eq!(
        int_rows(&results.outputs[1]),
        vec![vec![8, 1], vec![10, 2], vec![11, 2]]
    );
}

#[test]
fn computed_strings_intern_after_the_join() {
    let mut dictionary = Dictionary::new();
    let mut rows = RowBuffer::new(2);
    for (key, name) in [(1, "alice"), (2, "bob")] {
        let name = dictionary
            .encode(DataValue::string(name).as_ref())
            .expect("strings are representable");
        rows.push_row(&[StorageValue::integer(key), name]);
    }
    let mut manager = TableManager::new();
    let people = manager.register("people", 2);
    manager.add_rows(people, Revision(1), ColumnOrder::identity(2), rows);

    let mut plan = OperationPlan::new();
    let [key, name, upper] = plan.markers();
    let named = plan.load(people, all(), vec![key, name]);
    let shouting = plan.join(vec![named], vec![key, name]);
    let shouting = plan.compute(
        shouting,
        computation(
            vec![name],
            vec![OpCode::Input(0), OpCode::UpperCase],
            Vec::new(),
            upper,
        ),
    );
    plan.output(shouting);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    let mut shouted: Vec<DataValue> = Vec::new();
    results.outputs[0].for_each_row(|row| {
        shouted.push(dictionary.decode(row[2]).expect("value decodes").to_owned());
    });
    assert_eq!(
        shouted,
        vec![DataValue::string("ALICE"), DataValue::string("BOB")]
    );
}

#[test]
fn computed_errors_drop_rows_and_count() {
    // Adding one to a string errors; the row vanishes, the count
    // arrives, and the clean row survives.
    let mut dictionary = Dictionary::new();
    let broken = dictionary
        .encode(DataValue::string("oops").as_ref())
        .expect("strings are representable");
    let mut rows = RowBuffer::new(1);
    rows.push_row(&[broken]);
    rows.push_row(&[StorageValue::integer(5)]);
    let mut manager = TableManager::new();
    let data = manager.register("data", 1);
    manager.add_rows(data, Revision(1), ColumnOrder::identity(1), rows);

    let mut plan = OperationPlan::new();
    let [a, next] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let incremented = plan.join(vec![loaded], vec![a]);
    let incremented = plan.compute(
        incremented,
        computation(
            vec![a],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            next,
        ),
    );
    plan.output(incremented);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![5, 6]]);
    assert_eq!(
        (
            results.errors.count(EvaluationError::TypeMismatch),
            results.errors.total()
        ),
        (1, 1)
    );
}

#[test]
fn computed_constants_distribute_over_blocks() {
    // A constant column over a two-block table: rows recurring
    // across the combinations still collapse.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]], &[&[2], &[3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, tag] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let tagged = plan.join(vec![loaded], vec![a]);
    let tagged = plan.compute(
        tagged,
        computation(
            Vec::new(),
            vec![OpCode::Constant(0)],
            vec![DataValue::integer(42)],
            tag,
        ),
    );
    plan.output(tagged);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![1, 42], vec![2, 42], vec![3, 42]]
    );
}

#[test]
fn computed_columns_chain_and_post_filters_prune() {
    // ?s = ?a + 1, ?d = ?s * ?s (a computed input), keep ?d > 10 —
    // the assignment-and-filter shape rule bodies compile into.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2], &[3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, s, d] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let joined = plan.join(vec![loaded], vec![a]);
    let joined = plan.compute(
        joined,
        computation(
            vec![a],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            s,
        ),
    );
    let joined = plan.compute(
        joined,
        computation(
            vec![s],
            vec![OpCode::Input(0), OpCode::Input(0), OpCode::Multiply],
            Vec::new(),
            d,
        ),
    );
    let joined = plan.filter(
        joined,
        condition(
            vec![d],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(10)],
        ),
    );
    plan.output(joined);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    assert_eq!(
        int_rows(&results.outputs[0]),
        vec![vec![3, 4, 16]],
        "only 3 squares past 10 after the increment"
    );
}

#[test]
fn computed_integers_fall_back_where_they_outgrow_i64() {
    // s = a + 1 runs the integer form, except where it overflows and
    // the interpreter widens; d = s - 1 reads the wide result
    // generically and lands back within `i64`; p = a > 0 is a
    // boolean the integer form computes.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[-3], &[i64::MAX]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, s, d, p] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let joined = plan.join(vec![loaded], vec![a]);
    let joined = plan.compute(
        joined,
        computation(
            vec![a],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            s,
        ),
    );
    let joined = plan.compute(
        joined,
        computation(
            vec![s],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Subtract],
            vec![DataValue::integer(1)],
            d,
        ),
    );
    let joined = plan.compute(
        joined,
        computation(
            vec![a],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(0)],
            p,
        ),
    );
    plan.output(joined);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.errors.is_empty());
    let mut rows: Vec<Vec<DataValue>> = Vec::new();
    results.outputs[0].for_each_row(|row| {
        rows.push(
            row.iter()
                .map(|&value| dictionary.decode(value).expect("value decodes").to_owned())
                .collect(),
        );
    });
    let int = DataValue::integer;
    let wide = DataValue::decimal(i128::from(i64::MAX) + 1, 0).expect("bounded");
    assert_eq!(
        rows,
        vec![
            vec![int(-3), int(-2), int(-3), DataValue::boolean(false)],
            vec![int(i64::MAX), wide, int(i64::MAX), DataValue::boolean(true)],
        ]
    );
}

#[test]
fn post_filter_errors_drop_rows_and_count() {
    // A post-filter dividing by a computed zero: the erroring row
    // drops and counts, the clean row survives.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[6, 0], &[6, 2]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let loaded = plan.load(data, all(), vec![x, y]);
    let joined = plan.join(vec![loaded], vec![x, y]);
    let joined = plan.filter(
        joined,
        condition(
            vec![x, y],
            vec![
                OpCode::Input(0),
                OpCode::Input(1),
                OpCode::Divide,
                OpCode::Constant(0),
                OpCode::Equal,
            ],
            vec![DataValue::integer(3)],
        ),
    );
    plan.output(joined);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![6, 2]]);
    assert_eq!(
        (
            results.errors.count(EvaluationError::DivisionByZero),
            results.errors.total()
        ),
        (1, 1)
    );
}

#[test]
fn nondeterministic_columns_evaluate_per_row() {
    // A RAND column over three rows draws three values — the
    // per-prefix memoization must not freeze it.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2], &[3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, r] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let joined = plan.join(vec![loaded], vec![a]);
    let joined = plan.compute(
        joined,
        computation(Vec::new(), vec![OpCode::Random], Vec::new(), r),
    );
    plan.output(joined);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    let trie = &results.outputs[0];
    assert_eq!(trie.row_count(), 3);
    let mut draws = Vec::new();
    trie.for_each_row(|row| draws.push(row[1]));
    draws.sort_unstable();
    draws.dedup();
    assert_eq!(draws.len(), 3, "every row draws its own value");
}

#[test]
fn computed_columns_are_validated() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2]]]);

    let mut plan = OperationPlan::new();
    let [x, y, r] = plan.markers();
    let rows = plan.load(data, all(), vec![x, y]);
    let identity = |input: Marker, result: Marker| {
        computation(vec![input], vec![OpCode::Input(0)], Vec::new(), result)
    };

    // A column projected away cannot feed a computation.
    let sources = plan.project(rows, vec![x]);
    assert_panics("input's markers", || plan.compute(sources, identity(y, r)));
    // The program arity must match the inputs.
    let unary = StackProgram::new(vec![OpCode::Constant(0)], vec![DataValue::integer(1)], 1);
    assert_panics("arity", || {
        ComputedColumnSpec::from_program(Vec::new(), unary.clone(), r)
    });
    assert_panics("arity", || FilterSpec::from_program(Vec::new(), unary));
    // Results need fresh markers, not the input's.
    assert_panics("fresh marker", || plan.compute(rows, identity(x, y)));
    let extended = plan.compute(rows, identity(x, r));
    assert_panics("fresh marker", || plan.compute(extended, identity(y, r)));
}

#[test]
fn shared_existential_markers_stay_compared() {
    // R and S both carry b, which nothing reads: b stays a layer,
    // so the join still requires the two rows to agree on it.
    let mut manager = TableManager::new();
    let left = add_table(&mut manager, 2, &[&[&[1, 2], &[4, 5]]]);
    let right = add_table(&mut manager, 2, &[&[&[1, 3], &[4, 5]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b] = plan.markers();
    let first = plan.load(left, all(), vec![a, b]);
    let second = plan.load(right, all(), vec![a, b]);
    let matched = joined(&mut plan, vec![first, second], vec![a, b], 1);
    plan.output(matched);

    let layout = plan.layout();
    let member = the_member(&layout);
    assert_eq!((member.layers.len(), member.emitted), (2, 1), "{layout}");
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![4]]);
}

#[test]
fn private_markers_leave_the_descent() {
    // A marker of only one input that nothing reads is no layer:
    // the join never enumerates it, and results are distinct without it.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3], &[2, 5]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let edges = plan.load(edge, all(), vec![x, y]);
    let sources = joined(&mut plan, vec![edges], vec![x, y], 1);
    plan.output(sources);

    let layout = plan.layout();
    let member = the_member(&layout);
    assert_eq!((member.layers.len(), member.emitted), (1, 1), "{layout}");
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1], vec![2]]);
}

#[test]
fn leaving_markers_spare_the_sink() {
    // R(x, z) ⋈ S(x, y) in the order x, z, y, projected to (x, y).
    // z is R's deepest marker in the order and nothing reads it, so it leaves the descent,
    // and the rows over x and y arrive distinct without a sink.
    // In the order z, x, y, z lies above x and stays a layer, so the projection merges rows.
    let mut manager = TableManager::new();
    let r = add_table(&mut manager, 2, &[&[&[1, 10], &[1, 11], &[2, 12]]]);
    let s = add_table(&mut manager, 2, &[&[&[1, 5], &[2, 6], &[3, 7]]]);
    let mut dictionary = Dictionary::new();

    for z_first in [false, true] {
        let mut plan = OperationPlan::new();
        let [x, z, y] = plan.markers();
        let left = plan.load(r, all(), vec![x, z]);
        let right = plan.load(s, all(), vec![x, y]);
        let order = if z_first {
            vec![z, x, y]
        } else {
            vec![x, z, y]
        };
        let join = plan.join(vec![left, right], order);
        let pairs = plan.project(join, vec![x, y]);
        plan.output(pairs);

        let layout = plan.layout();
        let member = the_member(&layout);
        let expected = if z_first { (3, 3) } else { (2, 2) };
        assert_eq!((member.layers.len(), member.emitted), expected, "{layout}");
        let sink = matches!(member.delivery, Delivery::Sink(_));
        assert_eq!(sink, z_first, "{layout}");
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 5], vec![2, 6]]);
    }
}

#[test]
fn every_input_keeps_a_layer() {
    // R(x) ⋈ S(z) projected to x: nothing reads z, but S keeps it as a layer,
    // since only a layer finds out whether S holds a row at all.
    let mut manager = TableManager::new();
    let r = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let full = add_table(&mut manager, 1, &[&[&[7]]]);
    let empty = manager.register("empty", 1);
    let mut dictionary = Dictionary::new();

    for (table, expected) in [(full, vec![vec![1], vec![2]]), (empty, Vec::new())] {
        let mut plan = OperationPlan::new();
        let [x, z] = plan.markers();
        let left = plan.load(r, all(), vec![x]);
        let right = plan.load(table, all(), vec![z]);
        let guarded = joined(&mut plan, vec![left, right], vec![x, z], 1);
        plan.output(guarded);

        let layout = plan.layout();
        let member = the_member(&layout);
        assert_eq!((member.layers.len(), member.emitted), (2, 1), "{layout}");
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected);
    }
}

#[test]
fn dead_nodes_are_skipped() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b] = plan.markers();
    let live = plan.load(data, all(), vec![a]);
    // The dead branch is never executed — its load would fail on the
    // table's arity.
    let dead = plan.load(data, all(), vec![a, b]);
    joined(&mut plan, vec![dead], vec![a, b], 2);
    plan.output(live);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1]]);
}

#[test]
fn construction_rejects_malformed_nodes() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2]]]);
    let unary = add_table(&mut manager, 1, &[&[&[7]]]);

    // A failed construction adds no node, so the cases do not interfere.
    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let rows = plan.load(data, all(), vec![x, y]);
    let other = plan.load(data, all(), vec![x, z]);
    let apart = plan.load(unary, all(), vec![z]);

    // Duplicate and foreign markers.
    assert_panics("distinct", || plan.load(data, all(), vec![x, x]));
    assert_panics("allocated", || plan.load(data, all(), vec![x, Marker(99)]));
    // Unknown input nodes.
    assert_panics("unknown node", || plan.project(NodeId(99), vec![x]));
    // An alias must cover the input arity.
    assert_panics("rename every column", || plan.alias(rows, vec![z]));
    // Unions need inputs over one marker set.
    assert_panics("at least one input", || plan.union(Vec::new()));
    assert_panics("one marker set", || plan.union(vec![rows, other]));
    // Joins: no input, orders missing an input's marker or listing a foreign one.
    assert_panics("at least one input", || plan.join(Vec::new(), Vec::new()));
    assert_panics("every marker", || plan.join(vec![rows], Vec::new()));
    assert_panics("every marker", || plan.join(vec![rows], vec![x]));
    assert_panics("only list markers", || plan.join(vec![rows], vec![x, y, z]));
    assert_panics("every marker", || plan.join(vec![rows, apart], vec![x, y]));
    // A subtracted relation sharing no marker, and a filter reading a foreign one.
    assert_panics("share a marker", || plan.subtract(rows, apart));
    assert_panics("input's markers", || {
        plan.filter(rows, condition(vec![z], vec![OpCode::Input(0)], Vec::new()))
    });
    // Projections cannot invent markers.
    assert_panics("keep the input's markers", || plan.project(rows, vec![z]));
    // Aggregations: foreign or repeated group markers, no aggregates,
    // foreign inputs, colliding results.
    let count = AggregateColumnSpec::new(AggregateFunction::Count, z).per(vec![y]);
    assert_panics("group key", || {
        plan.aggregate(rows, vec![z], vec![count.clone()])
    });
    assert_panics("distinct", || {
        plan.aggregate(rows, vec![x, x], vec![count.clone()])
    });
    assert_panics("at least one aggregate", || {
        plan.aggregate(rows, vec![x], Vec::new())
    });
    let foreign = AggregateColumnSpec::new(AggregateFunction::Sum(z), z);
    assert_panics("read only the input's markers", || {
        plan.aggregate(rows, vec![x], vec![foreign])
    });
    let colliding = AggregateColumnSpec::new(AggregateFunction::Count, x).per(vec![y]);
    assert_panics("fresh marker", || {
        plan.aggregate(rows, vec![x], vec![colliding])
    });
    // A result marker must be fresh, not a dropped input marker.
    let reused = AggregateColumnSpec::new(AggregateFunction::Count, y).per(vec![y]);
    assert_panics("fresh marker", || {
        plan.aggregate(rows, vec![x], vec![reused])
    });
    // Expressions: an operand count that does not fit the instruction,
    // and an input instruction applied as an operation.
    let lonely = Expression::apply(OpCode::Add, [Expression::column(x)]);
    assert_panics("one operand per value", || FilterSpec::new(&lonely));
    let raw = Expression::apply(OpCode::Input(0), []);
    assert_panics("as operands", || ComputedColumnSpec::new(&raw, z));
    // Outputs exist and are declared once.
    assert_panics("unknown node", || plan.output(NodeId(99)));
    plan.output(rows);
    assert_panics("only once", || plan.output(rows));
}

#[test]
fn execution_validates_loads() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2]]]);
    let mut dictionary = Dictionary::new();

    // A load must cover the table's columns.
    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let rows = plan.load(data, all(), vec![a]);
    plan.output(rows);
    assert_panics("cover the table's columns", || {
        plan.execute(&mut manager, &mut dictionary, EvaluationContext::now())
    });

    // A load range must align with the table's blocks.
    let split = add_table(&mut manager, 1, &[&[&[1]], &[&[2]]]);
    manager.consolidate(split, Revision(1)..Revision(3));
    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let rows = plan.load(split, Revision(1)..Revision(2), vec![a]);
    plan.output(rows);
    assert_panics("align", || {
        plan.execute(&mut manager, &mut dictionary, EvaluationContext::now())
    });
}

#[test]
fn layouts_are_reused_across_load_ranges() {
    // The plan is built and laid out once, and each execution reads other revisions
    // through the same layout and arena. The join reads the first load
    // in a column order the table does not store yet.
    let mut manager = TableManager::new();
    let edge = add_table(
        &mut manager,
        2,
        &[&[&[1, 2], &[2, 3]], &[&[3, 4]], &[&[4, 5], &[2, 6]]],
    );
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let first = plan.load(edge, all(), vec![x, y]);
    let second = plan.load(edge, all(), vec![y, z]);
    let paths = plan.join(vec![first, second], vec![y, x, z]);
    plan.output(paths);

    let layout = plan.layout();
    let arena = JoinArena::new();
    let cases = [
        (
            Revision(1)..Revision(2),
            vec![vec![2, 1, 3], vec![2, 1, 6], vec![3, 2, 4]],
        ),
        (Revision(2)..Revision(3), vec![vec![4, 3, 5]]),
        (Revision(3)..Revision(4), Vec::new()),
        (
            Revision(1)..Revision(4),
            vec![vec![2, 1, 3], vec![2, 1, 6], vec![3, 2, 4], vec![4, 3, 5]],
        ),
    ];
    for (range, expected) in cases {
        plan.set_load_range(first, range.clone());
        let results = plan.execute_with_layout(
            &layout,
            &mut manager,
            &mut dictionary,
            EvaluationContext::now(),
            &arena,
        );
        assert_eq!(int_rows(&results.outputs[0]), expected, "{range:?}");
    }

    assert_panics("only load nodes", || plan.set_load_range(paths, all()));
}

#[test]
fn random_block_splits_match_consolidated_joins() {
    // The differential property of distribution: however the stored
    // rows are split into subtables, the chain join delivers the
    // rows of the consolidated join.
    let mut rng = StdRng::seed_from_u64(23);
    for round in 0..60 {
        let row_count = rng.gen_range(0..12);
        let rows: Vec<Vec<i64>> = (0..row_count)
            .map(|_| vec![rng.gen_range(0..4), rng.gen_range(0..4)])
            .collect();

        // Split the rows into up to three revision blocks.
        let mut blocks: Vec<Vec<Vec<i64>>> = (0..rng.gen_range(1..4)).map(|_| Vec::new()).collect();
        let block_count = blocks.len();
        for row in &rows {
            blocks[rng.gen_range(0..block_count)].push(row.clone());
        }

        let mut manager = TableManager::new();
        let edge = manager.register("edge", 2);
        for (index, block) in blocks.iter().enumerate() {
            let rows: Vec<&[i64]> = block.iter().map(Vec::as_slice).collect();
            manager.add_rows(
                edge,
                Revision(index as u64 + 1),
                ColumnOrder::identity(2),
                int_buffer(2, &rows),
            );
        }
        let mut dictionary = Dictionary::new();

        let mut plan = OperationPlan::new();
        let [x, y, z] = plan.markers();
        let first = plan.load(edge, all(), vec![x, y]);
        let second = plan.load(edge, all(), vec![y, z]);
        let chain = joined(&mut plan, vec![first, second], vec![x, y, z], 3);
        plan.output(chain);
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());

        let mut expected = BTreeSet::new();
        for left in &rows {
            for right in &rows {
                if left[1] == right[0] {
                    expected.insert(vec![left[0], left[1], right[1]]);
                }
            }
        }
        assert_eq!(
            int_rows(&results.outputs[0]),
            expected.into_iter().collect::<Vec<_>>(),
            "round {round}"
        );
    }
}

/// A relation of integer rows under markers, computed by brute force.
#[derive(Debug, Clone)]
struct Relation {
    /// The column markers.
    markers: Vec<Marker>,
    /// The rows, each once.
    rows: BTreeSet<Vec<i64>>,
}

impl Relation {
    /// Returns the position of a marker's column.
    fn column(&self, marker: Marker) -> usize {
        self.markers
            .iter()
            .position(|&candidate| candidate == marker)
            .expect("the marker is a column")
    }

    /// Returns the rows reordered to the given markers.
    fn in_order(&self, order: &[Marker]) -> Vec<Vec<i64>> {
        let columns: Vec<usize> = order.iter().map(|&marker| self.column(marker)).collect();
        let rows: BTreeSet<Vec<i64>> = self
            .rows
            .iter()
            .map(|row| columns.iter().map(|&column| row[column]).collect())
            .collect();
        rows.into_iter().collect()
    }
}

/// Returns the natural join of two relations by brute force.
fn natural_join(left: &Relation, right: &Relation) -> Relation {
    let mut markers = left.markers.clone();
    markers.extend(
        right
            .markers
            .iter()
            .copied()
            .filter(|marker| !left.markers.contains(marker)),
    );
    let mut rows = BTreeSet::new();
    for first in &left.rows {
        for second in &right.rows {
            let agrees = right.markers.iter().enumerate().all(|(column, marker)| {
                left.markers
                    .iter()
                    .position(|candidate| candidate == marker)
                    .is_none_or(|position| first[position] == second[column])
            });
            if agrees {
                let mut row = first.clone();
                for (column, marker) in right.markers.iter().enumerate() {
                    if !left.markers.contains(marker) {
                        row.push(second[column]);
                    }
                }
                rows.insert(row);
            }
        }
    }
    Relation { markers, rows }
}

/// Adds a random operator above a node, to the plan and to its brute-force relation,
/// and returns the new node.
fn random_operator(
    rng: &mut StdRng,
    plan: &mut OperationPlan,
    manager: &mut TableManager,
    node: NodeId,
    relation: &mut Relation,
) -> NodeId {
    let pick = |rng: &mut StdRng, relation: &Relation| {
        relation.markers[rng.gen_range(0..relation.markers.len())]
    };
    match rng.gen_range(0..5) {
        // A filter keeping the rows where a column differs from a constant.
        0 => {
            let marker = pick(rng, relation);
            let excluded = rng.gen_range(0..5);
            let column = relation.column(marker);
            relation.rows.retain(|row| row[column] != excluded);
            let spec = condition(
                vec![marker],
                vec![
                    OpCode::Input(0),
                    OpCode::Constant(0),
                    OpCode::Same,
                    OpCode::Not,
                ],
                vec![DataValue::integer(excluded)],
            );
            plan.filter(node, spec)
        }
        // A filter keeping the rows where one column is below another, read by the interpreter.
        1 => {
            let first = pick(rng, relation);
            let second = pick(rng, relation);
            let (left, right) = (relation.column(first), relation.column(second));
            relation.rows.retain(|row| row[left] <= row[right]);
            let spec = condition(
                vec![first, second],
                vec![OpCode::Input(0), OpCode::Input(1), OpCode::LessOrEqual],
                Vec::new(),
            );
            plan.filter(node, spec)
        }
        // A computed column: a column plus one, or a constant.
        2 => {
            let result = plan.marker();
            let spec = if rng.gen_bool(0.7) {
                let marker = pick(rng, relation);
                let column = relation.column(marker);
                relation.rows = relation
                    .rows
                    .iter()
                    .map(|row| {
                        let mut row = row.clone();
                        row.push(row[column] + 1);
                        row
                    })
                    .collect();
                computation(
                    vec![marker],
                    vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
                    vec![DataValue::integer(1)],
                    result,
                )
            } else {
                relation.rows = relation
                    .rows
                    .iter()
                    .map(|row| {
                        let mut row = row.clone();
                        row.push(3);
                        row
                    })
                    .collect();
                computation(
                    Vec::new(),
                    vec![OpCode::Constant(0)],
                    vec![DataValue::integer(3)],
                    result,
                )
            };
            relation.markers.push(result);
            plan.compute(node, spec)
        }
        // A subtraction of a stored relation over some columns and a private one.
        3 => {
            let mut shared: Vec<Marker> = Vec::new();
            for _ in 0..rng.gen_range(1..3) {
                let marker = pick(rng, relation);
                if !shared.contains(&marker) {
                    shared.push(marker);
                }
            }
            let private = rng.gen_bool(0.3);
            let arity = shared.len() + usize::from(private);
            let stored = random_rows(rng, arity);
            let stored_refs: Vec<&[i64]> = stored.iter().map(Vec::as_slice).collect();
            let table = add_table(manager, arity, &[&stored_refs]);
            let mut markers = shared.clone();
            if private {
                markers.push(plan.marker());
            }

            let columns: Vec<usize> = shared
                .iter()
                .map(|&marker| relation.column(marker))
                .collect();
            relation.rows.retain(|row| {
                !stored.iter().any(|stored_row| {
                    columns
                        .iter()
                        .enumerate()
                        .all(|(position, &column)| stored_row[position] == row[column])
                })
            });
            let load = plan.load(table, all(), markers);
            plan.subtract(node, load)
        }
        // A projection to some of the columns, in some order.
        _ => {
            let mut kept = relation.markers.clone();
            for index in (1..kept.len()).rev() {
                kept.swap(index, rng.gen_range(0..=index));
            }
            kept.truncate(rng.gen_range(1..=kept.len()));
            relation.rows = relation.in_order(&kept).into_iter().collect();
            relation.markers = kept.clone();
            plan.project(node, kept)
        }
    }
}

#[test]
fn random_chains_match_their_operators_one_by_one() {
    // The differential property of units: a chain of operators above a join,
    // or above a union of joins, delivers the same rows fused into one unit
    // as with every node an output, which makes each operator a unit of its own,
    // and both match brute force.
    // Where the chain projects a join's private marker away, the marker leaves the descent.
    let mut rng = StdRng::seed_from_u64(29);
    let mut fused_chains = 0;
    let mut pruned_descents = 0;
    for round in 0..300 {
        let mut manager = TableManager::new();
        let r_rows = random_rows(&mut rng, 2);
        let s_rows = random_rows(&mut rng, 2);
        let r_halves = r_rows.split_at(r_rows.len() / 2);
        let r_refs: [Vec<&[i64]>; 2] = [
            r_halves.0.iter().map(Vec::as_slice).collect(),
            r_halves.1.iter().map(Vec::as_slice).collect(),
        ];
        let r = add_table(&mut manager, 2, &[&r_refs[0], &r_refs[1]]);
        let s_refs: Vec<&[i64]> = s_rows.iter().map(Vec::as_slice).collect();
        let s = add_table(&mut manager, 2, &[&s_refs]);

        let mut plan = OperationPlan::new();
        let [x, y, z] = plan.markers();
        let mut layers = vec![x, y, z];
        for index in (1..layers.len()).rev() {
            layers.swap(index, rng.gen_range(0..=index));
        }
        let united = rng.gen_bool(0.5);
        let ranges = if united {
            vec![Revision(0)..Revision(2), Revision(2)..Revision(100)]
        } else {
            vec![all()]
        };
        let mut joins = Vec::new();
        for range in ranges {
            let r_load = plan.load(r, range, vec![x, y]);
            let s_load = plan.load(s, all(), vec![y, z]);
            joins.push(plan.join(vec![r_load, s_load], layers.clone()));
        }
        let mut node = if united { plan.union(joins) } else { joins[0] };

        let to_relation = |rows: &[Vec<i64>], markers: Vec<Marker>| Relation {
            markers,
            rows: rows.iter().cloned().collect(),
        };
        let joined = natural_join(
            &to_relation(&r_rows, vec![x, y]),
            &to_relation(&s_rows, vec![y, z]),
        );
        let mut relation = Relation {
            rows: joined.in_order(&layers).into_iter().collect(),
            markers: layers.clone(),
        };

        let operators = rng.gen_range(1..6);
        for _ in 0..operators {
            node = random_operator(&mut rng, &mut plan, &mut manager, node, &mut relation);
        }
        let expected = relation.in_order(plan.node_markers(node));

        plan.output(node);
        let layout = plan.layout();
        if layout.steps.len() < operators + 1 {
            fused_chains += 1;
        }
        if members(&layout)
            .iter()
            .any(|member| member.inputs.len() == 2 && (1..=2).contains(&member.layers.len()))
        {
            pruned_descents += 1;
        }
        let mut dictionary = Dictionary::new();
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected, "round {round}");

        // Every node an output: no operator runs inside another's unit.
        plan.outputs.clear();
        for index in 0..plan.nodes.len() {
            plan.output(NodeId::new(index));
        }
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(
            int_rows(&results.outputs[node.index()]),
            expected,
            "round {round}, apart"
        );
    }
    assert!(fused_chains > 100, "most chains fuse: {fused_chains}");
    assert!(
        pruned_descents > 20,
        "some joins leave a marker out of the descent: {pruned_descents}"
    );
}

#[test]
fn constants_are_looked_up() {
    // ground(k) with k the same term as 7, beside data(a), which shares no marker with it:
    // the layout looks k up under every a instead of enumerating ground,
    // and a difference from the constant stays a filter.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let ground = add_table(&mut manager, 1, &[&[&[5], &[7]]]);
    let mut dictionary = Dictionary::new();

    for (negated, shape, expected) in [
        (false, (0, 1), vec![vec![1, 7], vec![2, 7]]),
        (true, (1, 0), vec![vec![1, 5], vec![2, 5]]),
    ] {
        let mut plan = OperationPlan::new();
        let [a, k] = plan.markers();
        let left = plan.load(data, all(), vec![a]);
        let right = plan.load(ground, all(), vec![k]);
        let joined = plan.join(vec![left, right], vec![a, k]);
        let mut operations = vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same];
        if negated {
            operations.push(OpCode::Not);
        }
        let filtered = plan.filter(
            joined,
            condition(vec![k], operations, vec![DataValue::integer(7)]),
        );
        plan.output(filtered);

        let layout = plan.layout();
        let member = the_member(&layout);
        assert_eq!(
            (member.filters.len(), member.bound_layers.len()),
            shape,
            "{layout}"
        );
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected);
    }
}

#[test]
fn bindings_are_looked_up() {
    // next(b, c) with c the same term as a + 1, written as a filter:
    // the layout recognizes the binding and looks c up, with the rows of the plain filter.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2], &[3]]]);
    let next = add_table(&mut manager, 2, &[&[&[10, 2], &[20, 3], &[30, 5]]]);
    let mut dictionary = Dictionary::new();

    for reversed in [false, true] {
        let mut plan = OperationPlan::new();
        let [a, b, c] = plan.markers();
        let left = plan.load(data, all(), vec![a]);
        let right = plan.load(next, all(), vec![b, c]);
        let joined = plan.join(vec![left, right], vec![a, b, c]);
        // c = a + 1, or a + 1 = c.
        let (inputs, operations) = if reversed {
            (
                vec![a, c],
                vec![
                    OpCode::Input(0),
                    OpCode::Constant(0),
                    OpCode::Add,
                    OpCode::Input(1),
                    OpCode::Same,
                ],
            )
        } else {
            (
                vec![c, a],
                vec![
                    OpCode::Input(0),
                    OpCode::Input(1),
                    OpCode::Constant(0),
                    OpCode::Add,
                    OpCode::Same,
                ],
            )
        };
        let bound = plan.filter(
            joined,
            condition(inputs, operations, vec![DataValue::integer(1)]),
        );
        plan.output(bound);

        let layout = plan.layout();
        assert_eq!(the_member(&layout).bound_layers.len(), 1, "{layout}");
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(
            int_rows(&results.outputs[0]),
            vec![vec![1, 10, 2], vec![2, 20, 3]]
        );
    }
}

#[test]
fn shared_joins_emit_what_every_consumer_needs() {
    // A join read by two projections materializes once,
    // emitting the layers up to the deeper of the two,
    // and each projection reads the result.
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[2, 3], &[2, 4]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let first = plan.load(edge, all(), vec![x, y]);
    let second = plan.load(edge, all(), vec![y, z]);
    let paths = plan.join(vec![first, second], vec![x, y, z]);
    let sources = plan.project(paths, vec![x]);
    let middles = plan.project(paths, vec![y, x]);
    plan.output(sources);
    plan.output(middles);

    let layout = plan.layout();
    let produced = layout.materializations[paths.index()]
        .as_ref()
        .expect("the shared join materializes")
        .produced
        .clone();
    assert_eq!(*produced, [x, y], "the deepest needed layer is y");
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1]]);
    assert_eq!(int_rows(&results.outputs[1]), vec![vec![2, 1]]);
}

#[test]
fn subtractions_over_computed_columns_read_the_result() {
    // The head shape (x, x + 1) minus the stored rows:
    // the subtraction compares a computed column, so it runs over the computed rows' result.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2], &[3]]]);
    let stored = add_table(&mut manager, 2, &[&[&[2, 3]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, next] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let computed = plan.compute(
        loaded,
        computation(
            vec![a],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            next,
        ),
    );
    let existing = plan.load(stored, all(), vec![a, next]);
    let fresh = plan.subtract(computed, existing);
    plan.output(fresh);

    let layout = plan.layout();
    assert!(layout.materializations[computed.index()].is_some());
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 2], vec![3, 4]]);
}

#[test]
fn computed_columns_extend_the_empty_row() {
    // A relation without columns, read twice, holds the empty row where its input has a row;
    // a computed constant over it yields one row, and nothing over an empty input.
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let empty = manager.register("empty", 1);
    let mut dictionary = Dictionary::new();

    for (table, expected) in [(data, vec![vec![7]]), (empty, Vec::new())] {
        let mut plan = OperationPlan::new();
        let [a, seven] = plan.markers();
        let loaded = plan.load(table, all(), vec![a]);
        let exists = plan.project(loaded, Vec::new());
        let constant = plan.compute(
            exists,
            computation(
                Vec::new(),
                vec![OpCode::Constant(0)],
                vec![DataValue::integer(7)],
                seven,
            ),
        );
        plan.output(exists);
        plan.output(constant);

        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[1]), expected);
    }
}
