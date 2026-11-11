use super::*;

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::{
    function::{OpCode, StackProgram},
    management::ColumnOrder,
    plan::layout::{AggregatePass, Delivery, MemberLayout, Step},
    storage::{PayloadEncoding, StorageValue},
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

/// Builds a plan whose projection and subtraction can run inside the joins of a union,
/// and returns it with the subtraction node and the union.
///
/// Two joins R(x, z) ⋈ S(z, y), each over a different range of R's revisions, are united.
/// The union is projected to the columns `kept` lists,
/// and the rows of the stored table are subtracted from the projection.
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

/// A projection and a subtraction that run inside the joins of a union yield the rows
/// of the same plan with a second consumer of the union, which runs them apart,
/// and both match brute force.
#[test]
fn fused_subtract_projections_match_the_unfused_rows() {
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

        // With a second consumer of the union, the operators must not run inside its joins,
        // and the rows must be the same.
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

/// A join that reads a table in another column order has the table build that arrangement,
/// and the delivered trie can be stored as it is.
#[test]
fn joins_derive_stored_orders() {
    // Converging edges: rows (x, y, z) with edge(x, y) and edge(z, y).
    // The second load reads the table as (z, y), and the join reads it in the order (y, z),
    // which the table builds on demand.
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

    // The delivered trie can be added to the manager unchanged.
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

/// A condition that binds a layer to an expression over a deeper layer stays a filter.
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

/// A join past the parallel threshold runs in cells,
/// and delivers the sorted, distinct rows of the sequential join.
#[test]
fn partitioned_joins_match_the_sequential_row_sequence() {
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

/// A row stage in cells evaluates the programs on each cell's rows,
/// and every row gets its computed value once the cells are done.
#[test]
fn partitioned_computed_columns_extend_every_row() {
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

/// A projection that merges rows in cells yields each projected row once.
#[test]
fn partitioned_projections_merge_across_cells() {
    // (x, x mod 10) projected to its second column. x lies above the residue and stays a layer,
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

/// A join that emits no layer, over a table large enough for cells,
/// yields exactly one empty row, however many cells the machine runs.
#[test]
fn partitioned_nullary_joins_emit_one_row() {
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

/// A chain join over a table split into three subtables, with a repeated row,
/// yields the rows of the join over the same rows stored as one block.
#[test]
fn joins_read_every_block() {
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

/// One join reads both blocks of each of twenty loads of a table.
#[test]
fn a_join_reads_many_inputs_of_several_blocks_at_once() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2]], &[&[2, 1]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let loads = (0..20)
        .map(|_| plan.load(data, all(), vec![x, y]))
        .collect();
    let joined = plan.join(loads, vec![x, y]);
    plan.output(joined);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1, 2], vec![2, 1]]);
    assert_eq!((results.counts.joins, results.counts.blocks), (1, 40));
}

/// The union of delta ⋈ full and old ⋈ delta yields the rows of full ⋈ full
/// that old ⋈ old lacks, over one table split into old and new rows.
#[test]
fn unions_assemble_seminaive_variants() {
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

/// R(a, b) minus T(b), with T split into two subtables,
/// drops the rows that match either block.
#[test]
fn subtraction_covers_every_block() {
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

/// A projection of edges to their targets reads the input with the kept marker first,
/// and yields each target once.
#[test]
fn projections_stay_sorted_and_deduplicate() {
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

/// A materialized node builds a trie for each order it is read in,
/// and a read through an alias reuses the trie of the translated order.
#[test]
fn intermediates_materialize_every_consumed_order() {
    let mut manager = TableManager::new();
    let edge = add_table(&mut manager, 2, &[&[&[1, 2], &[2, 3], &[3, 4]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y, z] = plan.markers();
    let edges = plan.load(edge, all(), vec![x, y]);
    let derived = joined(&mut plan, vec![edges], vec![x, y], 2);

    // The alias reads (x, y) as (y, z), which composes the relation with itself.
    let successors = plan.alias(derived, vec![y, z]);
    let composed = joined(&mut plan, vec![derived, successors], vec![x, y, z], 3);
    plan.output(composed);

    // A second consumer reads (y, x), and a third reads the alias as (z, y),
    // which translates to the same order (y, x).
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

/// An aggregation reads a row that two subtables both contain only once.
#[test]
fn aggregates_unite_blocks_before_grouping() {
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

/// A count without a group key counts the distinct combinations of its `per` columns,
/// and the layout counts the stored rows directly only if those include every column.
#[test]
fn counts_tell_rows_apart_by_their_own_columns() {
    // The rows (1, 2) and (1, 3) of (x, y) have one distinct x,
    // two distinct (x, y), and one empty combination.
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
            matches!(step, Step::Aggregate(aggregate) if matches!(aggregate.pass, AggregatePass::CountRows { .. }))
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

/// Several counts of an input's rows each hold the count,
/// also over an input without columns, which has one row or none.
#[test]
fn several_counts_of_the_rows_agree() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 2, &[&[&[1, 2], &[1, 3]]]);
    let holds = add_table(&mut manager, 0, &[&[&[]]]);
    let empty = manager.register("empty", 0);
    let mut dictionary = Dictionary::new();

    let cases = [
        (data, 2, vec![vec![2, 2]]),
        (holds, 0, vec![vec![1, 1]]),
        (empty, 0, Vec::new()),
    ];
    for (table, arity, expected) in cases {
        let mut plan = OperationPlan::new();
        let [x, y, first, second] = plan.markers();
        let markers = [x, y][..arity].to_vec();
        let rows = plan.load(table, all(), markers.clone());
        let counts = [first, second].map(|result| {
            AggregateColumnSpec::new(AggregateFunction::Count, result).per(markers.clone())
        });
        let counted = plan.aggregate(rows, Vec::new(), counts.to_vec());
        plan.output(counted);

        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected, "arity {arity}");
    }
}

/// An aggregation may group by a column that is not the input's first.
#[test]
fn group_keys_need_not_lead_the_input() {
    // (a, b) grouped by b. Per b, the number of distinct a, their sum,
    // and the count without `per` columns, which is 1.
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

/// Expressions compile to a computed column and filters,
/// and a column that an expression reads twice is one input.
#[test]
fn expressions_compile_to_specs() {
    // next = a + 1, kept where next is b.
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

/// An expression with more constants than an operand can address is refused,
/// with the matching diagnostic.
#[test]
fn expressions_beyond_the_operand_limit_are_refused() {
    /// Returns the largest of `count` distinct constants, as the maximum of maxima of groups.
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

/// A filter reads the layer its marker names.
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

/// The evaluation errors of a filter are delivered with the results.
#[test]
fn evaluation_errors_are_delivered() {
    // Comparing an integer with a string fails once per row.
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

/// A join that does not emit its deepest layer yields each emitted row once.
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

/// A join that emits no layer yields one empty row if its input has rows, and none otherwise.
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

/// A joined relation without columns keeps the rows of the join if it holds the empty row,
/// and drops them otherwise.
/// A join of such relations alone holds the empty row only if each of them does.
#[test]
fn relations_without_columns_decide_a_join_whole() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let holds = add_table(&mut manager, 0, &[&[&[]]]);
    let empty = manager.register("empty", 0);
    let mut dictionary = Dictionary::new();

    for (flag, expected) in [(holds, vec![vec![1], vec![2]]), (empty, Vec::new())] {
        let mut plan = OperationPlan::new();
        let [a] = plan.markers();
        let flag = plan.load(flag, all(), Vec::new());
        let rows = plan.load(data, all(), vec![a]);
        let both = plan.join(vec![flag, rows], vec![a]);
        plan.output(both);
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected);
    }

    for (first, second, expected) in [(holds, holds, 1), (holds, empty, 0), (empty, holds, 0)] {
        let mut plan = OperationPlan::new();
        let first = plan.load(first, all(), Vec::new());
        let second = plan.load(second, all(), Vec::new());
        let both = plan.join(vec![first, second], Vec::new());
        plan.output(both);
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(results.outputs[0].row_count(), expected);
    }
}

/// A subtracted relation that shares no marker with the input drops every row if it has a row,
/// and none otherwise, with or without columns of its own.
#[test]
fn a_subtraction_sharing_no_marker_drops_every_row_or_none() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let holds = add_table(&mut manager, 0, &[&[&[]]]);
    let empty = manager.register("empty", 0);
    let other = add_table(&mut manager, 1, &[&[&[7]]]);
    let none = manager.register("none", 1);
    let mut dictionary = Dictionary::new();

    let cases = [
        (holds, 0, Vec::new()),
        (empty, 0, vec![vec![1], vec![2]]),
        (other, 1, Vec::new()),
        (none, 1, vec![vec![1], vec![2]]),
    ];
    for (relation, arity, expected) in cases {
        let mut plan = OperationPlan::new();
        let [a, b] = plan.markers();
        let rows = plan.load(data, all(), vec![a]);
        let subtracted = plan.load(relation, all(), vec![b; arity]);
        let rest = plan.subtract(rows, subtracted);
        plan.output(rest);
        let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
        assert_eq!(int_rows(&results.outputs[0]), expected, "arity {arity}");
    }
}

/// A materialized relation that a later unit subtracts without sharing a marker
/// is kept until that unit has read it.
#[test]
fn a_relation_subtracted_without_a_shared_marker_is_kept_until_read() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let source = add_table(&mut manager, 1, &[&[&[7], &[8]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, z] = plan.markers();
    let loaded = plan.load(source, all(), vec![z]);
    let seven = plan.filter(
        loaded,
        condition(
            vec![z],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same],
            vec![DataValue::integer(7)],
        ),
    );
    // The first unit reads the relation, and the second subtracts it after that.
    let rows = plan.load(data, all(), vec![a]);
    let first = plan.join(vec![seven, rows], vec![z, a]);
    let rows = plan.load(data, all(), vec![a]);
    let second = plan.subtract(rows, seven);
    plan.output(first);
    plan.output(second);

    let layout = plan.layout();
    assert!(layout.materializations[seven.index()].is_some(), "{layout}");
    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![7, 1], vec![7, 2]]);
    assert!(results.outputs[1].is_empty());
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

/// Computed columns extend each row, and a projection can read them in another order.
#[test]
fn computed_columns_extend_rows() {
    // The chain join computes s = x + z, evaluated on each row,
    // and d = x * 10, reused while x is unchanged.
    // A projection then reads the results in another order.
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

/// A computed string is added to the dictionary after the join.
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

/// A computed column that fails drops its row, and the error is counted.
#[test]
fn computed_errors_drop_rows_and_count() {
    // Adding one to a string fails.
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

/// A constant column over a table of two blocks extends a row that both blocks contain
/// only once.
#[test]
fn computed_constants_over_several_blocks() {
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

/// A computed column may read another,
/// and a filter on a computed column drops the rows it rejects.
#[test]
fn computed_columns_chain_and_post_filters_prune() {
    // s = a + 1, then d = s * s, which reads a computed column, then the filter d > 10.
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

/// A computed integer that overflows `i64` falls back to the generic interpreter,
/// which widens it, and the integer form computes booleans.
#[test]
fn computed_integers_fall_back_where_they_outgrow_i64() {
    // s = a + 1 runs the integer form,
    // except where it overflows and the interpreter widens the result.
    // d = s - 1 reads the wide result in the interpreter, and returns within `i64`.
    // p = a > 0 is a boolean that the integer form computes.
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

/// A filter that fails to evaluate drops its row, and the error is counted.
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

/// A nondeterministic column is evaluated on every row, not once per prefix.
#[test]
fn nondeterministic_columns_evaluate_per_row() {
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

/// Building a computed column panics if it reads a marker its input lacks,
/// if its program's arity does not match its inputs, or if its result marker is not fresh.
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

    // A column projected away cannot be an input of a computation.
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

/// A marker that two inputs share stays a layer, although nothing reads it,
/// so the join still requires their rows to agree on it.
#[test]
fn shared_existential_markers_stay_compared() {
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

/// A marker of only one input that nothing reads is not a layer,
/// so the join never enumerates it, and its rows are distinct without it.
#[test]
fn private_markers_leave_the_descent() {
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

/// A projection needs no sink if the marker it drops has left the descent.
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

/// Every input keeps a layer, even if nothing reads its markers,
/// so the join still checks that the input has a row.
#[test]
fn every_input_keeps_a_layer() {
    // R(x) ⋈ S(z) projected to x. Nothing reads z, but S keeps it as a layer,
    // since only a layer finds out whether S has a row at all.
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

/// A node that no output depends on is not executed.
#[test]
fn dead_nodes_are_skipped() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [a, b] = plan.markers();
    let live = plan.load(data, all(), vec![a]);
    // The dead branch is never executed, since its load would fail on the table's arity.
    let dead = plan.load(data, all(), vec![a, b]);
    joined(&mut plan, vec![dead], vec![a, b], 2);
    plan.output(live);

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert_eq!(int_rows(&results.outputs[0]), vec![vec![1]]);
}

/// Each node constructor panics on a node that breaks one of its requirements.
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
    // An alias must rename every column of its input.
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
    // A filter reading a foreign marker.
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

/// Executing a plan panics if a load does not cover the table's columns,
/// or if its range does not align with the table's blocks.
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

/// A plan laid out once yields the right rows for every range of its load,
/// through the same layout and arena.
#[test]
fn layouts_are_reused_across_load_ranges() {
    // The join reads the first load in a column order the table does not store yet.
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

/// However the stored rows are split into subtables,
/// a chain join delivers the rows of brute-force evaluation.
#[test]
fn random_block_splits_match_consolidated_joins() {
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
        // A filter keeping the rows where one column is at most another, read by the interpreter.
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
        // A subtraction of a stored relation over some columns, and possibly a column of its own.
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

/// A random chain of operators above a join, or above a union of joins,
/// with the rows of its top by brute force.
struct RandomChain {
    /// The tables that the chain reads.
    manager: TableManager,
    /// The plan of the chain, without an output.
    plan: OperationPlan,
    /// The top of the chain.
    node: NodeId,
    /// The number of operators above the join or the union.
    operators: usize,
    /// The rows of the top, with the columns in the order of its markers, in storage order.
    expected: Vec<Vec<i64>>,
}

/// Returns a random chain of one to five operators above R(x, y) ⋈ S(y, z),
/// which is joined in a random order, and half of the time as a union of two joins.
///
/// Where the chain projects away a marker of only one input, the marker leaves the descent.
fn random_chain(rng: &mut StdRng) -> RandomChain {
    let mut manager = TableManager::new();
    let r_rows = random_rows(rng, 2);
    let s_rows = random_rows(rng, 2);
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
        node = random_operator(rng, &mut plan, &mut manager, node, &mut relation);
    }
    let expected = relation.in_order(plan.node_markers(node));

    RandomChain {
        manager,
        plan,
        node,
        operators,
        expected,
    }
}

/// A random chain of operators above a join, or above a union of joins,
/// delivers the same rows in one unit as with every node an output,
/// which makes each operator a unit of its own, and both match brute force.
#[test]
fn random_chains_match_their_operators_one_by_one() {
    let mut rng = StdRng::seed_from_u64(29);
    let mut fused_chains = 0;
    let mut pruned_descents = 0;
    for round in 0..300 {
        let RandomChain {
            mut manager,
            mut plan,
            node,
            operators,
            expected,
        } = random_chain(&mut rng);

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

/// A condition that a layer is the same term as a constant binds the layer,
/// and its negation stays a filter.
#[test]
fn constants_are_looked_up() {
    // ground(k), with k the same term as 7, beside data(a), which shares no marker with it.
    // The layout looks k up under every a instead of enumerating ground.
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

/// A condition that a layer is the same term as an expression over shallower layers
/// binds the layer, and the rows are those of the plain filter.
#[test]
fn bindings_are_looked_up() {
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

/// A join that two projections read materializes once,
/// emitting the layers up to the deepest one either projection needs.
#[test]
fn shared_joins_emit_what_every_consumer_needs() {
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

/// A subtraction that compares a computed column runs over the materialized computed rows.
#[test]
fn subtractions_over_computed_columns_read_the_result() {
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

/// A computed constant over a relation without columns yields one row
/// if the relation has the empty row, and none otherwise.
#[test]
fn computed_columns_extend_the_empty_row() {
    // The projection to no column is read twice, as an output and by the computed column.
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

/// Returns the integers of a row.
fn ints(row: &[StorageValue]) -> Vec<i64> {
    row.iter()
        .map(|value| i64::from_payload(value.payload))
        .collect()
}

/// Returns the row that a search of a plan's output finds, as integers,
/// for a verdict on the integers of a row.
fn find_judged(
    plan: &OperationPlan,
    manager: &mut TableManager,
    mut judge: impl FnMut(&[i64]) -> Verdict,
) -> Option<Vec<i64>> {
    let mut dictionary = Dictionary::new();
    let context = EvaluationContext::now();
    let found = plan.find_row(manager, &mut dictionary, context, |_, row| {
        judge(&ints(row))
    });

    found.as_deref().map(ints)
}

/// Returns the row that a search of a plan's output finds, as integers,
/// for a condition on the integers of a row, which rejects each row alone.
fn find(
    plan: &OperationPlan,
    manager: &mut TableManager,
    mut fits: impl FnMut(&[i64]) -> bool,
) -> Option<Vec<i64>> {
    find_judged(plan, manager, |row| {
        if fits(row) {
            Verdict::Accept
        } else {
            Verdict::Reject { prefix: row.len() }
        }
    })
}

/// Builds the paths edge(x, y) ⋈ edge(y, z) whose middle node is not blocked,
/// with their length s = x + z if it is above 6, and returns the plan with that node.
///
/// The edges form the chain 1 → 2 → 3 → 4 → 5, and node 2 is blocked,
/// so the only such path is 3 → 4 → 5, of length 8.
fn long_open_paths(manager: &mut TableManager) -> (OperationPlan, NodeId, [Marker; 3]) {
    let edge = add_table(manager, 2, &[&[&[1, 2], &[2, 3], &[3, 4], &[4, 5]]]);
    let blocked = add_table(manager, 1, &[&[&[2]]]);

    let mut plan = OperationPlan::new();
    let [x, y, z, s] = plan.markers();
    let first = plan.load(edge, all(), vec![x, y]);
    let second = plan.load(edge, all(), vec![y, z]);
    let paths = plan.join(vec![first, second], vec![x, y, z]);
    let closed = plan.load(blocked, all(), vec![y]);
    let open = plan.subtract(paths, closed);
    let measured = plan.compute(
        open,
        computation(
            vec![x, z],
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Add],
            Vec::new(),
            s,
        ),
    );
    let long = plan.filter(
        measured,
        condition(
            vec![s],
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(6)],
        ),
    );

    (plan, long, [x, y, z])
}

/// A search offers the rows of a join in storage order, with the table manager,
/// until a row fits, and finds no row if none fits.
#[test]
fn a_join_is_searched_until_a_row_fits() {
    // The pairs of the numbers 1 to 4, of which 2 and 4 are stored at revision 2.
    let mut manager = TableManager::new();
    let number = add_table(&mut manager, 1, &[&[&[1], &[3]], &[&[2], &[4]]]);
    let mut dictionary = Dictionary::new();

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let first = plan.load(number, all(), vec![x]);
    let second = plan.load(number, all(), vec![y]);
    let pairs = plan.join(vec![first, second], vec![x, y]);
    plan.output(pairs);
    assert!(plan.finds_rows_in_joins());

    // The first pair of numbers from revision 2 follows the four pairs that start with 1,
    // and the pair (2, 1).
    let mut offered = Vec::new();
    let context = EvaluationContext::now();
    let found = plan.find_row(&mut manager, &mut dictionary, context, |manager, row| {
        offered.push(ints(row));
        let late = |&value| manager.revision_of(number, &[value]) == Some(Revision(2));
        Verdict::for_row(row, row.iter().all(late))
    });
    assert_eq!(found.as_deref().map(ints), Some(vec![2, 2]));
    assert_eq!(offered, [[1, 1], [1, 2], [1, 3], [1, 4], [2, 1], [2, 2]]);

    assert_eq!(find(&plan, &mut manager, |_| false), None);
    assert_eq!(
        find(&plan, &mut manager, |row| row == [4, 3]),
        Some(vec![4, 3])
    );

    // A rejection of a first number passes over the other pairs that start with it.
    let mut offered = Vec::new();
    let found = find_judged(&plan, &mut manager, |row| {
        offered.push(row.to_vec());
        match row {
            [1 | 3, _] => Verdict::Reject { prefix: 1 },
            [_, 2 | 4] => Verdict::Accept,
            _ => Verdict::Reject { prefix: 2 },
        }
    });
    assert_eq!(found, Some(vec![2, 2]));
    assert_eq!(offered, [[1, 1], [2, 1], [2, 2]]);
}

/// Where the joins of a union find the output's rows, each join is searched in turn,
/// and a rejection of every row ends the search before the next join.
#[test]
fn a_union_of_joins_is_searched_join_by_join() {
    // The pairs of the numbers 1 to 4. One join finds those that start with 1 or 3,
    // which are stored at revision 1, and the other those that start with 2 or 4.
    let mut manager = TableManager::new();
    let number = add_table(&mut manager, 1, &[&[&[1], &[3]], &[&[2], &[4]]]);

    let mut plan = OperationPlan::new();
    let [x, y] = plan.markers();
    let mut joins = Vec::new();
    for range in [Revision(0)..Revision(2), Revision(2)..Revision(100)] {
        let first = plan.load(number, range, vec![x]);
        let second = plan.load(number, all(), vec![y]);
        joins.push(plan.join(vec![first, second], vec![x, y]));
    }
    let pairs = plan.union(joins);
    plan.output(pairs);
    assert!(plan.finds_rows_in_joins());
    assert_eq!(members(&plan.layout()).len(), 2);

    // The first join has no pair that starts with an even number, and the second join has.
    let mut offered = Vec::new();
    let found = find_judged(&plan, &mut manager, |row| {
        offered.push(row.to_vec());
        match row[0] % 2 {
            0 => Verdict::Accept,
            _ => Verdict::Reject { prefix: 1 },
        }
    });
    assert_eq!(found, Some(vec![2, 1]));
    assert_eq!(offered, [[1, 1], [3, 1], [2, 1]]);

    let mut offered = 0;
    let found = find_judged(&plan, &mut manager, |_| {
        offered += 1;
        Verdict::Reject { prefix: 0 }
    });
    assert_eq!((found, offered), (None, 1));
}

/// A row found in a join has passed the operators above the join:
/// the subtraction of its descent, and the computed column and the filter of its row stage.
#[test]
fn a_found_row_has_passed_the_operators_above_its_join() {
    let mut manager = TableManager::new();
    let (mut plan, long, [x, y, z]) = long_open_paths(&mut manager);
    // Without the computed column, the output's rows are the rows that the join emits.
    let path = plan.project(long, vec![x, y, z]);
    plan.output(path);
    assert!(plan.finds_rows_in_joins());

    let mut offered = Vec::new();
    let found = find(&plan, &mut manager, |row| {
        offered.push(row.to_vec());
        true
    });
    assert_eq!(found, Some(vec![3, 4, 5]));
    assert_eq!(offered, [[3, 4, 5]], "no other path reaches the search");
}

/// An output with a computed column is computed before it is searched,
/// and its rows are offered with the column.
#[test]
fn an_output_with_a_computed_column_is_searched_once_computed() {
    let mut manager = TableManager::new();
    let (mut plan, long, _) = long_open_paths(&mut manager);
    plan.output(long);
    assert!(!plan.finds_rows_in_joins());

    assert_eq!(find(&plan, &mut manager, |_| true), Some(vec![3, 4, 5, 8]));
    assert_eq!(find(&plan, &mut manager, |row| row[3] != 8), None);
}

/// An output that no join emits is searched in storage order once it is computed:
/// a stored table, an aggregation, and a projection that drops a marker between those it keeps.
#[test]
fn outputs_that_no_join_emits_are_searched_once_computed() {
    // The paths 1 → 2 → 4 and 1 → 3 → 4, and the edge 3 → 5.
    let mut manager = TableManager::new();
    let edge = add_table(
        &mut manager,
        2,
        &[&[&[1, 2], &[2, 4]], &[&[1, 3], &[3, 4], &[3, 5]]],
    );

    let mut stored = OperationPlan::new();
    let [x, y] = stored.markers();
    let load = stored.load(edge, all(), vec![x, y]);
    stored.output(load);

    let mut counted = OperationPlan::new();
    let [x, y, count] = counted.markers();
    let load = counted.load(edge, all(), vec![x, y]);
    let targets = AggregateColumnSpec::new(AggregateFunction::Count, count).per(vec![y]);
    let counts = counted.aggregate(load, vec![x], vec![targets]);
    counted.output(counts);

    let mut ends = OperationPlan::new();
    let [x, y, z] = ends.markers();
    let first = ends.load(edge, all(), vec![x, y]);
    let second = ends.load(edge, all(), vec![y, z]);
    let paths = ends.join(vec![first, second], vec![x, y, z]);
    let merged = ends.project(paths, vec![x, z]);
    ends.output(merged);

    // Each search is for the first row with a given last value.
    let searches: [(&OperationPlan, i64, Vec<i64>); 3] = [
        (&stored, 4, vec![2, 4]),
        (&counted, 1, vec![2, 1]),
        (&ends, 4, vec![1, 4]),
    ];
    for (plan, last, expected) in searches {
        assert!(!plan.finds_rows_in_joins());
        let found = find(plan, &mut manager, |row| row.last() == Some(&last));
        assert_eq!(found, Some(expected));
        assert_eq!(find(plan, &mut manager, |_| false), None);
    }

    // A rejection of a first value passes over the other rows that start with it.
    let mut offered = Vec::new();
    let found = find_judged(&stored, &mut manager, |row| {
        offered.push(row.to_vec());
        if row[0] == 3 {
            Verdict::Accept
        } else {
            Verdict::Reject { prefix: 1 }
        }
    });
    assert_eq!(found, Some(vec![3, 4]));
    assert_eq!(offered, [[1, 2], [2, 4], [3, 4]]);

    // A rejection of every row ends the search.
    let mut offered = 0;
    let found = find_judged(&stored, &mut manager, |_| {
        offered += 1;
        Verdict::Reject { prefix: 0 }
    });
    assert_eq!((found, offered), (None, 1));
}

/// A search of an output without columns finds the empty row if the output has it.
#[test]
fn an_output_without_columns_is_searched_for_the_empty_row() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let empty = manager.register("empty", 1);

    for (table, expected) in [(data, Some(Vec::new())), (empty, None)] {
        let mut plan = OperationPlan::new();
        let [a] = plan.markers();
        let loaded = plan.load(table, all(), vec![a]);
        let exists = plan.project(loaded, Vec::new());
        plan.output(exists);
        assert!(plan.finds_rows_in_joins());

        assert_eq!(find(&plan, &mut manager, |_| true), expected);
        assert_eq!(find(&plan, &mut manager, |_| false), None);
    }
}

/// A row with a computed value that the dictionary cannot store is no row of the output,
/// so a search does not find it, although the output does not deliver the value.
#[test]
fn a_row_whose_computed_value_cannot_be_stored_is_not_found() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1], &[2]]]);
    let mut dictionary = Dictionary::new();

    // The computed column is a null that the dictionary did not mint.
    let mut plan = OperationPlan::new();
    let [a, foreign] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    let extended = plan.compute(
        loaded,
        computation(
            Vec::new(),
            vec![OpCode::Constant(0)],
            vec![DataValue::null(7)],
            foreign,
        ),
    );
    let kept = plan.project(extended, vec![a]);
    plan.output(kept);
    assert!(plan.finds_rows_in_joins());

    let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
    assert!(results.outputs[0].is_empty());
    assert_eq!(find(&plan, &mut manager, |_| true), None);
}

/// A search needs a plan with exactly one output.
#[test]
fn a_search_needs_exactly_one_output() {
    let mut manager = TableManager::new();
    let data = add_table(&mut manager, 1, &[&[&[1]]]);

    let mut plan = OperationPlan::new();
    let [a] = plan.markers();
    let loaded = plan.load(data, all(), vec![a]);
    assert_panics("exactly one output", || find(&plan, &mut manager, |_| true));
    assert_panics("exactly one output", || plan.finds_rows_in_joins());

    let joined = plan.join(vec![loaded], vec![a]);
    plan.output(loaded);
    plan.output(joined);
    assert_panics("exactly one output", || find(&plan, &mut manager, |_| true));
    assert_panics("exactly one output", || plan.finds_rows_in_joins());
}

/// A search of a random chain finds a row of the chain's output that fits,
/// or no row if none fits, whether the joins emit the output or it is computed first.
/// It finds the first such row in storage order, unless several joins emit the output.
/// The search offers no row that a rejection passed over.
#[test]
fn rows_found_in_random_chains_are_rows_of_their_outputs() {
    let mut rng = StdRng::seed_from_u64(31);
    let mut emitted_outputs = 0;
    let mut found_rows = 0;
    let mut passed_over = 0;
    for round in 0..400 {
        let RandomChain {
            mut manager,
            mut plan,
            node,
            expected,
            ..
        } = random_chain(&mut rng);
        plan.output(node);

        let layout = plan.layout();
        let emitted = plan.finds_rows_in_joins();
        let in_order = !emitted || members(&layout).len() == 1;
        emitted_outputs += usize::from(emitted);

        // A row fits if its first value is not the barred one,
        // and its values add up to a given remainder.
        let barred = rng.gen_range(0..5);
        let remainder = rng.gen_range(0..3);
        let fits = |row: &[i64]| row[0] != barred && row.iter().sum::<i64>() % 3 == remainder;
        let fitting: Vec<&Vec<i64>> = expected.iter().filter(|row| fits(row)).collect();

        // A row with the barred first value rejects every row that starts with it.
        let mut offered: Vec<Vec<i64>> = Vec::new();
        let found = find_judged(&plan, &mut manager, |row| {
            offered.push(row.to_vec());
            if row[0] == barred {
                Verdict::Reject { prefix: 1 }
            } else if fits(row) {
                Verdict::Accept
            } else {
                Verdict::Reject { prefix: row.len() }
            }
        });
        match &found {
            Some(row) if in_order => assert_eq!(Some(&row), fitting.first(), "round {round}"),
            Some(row) => assert!(fitting.contains(&row), "round {round}: {row:?}"),
            None => assert!(fitting.is_empty(), "round {round}: {fitting:?}"),
        }
        found_rows += usize::from(found.is_some());

        // Each join, or the computed output, offers at most one row with the barred value.
        let barred_offers = offered.iter().filter(|row| row[0] == barred).count();
        let joins = if emitted { members(&layout).len() } else { 1 };
        assert!(barred_offers <= joins, "round {round}: {offered:?}");
        let barred_rows = expected.iter().filter(|row| row[0] == barred).count();
        passed_over += usize::from(barred_rows > barred_offers);
    }
    assert!(
        emitted_outputs > 100 && emitted_outputs < 300,
        "joins emit some outputs and not others: {emitted_outputs}"
    );
    assert!(found_rows > 100, "many searches find a row: {found_rows}");
    assert!(
        passed_over > 50,
        "many searches pass over a row: {passed_over}"
    );
}
