use super::*;

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::{
    dictionary::Dictionary,
    function::{EvaluationContext, OpCode, SpecializedProgram, StackProgram},
    parallel::Workers,
    storage::StorageValue,
    testing::{CELL_ROWS, assert_panics, at, int_trie, trie},
    trie::{RowBuffer, Trie},
    value::{DataValue, ErrorCounts, EvaluationError, number::decimal},
};

/// Returns the join of a shape over the given tries, which the shape must fit.
fn join<'a>(
    shape: &'a OwnedJoinShape,
    tries: &[&'a Trie],
    subtracted: &[&'a Trie],
    dictionary: Option<&'a Dictionary>,
    arena: &'a JoinArena,
) -> TrieJoin<'a> {
    TrieJoin::new(
        shape.view(),
        tries,
        subtracted,
        dictionary,
        EvaluationContext::now(),
        arena,
    )
}

/// Returns the chain shape R(a, b) ⋈ S(b, c), emitting the given number of layers.
fn chain_shape(emitted: usize, subtracted: Vec<SubtractedShape>) -> OwnedJoinShape {
    chain_shape_with(emitted, subtracted, vec![])
}

/// Returns the chain shape with filters.
fn chain_shape_with(
    emitted: usize,
    subtracted: Vec<SubtractedShape>,
    filters: Vec<FilterShape>,
) -> OwnedJoinShape {
    OwnedJoinShape::new(
        vec![vec![at(0, 0)], vec![at(0, 1), at(1, 0)], vec![at(1, 1)]],
        emitted,
        subtracted,
        filters,
    )
    .expect("the chain shape is well-formed")
}

/// Returns the triangle shape E(a, b) ⋈ E(b, c) ⋈ E(a, c), emitting every layer.
fn triangle_shape() -> OwnedJoinShape {
    OwnedJoinShape::new(
        vec![
            vec![at(0, 0), at(2, 0)],
            vec![at(0, 1), at(1, 0)],
            vec![at(1, 1), at(2, 1)],
        ],
        3,
        vec![],
        vec![],
    )
    .expect("the triangle shape is well-formed")
}

/// Returns the edges of the complete graph on the terms 1 to 4.
///
/// Its four triangles are the triples a < b < c,
/// and terms keep every column at 32 bits.
fn complete_graph() -> Trie {
    let edges: Vec<Vec<StorageValue>> = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]
        .iter()
        .map(|&(from, to)| vec![StorageValue::term(from), StorageValue::term(to)])
        .collect();
    trie(2, &edges)
}

/// A sink collecting the rows of a join,
/// which checks each row's changed layer against the previous row.
#[derive(Debug, Default)]
struct Collected(Vec<Vec<StorageValue>>);

impl RowSink for Collected {
    fn accept(&mut self, row: &[StorageValue], changed: usize) {
        let expected = match self.0.last() {
            None => 0,
            Some(last) => (0..row.len())
                .find(|&index| last[index] != row[index])
                .expect("consecutive rows differ"),
        };
        assert_eq!(changed, expected);
        self.0.push(row.to_vec());
    }
}

/// Returns the rows of a join, draining it,
/// and checks its changed layer against the rows.
fn collect(join: &mut TrieJoin<'_>) -> Vec<Vec<StorageValue>> {
    let mut collected = Collected::default();
    join.drain(&mut collected);
    collected.0
}

/// Returns rows of integers.
fn storage_rows(rows: &[&[i64]]) -> Vec<Vec<StorageValue>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|&value| StorageValue::integer(value))
                .collect()
        })
        .collect()
}

#[test]
fn chain_joins_emit_every_matching_row() {
    let arena = JoinArena::new();
    let left = int_trie(2, &[&[1, 2], &[1, 3], &[2, 3]]);
    let right = int_trie(2, &[&[2, 7], &[3, 8], &[3, 9]]);
    let shape = chain_shape(3, vec![]);
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);

    assert_eq!(
        collect(&mut join),
        storage_rows(&[&[1, 2, 7], &[1, 3, 8], &[1, 3, 9], &[2, 3, 8], &[2, 3, 9]])
    );
}

#[test]
fn triangle_joins_find_every_triangle() {
    let arena = JoinArena::new();
    let edges = complete_graph();
    let shape = triangle_shape();

    let mut join = join(&shape, &[&edges, &edges, &edges], &[], None, &arena);
    assert_eq!(collect(&mut join).len(), 4);
}

#[test]
fn rebinding_matches_building_and_refuses_other_kinds() {
    let arena = JoinArena::new();
    // The chain R(a, b) ⋈ S(b, c), rebound between pairs of blocks.
    let shape = chain_shape(3, vec![]);
    let terms = |pairs: &[(u64, u64)]| {
        trie(
            2,
            &pairs
                .iter()
                .map(|&(a, b)| vec![StorageValue::term(a), StorageValue::term(b)])
                .collect::<Vec<_>>(),
        )
    };
    let (r1, s1) = (terms(&[(1, 2), (2, 3)]), terms(&[(2, 5), (3, 6), (3, 7)]));
    let (r2, s2) = (terms(&[(4, 7), (5, 7)]), terms(&[(7, 8), (7, 9)]));

    let mut rebound = join(&shape, &[&r1, &s1], &[], None, &arena);
    assert_eq!(collect(&mut rebound).len(), 3);
    assert!(rebound.rebind(&shape.view(), &[&r2, &s2], &arena));
    let rows = collect(&mut rebound);
    let mut fresh = join(&shape, &[&r2, &s2], &[], None, &arena);
    assert_eq!(rows, collect(&mut fresh));
    assert_eq!(rows.len(), 4);
    // Rebinding back works too, and a rebound join drains fully.
    assert!(rebound.rebind(&shape.view(), &[&r1, &s1], &arena));
    assert_eq!(collect(&mut rebound).len(), 3);

    // A block of another layout needs another kind of layer: a 64-bit
    // term column against the 32-bit core, or an empty block.
    let terms64 = terms(&[(u64::from(u32::MAX) + 1, 2)]);
    let empty = Trie::empty(2);
    let mut join32 = join(&shape, &[&r1, &s1], &[], None, &arena);
    assert!(!join32.rebind(&shape.view(), &[&terms64, &s1], &arena));
    let mut join32 = join(&shape, &[&r1, &s1], &[], None, &arena);
    assert!(!join32.rebind(&shape.view(), &[&empty, &s1], &arena));
}

#[test]
fn join_columns_may_mix_tags() {
    let arena = JoinArena::new();
    // The join column mixes terms and integers.
    let left = trie(
        2,
        &[
            vec![StorageValue::integer(1), StorageValue::term(0)],
            vec![StorageValue::integer(1), StorageValue::integer(3)],
            vec![StorageValue::integer(2), StorageValue::integer(3)],
        ],
    );
    let right = trie(
        2,
        &[
            vec![StorageValue::term(0), StorageValue::integer(7)],
            vec![StorageValue::integer(3), StorageValue::integer(8)],
            vec![StorageValue::integer(3), StorageValue::term(1)],
        ],
    );
    let shape = chain_shape(3, vec![]);
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);
    assert_eq!(collect(&mut join).len(), 5);
}

#[test]
fn existence_suffixes_deduplicate() {
    let arena = JoinArena::new();
    // R(a, b) with three matching c's in S: emitting only (a, b) must
    // yield each pair once, not three times.
    let left = int_trie(2, &[&[1, 2], &[1, 5]]);
    let right = int_trie(2, &[&[2, 7], &[2, 8], &[2, 9]]);
    let shape = chain_shape(2, vec![]);
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);

    // (1, 5) has no S-match and is filtered by the existence check.
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 2]]));
}

#[test]
fn nullary_output_reports_bare_existence() {
    let arena = JoinArena::new();
    let left = int_trie(2, &[&[1, 2]]);
    let right = int_trie(2, &[&[2, 7]]);
    let shape = chain_shape(0, vec![]);
    let mut existing = join(&shape, &[&left, &right], &[], None, &arena);
    assert_eq!(collect(&mut existing), vec![Vec::<StorageValue>::new()]);

    // With no matching rows, not even the empty row is produced.
    let empty_right = Trie::empty(2);
    let mut missing = join(&shape, &[&left, &empty_right], &[], None, &arena);
    assert!(collect(&mut missing).is_empty());
}

#[test]
fn subtracted_inputs_block_matching_rows() {
    let arena = JoinArena::new();
    // R(a, b) minus T(b): T maps only the second layer.
    let relation = int_trie(2, &[&[1, 2], &[1, 3], &[2, 2], &[2, 4]]);
    let blocked = int_trie(1, &[&[2], &[4]]);
    let shape = OwnedJoinShape::new(
        vec![vec![at(0, 0)], vec![at(0, 1)]],
        2,
        vec![SubtractedShape { layers: vec![1] }],
        vec![],
    )
    .expect("the shape is well-formed");

    let mut join = join(&shape, &[&relation], &[&blocked], None, &arena);
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 3]]));
}

#[test]
fn subtracted_inputs_follow_across_gaps() {
    let arena = JoinArena::new();
    // R(a, b) ⋈ S(b, c) minus U(a, c): U's columns map to layers 0
    // and 2, arming across the middle layer and across backtracking.
    let left = int_trie(2, &[&[1, 2], &[2, 2]]);
    let right = int_trie(2, &[&[2, 7], &[2, 8]]);
    let minus = int_trie(2, &[&[1, 7], &[2, 8]]);
    let shape = chain_shape(3, vec![SubtractedShape { layers: vec![0, 2] }]);

    let mut join = join(&shape, &[&left, &right], &[&minus], None, &arena);
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 2, 8], &[2, 2, 7]]));
}

#[test]
fn drained_buffers_are_sorted_and_build_tries_directly() {
    let arena = JoinArena::new();
    let left = int_trie(2, &[&[2, 3], &[1, 2], &[1, 3]]);
    let right = int_trie(2, &[&[3, 1], &[2, 9], &[3, 4]]);
    let shape = chain_shape(3, vec![]);
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);

    let mut buffer = RowBuffer::new(3);
    join.drain(&mut buffer);
    assert!(buffer.is_sorted());
    let rows = buffer.len();
    let result = Trie::from_buffer(buffer);
    assert_eq!(result.row_count(), rows);
}

#[test]
fn shapes_must_fit_the_tries() {
    let unary = int_trie(1, &[&[1]]);
    let misfit = |shape: &OwnedJoinShape, tries: &[&Trie], subtracted: &[&Trie]| {
        shape.view().validate_fit(tries, subtracted, None).is_err()
    };

    // The chain shape needs two binary tries; building the join refuses a misfit.
    let shape = chain_shape(3, vec![]);
    assert!(misfit(&shape, &[&unary, &unary], &[]));
    assert!(misfit(&shape, &[&unary], &[]));
    let arena = JoinArena::new();
    assert_panics("more columns", || {
        TrieJoin::new(
            shape.view(),
            &[&unary],
            &[],
            None,
            EvaluationContext::now(),
            &arena,
        )
    });

    // A join needs one subtracted trie per subtracted input,
    // each with at least the columns its input maps.
    let with_subtraction = chain_shape(3, vec![SubtractedShape { layers: vec![0, 2] }]);
    let binary = int_trie(2, &[&[1, 2]]);
    assert!(misfit(&with_subtraction, &[&binary, &binary], &[]));
    assert!(misfit(&with_subtraction, &[&binary, &binary], &[&unary]));
    assert!(misfit(&shape, &[&binary, &binary], &[&binary]));
}

/// Returns a random value: mode 0 mixes terms and integers,
/// mode 1 draws terms only (32-bit columns),
/// and mode 2 integers only (64-bit columns).
fn random_value(rng: &mut StdRng, mode: u8) -> StorageValue {
    match mode {
        0 if rng.r#gen::<bool>() => StorageValue::term(rng.gen_range(0..4)),
        0 => StorageValue::integer(rng.gen_range(-2..2)),
        1 => StorageValue::term(rng.gen_range(0..6)),
        _ => StorageValue::integer(rng.gen_range(-3..3)),
    }
}

/// Returns a random set of rows.
fn random_rows(rng: &mut StdRng, arity: usize, mode: u8) -> Vec<Vec<StorageValue>> {
    let count = rng.gen_range(0..20);
    (0..count)
        .map(|_| (0..arity).map(|_| random_value(rng, mode)).collect())
        .collect()
}

#[test]
fn random_joins_match_brute_force() {
    let arena = JoinArena::new();
    // The differential property: chain joins (full, existence-only,
    // and with a gapped subtraction) against nested-loop evaluation,
    // across data that exercises all three join cores.
    let mut rng = StdRng::seed_from_u64(11);
    for round in 0..300 {
        let mode = (round % 3) as u8;
        let left_rows = random_rows(&mut rng, 2, mode);
        let right_rows = random_rows(&mut rng, 2, mode);
        let minus_rows = random_rows(&mut rng, 2, mode);

        let left = trie(2, &left_rows);
        let right = trie(2, &right_rows);
        let minus = trie(2, &minus_rows);

        let subtraction = vec![SubtractedShape { layers: vec![0, 2] }];
        for (emitted, subtracted) in [(3, vec![]), (2, vec![]), (3, subtraction.clone())] {
            let with_minus = !subtracted.is_empty();
            let anti = if with_minus { vec![&minus] } else { vec![] };
            let shape = chain_shape(emitted, subtracted);
            let mut join = join(&shape, &[&left, &right], &anti, None, &arena);

            let mut expected = BTreeSet::new();
            for first in &left_rows {
                for second in &right_rows {
                    if first[1] != second[0] {
                        continue;
                    }
                    if with_minus
                        && minus_rows
                            .iter()
                            .any(|row| row[0] == first[0] && row[1] == second[1])
                    {
                        continue;
                    }
                    let row = [first[0], first[1], second[1]];
                    expected.insert(row[..emitted].to_vec());
                }
            }

            assert_eq!(
                collect(&mut join),
                expected.into_iter().collect::<Vec<_>>(),
                "round {round}, emit {emitted}, minus {with_minus}"
            );
        }
    }
}

/// Runs a join once per cell of a fixed partition,
/// layer 0 split at 0 and layer 1 split at -1 and 1,
/// and checks that the rows of every cell are sorted,
/// that the cells are disjoint,
/// and that together they are the unpartitioned result.
fn check_cells(
    shape: &OwnedJoinShape,
    tries: &[&Trie],
    subtracted: &[&Trie],
    dictionary: Option<&Dictionary>,
    expected: &[Vec<StorageValue>],
) {
    let arena = JoinArena::new();
    let zero = StorageValue::integer(0);
    let minus = StorageValue::integer(-1);
    let one = StorageValue::integer(1);
    let first = [(None, Some(zero)), (Some(zero), None)];
    let second = [
        (None, Some(minus)),
        (Some(minus), Some(one)),
        (Some(one), None),
    ];

    let mut all: Vec<Vec<StorageValue>> = Vec::new();
    for &(low0, high0) in &first {
        for &(low1, high1) in &second {
            let mut join = join(shape, tries, subtracted, dictionary, &arena);
            join.clip_layer(0, low0, high0);
            if shape.view().emitted > 1 {
                join.clip_layer(1, low1, high1);
            } else if low1.is_some() {
                // A layer that is not emitted is not split; the
                // remaining second-layer cells would repeat rows.
                continue;
            }
            let rows = collect(&mut join);
            assert!(rows.is_sorted(), "cell rows are in storage order");
            for row in &rows {
                assert!(!all.contains(row), "cells are disjoint");
            }
            all.extend(rows);
        }
    }

    all.sort();
    assert_eq!(all, expected);
}

#[test]
fn cells_partition_the_join() {
    // Integer data keeps every layer comparable with the clip bounds.
    let mut rng = StdRng::seed_from_u64(23);
    let dictionary = Dictionary::new();
    for _ in 0..100 {
        let left_rows = random_rows(&mut rng, 2, 2);
        let right_rows = random_rows(&mut rng, 2, 2);
        let minus_rows = random_rows(&mut rng, 2, 2);
        let left = trie(2, &left_rows);
        let right = trie(2, &right_rows);
        let minus = trie(2, &minus_rows);
        let subtraction = vec![SubtractedShape { layers: vec![0, 2] }];
        let condition = hook(
            vec![2, 0],
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Less],
            vec![],
        );
        for (emitted, subtracted, filters) in [
            (3, vec![], vec![]),
            (2, vec![], vec![]),
            (1, vec![], vec![]),
            (3, subtraction.clone(), vec![]),
            (3, vec![], vec![condition]),
        ] {
            let anti = if subtracted.is_empty() {
                vec![]
            } else {
                vec![&minus]
            };
            let shape = chain_shape_with(emitted, subtracted, filters);
            let arena = JoinArena::new();
            let mut unpartitioned =
                join(&shape, &[&left, &right], &anti, Some(&dictionary), &arena);
            let expected = collect(&mut unpartitioned);
            check_cells(
                &shape,
                &[&left, &right],
                &anti,
                Some(&dictionary),
                &expected,
            );
        }
    }
}

#[test]
fn cells_partition_bound_layers_of_u32_joins() {
    let arena = JoinArena::new();
    let edges = complete_graph();
    let shape = triangle_shape();
    let tries = [&edges, &edges, &edges];
    let mut unpartitioned = join(&shape, &tries, &[], None, &arena);
    let expected = collect(&mut unpartitioned);
    assert_eq!(expected.len(), 4);

    let mut all = Vec::new();
    for (low, high) in [
        (None, Some(StorageValue::term(2))),
        (Some(StorageValue::term(2)), Some(StorageValue::term(3))),
        (Some(StorageValue::term(3)), None),
    ] {
        for (low1, high1) in [
            (None, Some(StorageValue::term(3))),
            (Some(StorageValue::term(3)), None),
        ] {
            let mut join = join(&shape, &tries, &[], None, &arena);
            join.clip_layer(0, low, high);
            join.clip_layer(1, low1, high1);
            all.extend(collect(&mut join));
        }
    }
    all.sort();
    assert_eq!(all, expected);

    // A bound layer, probed rather than enumerated, must respect the clip like any other:
    // c = a + 1 over R(a, b) ⋈ S(b, c), clipped on b.
    let left = int_trie(2, &[&[1, 2], &[1, 3], &[2, 3], &[2, 5]]);
    let right = int_trie(2, &[&[2, 2], &[3, 2], &[3, 3], &[5, 3]]);
    let bound = BoundLayerShape::computed(
        2,
        vec![0],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
            vec![DataValue::integer(1)],
            1,
        ),
    );
    let shape = chain_shape(3, vec![])
        .with_bound_layers(vec![bound])
        .expect("well-formed");
    let dictionary = Dictionary::new();
    let mut unpartitioned = join(&shape, &[&left, &right], &[], Some(&dictionary), &arena);
    let expected = collect(&mut unpartitioned);
    assert_eq!(
        expected,
        storage_rows(&[&[1, 2, 2], &[1, 3, 2], &[2, 3, 3], &[2, 5, 3]])
    );
    check_cells(&shape, &[&left, &right], &[], Some(&dictionary), &expected);
}

#[test]
fn split_joins_deliver_the_sequential_rows() {
    // A unary relation past the parallel threshold splits on its only layer,
    // so the rows of the cells, in cell order, are the rows of the whole join.
    let shape = OwnedJoinShape::new(vec![vec![at(0, 0)]], 1, vec![], vec![])
        .expect("the shape is well-formed");
    let small = int_trie(1, &[[1], [2]]);
    assert!(
        Cells::partition(&shape.view(), &[&small]).is_none(),
        "little work runs whole"
    );

    let relation = int_trie(1, &(0..CELL_ROWS).map(|value| [value]).collect::<Vec<_>>());
    let tries = [&relation];
    let Some(cells) = Cells::partition(&shape.view(), &tries) else {
        assert_eq!(
            Workers::get().threads(),
            1,
            "only a single thread runs it whole"
        );
        return;
    };
    let dictionary = Dictionary::new();
    let template = JoinTemplate {
        shape: shape.view(),
        subtracted: &[],
        dictionary: &dictionary,
        evaluation: EvaluationContext::now(),
    };
    let (delivered, errors) = cells.run(template, &tries, collect);
    assert!(delivered.len() > 1);
    assert!(errors.is_empty());

    let arena = JoinArena::new();
    let mut whole = join(&shape, &tries, &[], None, &arena);
    assert_eq!(delivered.concat(), collect(&mut whole));
}

#[test]
fn row_stages_extend_and_filter_drained_rows() {
    // Over R(a, b) ⋈ S(b, c): s = a + c, d = a * 10 (stale only when a changes),
    // and the post-filter s > 9; the row delivers d, then s.
    let left = int_trie(2, &[[1, 2], [2, 3]]);
    let right = int_trie(2, &[[2, 7], [3, 8], [3, 9]]);
    let shape = chain_shape(3, vec![]);
    let program = |operations, constants, arity| {
        SpecializedProgram::new(StackProgram::new(operations, constants, arity))
    };
    let stage = RowStageShape {
        computed: vec![
            ComputedShape {
                inputs: vec![RowInput::Layer(0), RowInput::Layer(2)],
                program: program(
                    vec![OpCode::Input(0), OpCode::Input(1), OpCode::Add],
                    vec![],
                    2,
                ),
                deepest: Some(2),
                volatile: false,
            },
            ComputedShape {
                inputs: vec![RowInput::Layer(0)],
                program: program(
                    vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Multiply],
                    vec![DataValue::integer(10)],
                    1,
                ),
                deepest: Some(0),
                volatile: false,
            },
        ],
        post_filters: vec![PostFilterShape {
            inputs: vec![RowInput::Computed(0)],
            program: program(
                vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
                vec![DataValue::integer(9)],
                1,
            ),
        }],
        delivered: vec![1, 0],
    };

    let mut dictionary = Dictionary::new();
    let arena = JoinArena::new();
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);
    let mut evaluator = RowEvaluator::new(&stage, 3, &dictionary, EvaluationContext::now());
    join.drain(&mut evaluator);
    let pending = evaluator.finish();

    let mut buffer = RowBuffer::new(5);
    let errors = pending.encode(&stage.delivered, &mut dictionary, &mut buffer);
    assert!(errors.is_empty());
    let rows: Vec<Vec<StorageValue>> = (0..buffer.len())
        .map(|index| buffer.row(index).to_vec())
        .collect();
    assert_eq!(
        rows,
        storage_rows(&[&[2, 3, 8, 20, 10], &[2, 3, 9, 20, 11]])
    );
}

/// Returns a filter from the parts of its program.
fn hook(inputs: Vec<usize>, operations: Vec<OpCode>, constants: Vec<DataValue>) -> FilterShape {
    let arity = inputs.len();
    let program = StackProgram::new(operations, constants, arity);
    FilterShape::new(inputs, program)
}

#[test]
fn filters_prune_rows_on_the_integer_fast_path() {
    let arena = JoinArena::new();
    // R(a, b) join S(b, c), keeping rows with b != 3.
    let left = int_trie(2, &[&[1, 2], &[1, 3], &[2, 3]]);
    let right = int_trie(2, &[&[2, 7], &[3, 8], &[3, 9]]);
    let not_three = hook(
        vec![1],
        vec![
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Equal,
            OpCode::Not,
        ],
        vec![DataValue::integer(3)],
    );
    let shape = chain_shape_with(3, vec![], vec![not_three]);

    let dictionary = Dictionary::new();
    let mut join = join(&shape, &[&left, &right], &[], Some(&dictionary), &arena);
    assert!(join.filter_uses_integer_form(0));
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 2, 7]]));
    assert!(join.evaluation_errors().is_empty());
}

#[test]
fn identity_filters_agree_with_the_interpreter() {
    let arena = JoinArena::new();
    let mut dictionary = Dictionary::new();
    let terms: Vec<StorageValue> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|name| {
            dictionary
                .encode(DataValue::iri(name).as_ref())
                .expect("IRIs are representable")
        })
        .collect();
    let (present, absent) = (DataValue::iri("a"), DataValue::iri("z"));

    // Over R(a, b) ⋈ S(b, c):
    // a = c, a != c, a = <a>, and a != <z>, whose constant the dictionary does not hold.
    let conditions = [
        (vec![0, 2], vec![OpCode::Same], vec![]),
        (vec![0, 2], vec![OpCode::Same, OpCode::Not], vec![]),
        (vec![0], vec![OpCode::Same], vec![present]),
        (vec![0], vec![OpCode::Same, OpCode::Not], vec![absent]),
    ];

    let mut rng = StdRng::seed_from_u64(29);
    for round in 0..50 {
        let mut random_rows = || -> Vec<Vec<StorageValue>> {
            (0..rng.gen_range(1..16))
                .map(|_| (0..2).map(|_| terms[rng.gen_range(0..4)]).collect())
                .collect()
        };
        let left = trie(2, &random_rows());
        let right = trie(2, &random_rows());

        for (inputs, comparison, constants) in &conditions {
            let mut operations = vec![OpCode::Input(0)];
            operations.push(match inputs.len() {
                1 => OpCode::Constant(0),
                _ => OpCode::Input(1),
            });
            operations.extend(comparison);
            // A double negation hides the identity from the recognition,
            // so the interpreter decides the same condition.
            let mut disguised = operations.clone();
            disguised.extend([OpCode::Not, OpCode::Not]);

            let fast = chain_shape_with(
                3,
                vec![],
                vec![hook(inputs.clone(), operations, constants.clone())],
            );
            let slow = chain_shape_with(
                3,
                vec![],
                vec![hook(inputs.clone(), disguised, constants.clone())],
            );
            let mut fast = join(&fast, &[&left, &right], &[], Some(&dictionary), &arena);
            let mut slow = join(&slow, &[&left, &right], &[], Some(&dictionary), &arena);
            assert!(fast.filter_uses_identity_form(0));
            assert!(!slow.filter_uses_identity_form(0));
            assert_eq!(
                collect(&mut fast),
                collect(&mut slow),
                "round {round}, {comparison:?}"
            );
        }
    }
}

#[test]
fn constant_bound_layers_agree_with_identity_filters() {
    // Integer data keeps every layer comparable with the clip bounds of the cells.
    let mut rng = StdRng::seed_from_u64(31);
    let dictionary = Dictionary::new();
    for round in 0..50 {
        let left = trie(2, &random_rows(&mut rng, 2, 2));
        let right = trie(2, &random_rows(&mut rng, 2, 2));
        let tries = [&left, &right];

        // Each layer of R(a, b) ⋈ S(b, c), emitted or not, bound to a constant the data may hold
        // and to one the dictionary does not hold.
        for layer in 0..3 {
            for emitted in [3, 1] {
                for constant in [DataValue::integer(1), DataValue::iri("absent")] {
                    let operations = vec![OpCode::Constant(0), OpCode::Input(0), OpCode::Same];
                    let filter = hook(vec![layer], operations, vec![constant.clone()]);
                    let filtered = chain_shape_with(emitted, vec![], vec![filter]);
                    let bound = chain_shape(emitted, vec![])
                        .with_bound_layers(vec![BoundLayerShape::constant(layer, constant)])
                        .expect("well-formed");

                    let arena = JoinArena::new();
                    let mut by_filter = join(&filtered, &tries, &[], Some(&dictionary), &arena);
                    let expected = collect(&mut by_filter);
                    let mut by_lookup = join(&bound, &tries, &[], Some(&dictionary), &arena);
                    assert_eq!(
                        collect(&mut by_lookup),
                        expected,
                        "round {round}, layer {layer}, emitted {emitted}"
                    );
                    check_cells(&bound, &tries, &[], Some(&dictionary), &expected);
                }
            }
        }
    }
}

#[test]
fn filters_decode_dictionary_values() {
    let arena = JoinArena::new();
    // R(a, name), keeping rows whose name has five characters.
    let mut dictionary = Dictionary::new();
    let mut buffer = RowBuffer::new(2);
    for (key, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        let name = dictionary
            .encode(DataValue::string(name).as_ref())
            .expect("strings are representable");
        buffer.push_row(&[StorageValue::integer(key), name]);
    }
    let relation = Trie::from_buffer(buffer);

    let five_letters = hook(
        vec![1],
        vec![
            OpCode::Input(0),
            OpCode::StringLength,
            OpCode::Constant(0),
            OpCode::Equal,
        ],
        vec![DataValue::integer(5)],
    );
    let shape = OwnedJoinShape::new(
        vec![vec![at(0, 0)], vec![at(0, 1)]],
        2,
        vec![],
        vec![five_letters],
    )
    .expect("the shape is well-formed");

    let mut join = join(&shape, &[&relation], &[], Some(&dictionary), &arena);
    assert!(!join.filter_uses_integer_form(0));
    let rows = collect(&mut join);
    assert_eq!(
        rows.iter().map(|row| row[0]).collect::<Vec<_>>(),
        &[StorageValue::integer(1), StorageValue::integer(3)]
    );
}

#[test]
fn filter_errors_are_counted_not_swallowed() {
    let arena = JoinArena::new();
    // Comparing integers against a string errors per candidate; the
    // rows vanish from the result but the errors are counted.
    let relation = int_trie(1, &[&[1], &[2], &[3]]);
    let broken = hook(
        vec![0],
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Less],
        vec![DataValue::string("x")],
    );
    let shape = OwnedJoinShape::new(vec![vec![at(0, 0)]], 1, vec![], vec![broken])
        .expect("the shape is well-formed");

    let dictionary = Dictionary::new();
    let mut join = join(&shape, &[&relation], &[], Some(&dictionary), &arena);
    assert!(collect(&mut join).is_empty());
    assert_eq!(
        (
            join.evaluation_errors()
                .count(EvaluationError::Incomparable),
            join.evaluation_errors().total()
        ),
        (3, 3)
    );
}

#[test]
fn filters_require_a_dictionary() {
    let arena = JoinArena::new();
    let relation = int_trie(1, &[&[1]]);
    let anything = hook(vec![0], vec![OpCode::Input(0)], vec![]);
    let shape = OwnedJoinShape::new(vec![vec![at(0, 0)]], 1, vec![], vec![anything])
        .expect("the shape is well-formed");
    assert_panics("needs the dictionary", || {
        TrieJoin::new(
            shape.view(),
            &[&relation],
            &[],
            None,
            EvaluationContext::now(),
            &arena,
        )
    });
}

#[test]
fn hand_built_shapes_are_validated() {
    // A shape whose filter reads no layer must come back as a
    // diagnostic, not a panic inside the driver.
    let layers = vec![vec![at(0, 0)]];
    let broken = hook(
        vec![],
        vec![OpCode::Constant(0)],
        vec![DataValue::integer(1)],
    );
    let filters = [broken];
    let shape = JoinShape {
        layers: &layers,
        emitted: 1,
        subtracted: &[],
        filters: &filters,
        bound_layers: &[],
    };
    assert!(shape.validate().is_err());
}

#[test]
fn random_filtered_joins_match_brute_force() {
    let arena = JoinArena::new();
    // The chain join with the cross-layer condition c < a, checked
    // against nested loops; integer data keeps the semantics exact.
    let mut rng = StdRng::seed_from_u64(17);
    let dictionary = Dictionary::new();
    for round in 0..100 {
        let left_rows = random_rows(&mut rng, 2, 2);
        let right_rows = random_rows(&mut rng, 2, 2);
        let left = trie(2, &left_rows);
        let right = trie(2, &right_rows);

        let condition = hook(
            vec![2, 0],
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Less],
            vec![],
        );
        let shape = chain_shape_with(3, vec![], vec![condition]);
        let mut join = join(&shape, &[&left, &right], &[], Some(&dictionary), &arena);

        let mut expected = BTreeSet::new();
        for first in &left_rows {
            for second in &right_rows {
                if first[1] == second[0] && second[1] < first[0] {
                    expected.insert(vec![first[0], first[1], second[1]]);
                }
            }
        }
        assert_eq!(
            collect(&mut join),
            expected.into_iter().collect::<Vec<_>>(),
            "round {round}"
        );
        assert!(join.evaluation_errors().is_empty());
    }
}

/// Returns the rows and errors of aggregating one binary relation grouped by its first column.
fn aggregate_relation(
    rows: &[Vec<StorageValue>],
    aggregates: Vec<AggregateShape>,
    dictionary: &mut Dictionary,
) -> (RowBuffer, ErrorCounts) {
    let relation = trie(2, rows);
    let shape = OwnedJoinShape::new(vec![vec![at(0, 0)], vec![at(0, 1)]], 2, vec![], vec![])
        .expect("the shape is well-formed");
    let arena = JoinArena::new();
    let mut join = join(&shape, &[&relation], &[], None, &arena);
    let mut aggregator = Aggregator::new(
        AggregationShape {
            group_layers: 1,
            aggregates: &aggregates,
        },
        2,
        dictionary,
    );
    join.drain(&mut aggregator);
    let (rows, errors) = aggregator.finish().encode(dictionary);
    (rows.into_rows(), errors)
}

/// Returns the aggregate of a function over layer 1, without an identity of its own.
fn over_values(kind: AggregateKind) -> AggregateShape {
    AggregateShape {
        kind,
        input: Some(1),
        pair: None,
        per: vec![],
    }
}

#[test]
fn aggregates_range_over_their_own_identities() {
    // One shared relation (g, a, b); each aggregate names its own
    // identity, so a's count ignores rows differing only in b and
    // vice versa, while the pair-identified sum sees every row.
    let rows = storage_rows(&[&[1, 2, 7], &[1, 2, 8], &[1, 3, 7]]);
    let relation = trie(3, &rows);
    let shape = OwnedJoinShape::new(
        vec![vec![at(0, 0)], vec![at(0, 1)], vec![at(0, 2)]],
        3,
        vec![],
        vec![],
    )
    .expect("the shape is well-formed");
    let arena = JoinArena::new();
    let mut join = join(&shape, &[&relation], &[], None, &arena);
    let aggregates = vec![
        AggregateShape {
            kind: AggregateKind::Count,
            input: Some(1),
            pair: None,
            per: vec![],
        },
        AggregateShape {
            kind: AggregateKind::Count,
            input: Some(2),
            pair: None,
            per: vec![],
        },
        AggregateShape {
            kind: AggregateKind::Sum,
            input: Some(1),
            pair: None,
            per: vec![2],
        },
    ];
    let mut dictionary = Dictionary::new();
    let mut aggregator = Aggregator::new(
        AggregationShape {
            group_layers: 1,
            aggregates: &aggregates,
        },
        3,
        &dictionary,
    );
    join.drain(&mut aggregator);
    let (buffer, errors) = aggregator.finish().encode(&mut dictionary);
    let buffer = buffer.into_rows();

    assert!(errors.is_empty());
    assert_eq!(buffer.len(), 1);
    // (1, two distinct a, two distinct b, 2 + 2 + 3)
    assert_eq!(
        buffer.row(0),
        &[
            StorageValue::integer(1),
            StorageValue::integer(2),
            StorageValue::integer(2),
            StorageValue::integer(7),
        ]
    );
}

#[test]
fn aggregates_run_in_one_group_pass() {
    // Group (1): values 2, 4; group (2): value 10.
    let rows = storage_rows(&[&[1, 2], &[1, 4], &[2, 10]]);
    let mut dictionary = Dictionary::new();
    let aggregates = [
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::Average,
    ]
    .map(over_values)
    .to_vec();
    let (buffer, errors) = aggregate_relation(&rows, aggregates, &mut dictionary);

    assert!(errors.is_empty());
    assert_eq!(buffer.len(), 2);
    // (1, count 2, sum 6, min 2, max 4, avg 3)
    assert_eq!(
        buffer.row(0),
        &[
            StorageValue::integer(1),
            StorageValue::integer(2),
            StorageValue::integer(6),
            StorageValue::integer(2),
            StorageValue::integer(4),
            StorageValue::integer(3),
        ]
    );
    // (2, count 1, sum 10, min 10, max 10, avg 10)
    assert_eq!(
        buffer.row(1),
        &[
            StorageValue::integer(2),
            StorageValue::integer(1),
            StorageValue::integer(10),
            StorageValue::integer(10),
            StorageValue::integer(10),
            StorageValue::integer(10),
        ]
    );
}

#[test]
fn averages_divide_exactly_into_decimals() {
    let rows = storage_rows(&[&[1, 1], &[1, 2]]);
    let mut dictionary = Dictionary::new();
    let (buffer, _) = aggregate_relation(
        &rows,
        vec![over_values(AggregateKind::Average)],
        &mut dictionary,
    );
    let average = dictionary.decode(buffer.row(0)[1]).expect("value decodes");
    assert_eq!(
        average.to_owned(),
        DataValue::decimal(15, -1).expect("value is representable")
    );
}

#[test]
fn extrema_use_value_order_never_storage_order() {
    // Extrema compare values, not storage encodings: min over one
    // integer 100 and one double 1.0 is the double, although terms
    // and integers sort before doubles in storage order.
    let rows = vec![
        vec![StorageValue::integer(1), StorageValue::integer(100)],
        vec![StorageValue::integer(1), StorageValue::double(1.0)],
    ];
    let mut dictionary = Dictionary::new();
    let (buffer, _) = aggregate_relation(
        &rows,
        vec![
            over_values(AggregateKind::Min),
            over_values(AggregateKind::Max),
        ],
        &mut dictionary,
    );
    assert_eq!(buffer.row(0)[1], StorageValue::double(1.0));
    assert_eq!(buffer.row(0)[2], StorageValue::integer(100));
}

#[test]
fn extrema_over_strings_compare_content() {
    let mut dictionary = Dictionary::new();
    let encode = |dictionary: &mut Dictionary, name: &str| {
        dictionary
            .encode(DataValue::string(name).as_ref())
            .expect("strings are representable")
    };
    // Insertion order deliberately disagrees with lexicographic order.
    let rows = vec![
        vec![StorageValue::integer(1), encode(&mut dictionary, "mia")],
        vec![StorageValue::integer(1), encode(&mut dictionary, "alice")],
        vec![StorageValue::integer(1), encode(&mut dictionary, "zoe")],
    ];
    let (buffer, _) = aggregate_relation(
        &rows,
        vec![
            over_values(AggregateKind::Min),
            over_values(AggregateKind::Max),
        ],
        &mut dictionary,
    );
    let smallest = dictionary.decode(buffer.row(0)[1]).expect("value decodes");
    assert_eq!(smallest.to_owned(), DataValue::string("alice"));
    let largest = dictionary.decode(buffer.row(0)[2]).expect("value decodes");
    assert_eq!(largest.to_owned(), DataValue::string("zoe"));
}

#[test]
fn sums_overflow_into_wide_integers_not_panics() {
    let rows = vec![
        vec![
            StorageValue::integer(1),
            StorageValue::integer(i64::MAX - 1),
        ],
        vec![StorageValue::integer(1), StorageValue::integer(i64::MAX)],
    ];
    let mut dictionary = Dictionary::new();
    let (buffer, errors) = aggregate_relation(
        &rows,
        vec![over_values(AggregateKind::Sum)],
        &mut dictionary,
    );
    assert!(errors.is_empty());
    let sum = dictionary.decode(buffer.row(0)[1]).expect("value decodes");
    assert_eq!(
        sum.to_owned(),
        DataValue::decimal_from_lexical("18446744073709551613").expect("well-formed")
    );
}

#[test]
fn sums_carry_wide_numbers_exactly() {
    let mut dictionary = Dictionary::new();
    let mut big = |lexical: &str| {
        dictionary
            .encode(
                DataValue::decimal_from_lexical(lexical)
                    .expect("well-formed")
                    .as_ref(),
            )
            .expect("wide numbers encode")
    };
    let decimal = |mantissa, exponent| {
        StorageValue::decimal(decimal::Compact::new(mantissa, exponent).expect("representable"))
    };
    let rows = vec![
        // A big operand joins the sum.
        vec![StorageValue::integer(1), big("123456789012345678901")],
        vec![StorageValue::integer(1), StorageValue::integer(1)],
        vec![
            StorageValue::integer(1),
            big("0.0000000000000000000012345678901234567890123"),
        ],
        // Aligning far-apart decimals outgrows the i128 workspace.
        vec![StorageValue::integer(2), decimal(1, -1)],
        vec![StorageValue::integer(2), decimal(1, -40)],
        // An average divides the big sum.
        vec![StorageValue::integer(3), big("123456789012345678901")],
        vec![StorageValue::integer(3), big("123456789012345678903")],
    ];
    let (buffer, errors) = aggregate_relation(
        &rows,
        vec![
            over_values(AggregateKind::Sum),
            over_values(AggregateKind::Average),
        ],
        &mut dictionary,
    );
    assert!(errors.is_empty());
    let value = |row: usize, column: usize| {
        dictionary
            .decode(buffer.row(row)[column])
            .expect("value decodes")
            .to_owned()
    };
    let big = |lexical: &str| DataValue::decimal_from_lexical(lexical).expect("well-formed");
    assert_eq!(
        value(0, 1),
        big("123456789012345678902.0000000000000000000012345678901234567890123")
    );
    assert_eq!(
        value(1, 1),
        big("0.1000000000000000000000000000000000000001")
    );
    assert_eq!(value(2, 1), big("246913578024691357804"));
    assert_eq!(value(2, 2), big("123456789012345678902"));
}

#[test]
fn floats_promote_big_sums() {
    // A float beside a wide number approximates the group's sum and
    // average, as it does beside any exact number.
    let mut dictionary = Dictionary::new();
    let big = dictionary
        .encode(
            DataValue::decimal_from_lexical("123456789012345678901")
                .expect("well-formed")
                .as_ref(),
        )
        .expect("wide numbers encode");
    let rows = vec![
        vec![StorageValue::integer(1), big],
        vec![StorageValue::integer(1), StorageValue::double(0.5)],
        vec![StorageValue::integer(2), big],
        vec![StorageValue::integer(2), StorageValue::float(0.5)],
    ];
    let (buffer, errors) = aggregate_relation(
        &rows,
        vec![
            over_values(AggregateKind::Sum),
            over_values(AggregateKind::Average),
        ],
        &mut dictionary,
    );
    assert!(errors.is_empty());
    let value = |row: usize, column: usize| {
        dictionary
            .decode(buffer.row(row)[column])
            .expect("value decodes")
            .to_owned()
    };
    assert_eq!(value(0, 1), DataValue::double(1.2345678901234568e20));
    assert_eq!(value(0, 2), DataValue::double(6.172839450617284e19));
    assert_eq!(value(1, 1), DataValue::float(1.234_567_9e20));
    assert_eq!(value(1, 2), DataValue::float(6.172_839_5e19));
}

#[test]
fn errored_groups_are_dropped_and_counted() {
    // Group (1) sums a string; group (2) is clean.
    let mut dictionary = Dictionary::new();
    let name = dictionary
        .encode(DataValue::string("oops").as_ref())
        .expect("strings are representable");
    let rows = vec![
        vec![StorageValue::integer(1), name],
        vec![StorageValue::integer(2), StorageValue::integer(5)],
    ];
    let (buffer, errors) = aggregate_relation(
        &rows,
        vec![over_values(AggregateKind::Sum)],
        &mut dictionary,
    );
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.row(0)[0], StorageValue::integer(2));
    assert_eq!(
        (errors.count(EvaluationError::TypeMismatch), errors.total()),
        (1, 1)
    );
}

#[test]
fn nan_poisons_extrema() {
    let rows = vec![
        vec![StorageValue::integer(1), StorageValue::double(1.0)],
        vec![StorageValue::integer(1), StorageValue::double(f64::NAN)],
        vec![StorageValue::integer(1), StorageValue::double(2.0)],
    ];
    let mut dictionary = Dictionary::new();
    let (buffer, _) = aggregate_relation(
        &rows,
        vec![over_values(AggregateKind::Min)],
        &mut dictionary,
    );
    assert_eq!(buffer.row(0)[1], StorageValue::double(f64::NAN));
}

#[test]
fn malformed_aggregations_are_rejected() {
    let count = |input| AggregateShape {
        input: Some(input),
        ..over_values(AggregateKind::Count)
    };
    let rejected = |group_layers, aggregates: &[AggregateShape]| {
        let shape = AggregationShape {
            group_layers,
            aggregates,
        };
        shape
            .validate()
            .and_then(|()| shape.validate_fit(2))
            .is_err()
    };
    assert!(rejected(1, &[]));
    assert!(rejected(3, &[count(0)]));
    assert!(rejected(1, &[count(2)]));

    // Building the aggregator refuses a malformed shape.
    let dictionary = Dictionary::new();
    assert_panics("at least one aggregate", || {
        Aggregator::new(
            AggregationShape {
                group_layers: 1,
                aggregates: &[],
            },
            2,
            &dictionary,
        )
    });
}
