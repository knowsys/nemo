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

/// Returns the join of a shape over the given tries, each the only block of its input,
/// which the shape must fit.
fn join<'a>(
    shape: &'a OwnedJoinShape,
    tries: &[&'a Trie],
    subtracted: &[&'a Trie],
    dictionary: Option<&'a Dictionary>,
    arena: &'a JoinArena,
) -> TrieJoin<'a> {
    join_blocks(shape, &one_block_each(tries), subtracted, dictionary, arena)
}

/// Returns the join of a shape over the blocks of the given inputs, which the shape must fit.
fn join_blocks<'a>(
    shape: &'a OwnedJoinShape,
    inputs: &[&[&'a Trie]],
    subtracted: &[&'a Trie],
    dictionary: Option<&'a Dictionary>,
    arena: &'a JoinArena,
) -> TrieJoin<'a> {
    TrieJoin::new(
        shape.view(),
        inputs,
        subtracted,
        dictionary,
        EvaluationContext::now(),
        arena,
    )
}

/// Returns the given tries as inputs of one block each.
fn one_block_each<'t, 'a>(tries: &'t [&'a Trie]) -> Vec<&'t [&'a Trie]> {
    tries.iter().map(std::slice::from_ref).collect()
}

/// Returns the chain shape R(a, b) ⋈ S(b, c), emitting the given number of layers.
fn chain_shape(emitted: usize, subtracted: Vec<SubtractedShape<'static>>) -> OwnedJoinShape {
    chain_shape_with(emitted, subtracted, vec![])
}

/// Returns the chain shape with the given filters.
fn chain_shape_with(
    emitted: usize,
    subtracted: Vec<SubtractedShape<'static>>,
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

/// A sink that collects the rows of a join,
/// and checks each row's changed layer against the previous row.
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
/// and checks the changed layer of each row against the rows.
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

/// A chain join emits every combination of matching rows, in storage order.
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

/// A triangle join over the complete graph on four nodes finds its four triangles.
#[test]
fn triangle_joins_find_every_triangle() {
    let arena = JoinArena::new();
    let edges = complete_graph();
    let shape = triangle_shape();

    let mut join = join(&shape, &[&edges, &edges, &edges], &[], None, &arena);
    assert_eq!(collect(&mut join).len(), 4);
}

/// Empty blocks add no rows,
/// and an input without blocks, or with only empty ones, leaves the join without rows.
#[test]
fn inputs_without_rows_leave_no_rows() {
    let arena = JoinArena::new();
    let left = int_trie(2, &[&[1, 2]]);
    let right = int_trie(2, &[&[2, 7]]);
    let empty = Trie::empty(2);
    let shape = chain_shape(3, vec![]);

    // Empty blocks add nothing to an input with rows.
    let mut padded = join_blocks(
        &shape,
        &[&[&empty, &left], &[&right, &empty]],
        &[],
        None,
        &arena,
    );
    assert_eq!(collect(&mut padded), storage_rows(&[&[1, 2, 7]]));
    // An input of no blocks, or of empty ones, has no rows.
    let mut blockless = join_blocks(&shape, &[&[&left], &[]], &[], None, &arena);
    assert!(collect(&mut blockless).is_empty());
    let mut emptied = join_blocks(&shape, &[&[&left], &[&empty, &empty]], &[], None, &arena);
    assert!(collect(&mut emptied).is_empty());
}

/// Splits rows into one to three blocks at random, putting some rows into two of them,
/// and returns the tries of the blocks.
fn random_blocks(rng: &mut StdRng, arity: usize, rows: &[Vec<StorageValue>]) -> Vec<Trie> {
    let count = rng.gen_range(1..=3);
    let mut blocks = vec![Vec::new(); count];
    for row in rows {
        blocks[rng.gen_range(0..count)].push(row.clone());
        if rng.gen_ratio(1, 4) {
            blocks[rng.gen_range(0..count)].push(row.clone());
        }
    }

    blocks.iter().map(|rows| trie(arity, rows)).collect()
}

/// A join over inputs split into blocks at random yields the rows of the join
/// over each input in one trie, for data that exercises all three join cores,
/// and with every hook that reads the inputs' columns.
#[test]
fn joins_over_blocks_match_joins_over_their_union() {
    let mut rng = StdRng::seed_from_u64(37);
    let dictionary = Dictionary::new();
    for round in 0..300 {
        let mode = (round % 3) as u8;
        let left_rows = random_rows(&mut rng, 2, mode);
        let right_rows = random_rows(&mut rng, 2, mode);
        let (left, right) = (trie(2, &left_rows), trie(2, &right_rows));
        let minus = trie(2, &random_rows(&mut rng, 2, mode));
        let left_blocks = random_blocks(&mut rng, 2, &left_rows);
        let right_blocks = random_blocks(&mut rng, 2, &right_rows);
        let left_parts: Vec<&Trie> = left_blocks.iter().collect();
        let right_parts: Vec<&Trie> = right_blocks.iter().collect();

        let subtraction = vec![SubtractedShape { layers: &[0, 2] }];
        let mut shapes = vec![
            chain_shape(3, vec![]),
            chain_shape(2, vec![]),
            chain_shape(1, vec![]),
            chain_shape(3, subtraction),
        ];
        // Programs over integers only, since in the third mode every value is an integer.
        if mode == 2 {
            let condition = hook(
                vec![2, 0],
                vec![OpCode::Input(0), OpCode::Input(1), OpCode::Less],
                vec![],
            );
            shapes.push(chain_shape_with(3, vec![], vec![condition]));
            let bound = BoundLayerShape::constant(1, DataValue::integer(1));
            shapes.push(
                chain_shape(3, vec![])
                    .with_bound_layers(vec![bound])
                    .expect("well-formed"),
            );
        }

        for shape in &shapes {
            let anti = if shape.view().subtracted.is_empty() {
                vec![]
            } else {
                vec![&minus]
            };
            let arena = JoinArena::new();
            let mut whole = join(shape, &[&left, &right], &anti, Some(&dictionary), &arena);
            let expected = collect(&mut whole);
            let inputs: [&[&Trie]; 2] = [&left_parts, &right_parts];
            let mut split = join_blocks(shape, &inputs, &anti, Some(&dictionary), &arena);
            assert_eq!(collect(&mut split), expected, "round {round}, {shape:?}");
            if mode == 2 {
                check_cells(shape, &inputs, &anti, Some(&dictionary), &expected);
            }
        }
    }
}

/// A triangle join over an input split into blocks yields the rows of the join
/// over the input in one trie.
#[test]
fn triangles_over_blocks_match_triangles_over_their_union() {
    // The triangle reads its input three times, and links layer 0 to layer 2 for the third.
    // So a descent that skips a layer must restrict every block.
    let mut rng = StdRng::seed_from_u64(41);
    let shape = triangle_shape();
    for round in 0..200 {
        let edges: Vec<Vec<StorageValue>> = (0..rng.gen_range(0..30))
            .map(|_| {
                vec![
                    StorageValue::term(rng.gen_range(0..6)),
                    StorageValue::term(rng.gen_range(0..6)),
                ]
            })
            .collect();
        let whole = trie(2, &edges);
        let blocks = random_blocks(&mut rng, 2, &edges);
        let parts: Vec<&Trie> = blocks.iter().collect();

        let arena = JoinArena::new();
        let mut expected = join(&shape, &[&whole, &whole, &whole], &[], None, &arena);
        let mut split = join_blocks(&shape, &[&parts, &parts, &parts], &[], None, &arena);
        assert_eq!(collect(&mut split), collect(&mut expected), "round {round}");
    }
}

/// A join column may mix terms and integers.
#[test]
fn join_columns_may_mix_tags() {
    let arena = JoinArena::new();
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

/// A join that does not emit its last layer yields each emitted row once,
/// however many matches the row has.
#[test]
fn existence_suffixes_deduplicate() {
    let arena = JoinArena::new();
    // R(a, b), with three matching c in S for (1, 2).
    let left = int_trie(2, &[&[1, 2], &[1, 5]]);
    let right = int_trie(2, &[&[2, 7], &[2, 8], &[2, 9]]);
    let shape = chain_shape(2, vec![]);
    let mut join = join(&shape, &[&left, &right], &[], None, &arena);

    // (1, 5) has no match in S, so the existence check drops it.
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 2]]));
}

/// A join that emits no layer yields the empty row if it has a match, and no row otherwise.
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

/// A subtracted input drops the rows that agree with one of its rows.
#[test]
fn subtracted_inputs_block_matching_rows() {
    let arena = JoinArena::new();
    // R(a, b) minus T(b): T maps only the second layer.
    let relation = int_trie(2, &[&[1, 2], &[1, 3], &[2, 2], &[2, 4]]);
    let blocked = int_trie(1, &[&[2], &[4]]);
    let shape = OwnedJoinShape::new(
        vec![vec![at(0, 0)], vec![at(0, 1)]],
        2,
        vec![SubtractedShape { layers: &[1] }],
        vec![],
    )
    .expect("the shape is well-formed");

    let mut join = join(&shape, &[&relation], &[&blocked], None, &arena);
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 3]]));
}

/// A subtracted input whose columns map to layers 0 and 2 drops exactly the rows it matches.
#[test]
fn subtracted_inputs_follow_across_gaps() {
    let arena = JoinArena::new();
    // R(a, b) ⋈ S(b, c) minus U(a, c). U's columns map to layers 0 and 2,
    // so its second column is armed across the middle layer, and across backtracking.
    let left = int_trie(2, &[&[1, 2], &[2, 2]]);
    let right = int_trie(2, &[&[2, 7], &[2, 8]]);
    let minus = int_trie(2, &[&[1, 7], &[2, 8]]);
    let shape = chain_shape(3, vec![SubtractedShape { layers: &[0, 2] }]);

    let mut join = join(&shape, &[&left, &right], &[&minus], None, &arena);
    assert_eq!(collect(&mut join), storage_rows(&[&[1, 2, 8], &[2, 2, 7]]));
}

/// The rows a join drains into a buffer are sorted and distinct,
/// so a trie is built from them directly.
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

/// A shape that does not fit the given tries fails the fit check,
/// and building a join over them panics.
#[test]
fn shapes_must_fit_the_tries() {
    let unary = int_trie(1, &[&[1]]);
    let misfit = |shape: &OwnedJoinShape, tries: &[&Trie], subtracted: &[&Trie]| {
        shape
            .view()
            .validate_fit(&one_block_each(tries), subtracted, None)
            .is_err()
    };

    // The chain shape needs two binary tries, and building the join panics on a misfit.
    let shape = chain_shape(3, vec![]);
    assert!(misfit(&shape, &[&unary, &unary], &[]));
    assert!(misfit(&shape, &[&unary], &[]));
    let arena = JoinArena::new();
    assert_panics("more columns", || {
        TrieJoin::new(
            shape.view(),
            &[&[&unary]],
            &[],
            None,
            EvaluationContext::now(),
            &arena,
        )
    });

    // A join needs one subtracted trie per subtracted input,
    // each with at least the columns its input maps.
    let with_subtraction = chain_shape(3, vec![SubtractedShape { layers: &[0, 2] }]);
    let binary = int_trie(2, &[&[1, 2]]);
    assert!(misfit(&with_subtraction, &[&binary, &binary], &[]));
    assert!(misfit(&with_subtraction, &[&binary, &binary], &[&unary]));
    assert!(misfit(&shape, &[&binary, &binary], &[&binary]));
}

/// Returns a random value: mode 0 mixes terms and integers,
/// mode 1 draws terms only (32-bit columns),
/// and mode 2 draws integers only (64-bit columns).
fn random_value(rng: &mut StdRng, mode: u8) -> StorageValue {
    match mode {
        0 if rng.r#gen::<bool>() => StorageValue::term(rng.gen_range(0..4)),
        0 => StorageValue::integer(rng.gen_range(-2..2)),
        1 => StorageValue::term(rng.gen_range(0..6)),
        _ => StorageValue::integer(rng.gen_range(-3..3)),
    }
}

/// Returns fewer than 20 random rows of values drawn in the given mode.
fn random_rows(rng: &mut StdRng, arity: usize, mode: u8) -> Vec<Vec<StorageValue>> {
    let count = rng.gen_range(0..20);
    (0..count)
        .map(|_| (0..arity).map(|_| random_value(rng, mode)).collect())
        .collect()
}

/// Chain joins that emit every layer, or only the first two,
/// and with a subtraction over layers 0 and 2,
/// yield the rows of nested-loop evaluation, for data that exercises all three join cores.
#[test]
fn random_joins_match_brute_force() {
    let arena = JoinArena::new();
    let mut rng = StdRng::seed_from_u64(11);
    for round in 0..300 {
        let mode = (round % 3) as u8;
        let left_rows = random_rows(&mut rng, 2, mode);
        let right_rows = random_rows(&mut rng, 2, mode);
        let minus_rows = random_rows(&mut rng, 2, mode);

        let left = trie(2, &left_rows);
        let right = trie(2, &right_rows);
        let minus = trie(2, &minus_rows);

        let subtraction = vec![SubtractedShape { layers: &[0, 2] }];
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
/// with layer 0 split at 0 and layer 1 split at -1 and 1,
/// and checks that the rows of every cell are sorted,
/// that the cells are disjoint,
/// and that together they are the unpartitioned result.
fn check_cells(
    shape: &OwnedJoinShape,
    inputs: &[&[&Trie]],
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
            let mut join = join_blocks(shape, inputs, subtracted, dictionary, &arena);
            join.clip_layer(0, low0, high0);
            if shape.view().emitted > 1 {
                join.clip_layer(1, low1, high1);
            } else if low1.is_some() {
                // A layer that is not emitted is not split,
                // since the other second-layer cells would repeat rows.
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

/// The cells of a join yield sorted, disjoint rows that together are the join's rows,
/// with and without hooks.
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
        let subtraction = vec![SubtractedShape { layers: &[0, 2] }];
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
                &[&[&left], &[&right]],
                &anti,
                Some(&dictionary),
                &expected,
            );
        }
    }
}

/// Cells partition a join over 32-bit layers, and a join with a computed bound layer.
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

    // A bound layer is probed rather than enumerated, and its join must split into cells
    // like any other. Here c = a + 1 over R(a, b) ⋈ S(b, c), in cells clipped on a and b.
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
    check_cells(
        &shape,
        &[&[&left], &[&right]],
        &[],
        Some(&dictionary),
        &expected,
    );
}

/// A unary join past the parallel threshold is split on its only layer,
/// and the rows of its cells, in cell order, are the rows of the whole join.
#[test]
fn split_joins_deliver_the_sequential_rows() {
    let shape = OwnedJoinShape::new(vec![vec![at(0, 0)]], 1, vec![], vec![])
        .expect("the shape is well-formed");
    let small = int_trie(1, &[[1], [2]]);
    assert!(
        Cells::partition(&shape.view(), &[&[&small]]).is_none(),
        "little work runs whole"
    );

    let relation = int_trie(1, &(0..CELL_ROWS).map(|value| [value]).collect::<Vec<_>>());
    let inputs: [&[&Trie]; 1] = [&[&relation]];
    let Some(cells) = Cells::partition(&shape.view(), &inputs) else {
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
    let (delivered, errors) = cells.run(template, &inputs, collect);
    assert!(delivered.len() > 1);
    assert!(errors.is_empty());

    let arena = JoinArena::new();
    let mut whole = join_blocks(&shape, &inputs, &[], None, &arena);
    assert_eq!(delivered.concat(), collect(&mut whole));
}

/// A row stage extends each row by its computed columns,
/// and drops the rows its post-filter rejects.
#[test]
fn row_stages_extend_and_filter_drained_rows() {
    // Over R(a, b) ⋈ S(b, c), s = a + c, and d = a * 10, which goes stale only when a changes.
    // The post-filter is s > 9, and a result row delivers d, then s.
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

/// A filter over integer columns runs its integer form, without errors.
#[test]
fn filters_prune_rows_on_the_integer_fast_path() {
    let arena = JoinArena::new();
    // R(a, b) ⋈ S(b, c), keeping the rows with b != 3.
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

/// A filter decided by its identity form keeps the rows the interpreter keeps.
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

    // Over R(a, b) ⋈ S(b, c), the conditions a = c, a != c, a = <a>, and a != <z>,
    // where the dictionary does not have <z>.
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
            // A double negation hides the identity form,
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

/// A layer bound to a constant keeps the rows of the equivalent filter, also in cells.
#[test]
fn constant_bound_layers_agree_with_identity_filters() {
    // Integer data keeps every layer comparable with the clip bounds of the cells.
    let mut rng = StdRng::seed_from_u64(31);
    let dictionary = Dictionary::new();
    for round in 0..50 {
        let left = trie(2, &random_rows(&mut rng, 2, 2));
        let right = trie(2, &random_rows(&mut rng, 2, 2));
        let tries = [&left, &right];

        // Each layer of R(a, b) ⋈ S(b, c), emitted or not, is bound to a constant
        // that the data may contain, and to one that the dictionary does not have.
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
                    check_cells(
                        &bound,
                        &one_block_each(&tries),
                        &[],
                        Some(&dictionary),
                        &expected,
                    );
                }
            }
        }
    }
}

/// A filter over strings decodes them through the dictionary, without the integer form.
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

/// A filter that fails to evaluate drops the row, and the error is counted.
#[test]
fn filter_errors_are_counted_not_swallowed() {
    let arena = JoinArena::new();
    // Comparing an integer with a string fails for every candidate.
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

/// Building a join with a filter but without a dictionary panics.
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
            &[&[&relation]],
            &[],
            None,
            EvaluationContext::now(),
            &arena,
        )
    });
}

/// A hand-built shape whose filter reads no layer fails validation.
#[test]
fn hand_built_shapes_are_validated() {
    // The problem must come back from validation, not as a panic inside the driver.
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

/// A chain join with the condition c < a yields the rows of nested-loop evaluation,
/// without errors.
#[test]
fn random_filtered_joins_match_brute_force() {
    let arena = JoinArena::new();
    // With integer data, the storage order that the nested loops compare by
    // agrees with the value order of the condition.
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

/// Returns the aggregate of a function over layer 1, without `per` layers.
fn over_values(kind: AggregateKind) -> AggregateShape {
    AggregateShape {
        kind,
        input: Some(1),
        pair: None,
        per: vec![],
    }
}

/// Each aggregate reads each of its own identities once per group.
#[test]
fn aggregates_range_over_their_own_identities() {
    // One relation (g, a, b). The count of a ignores rows that differ only in b,
    // and the count of b those that differ only in a, while the sum of a per b reads every row.
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

/// Several aggregates are computed per group in one pass.
#[test]
fn aggregates_run_in_one_group_pass() {
    // Group 1 has the values 2 and 4, and group 2 the value 10.
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

/// The average of integers may be a decimal.
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

/// Minimum and maximum compare values in value order, not in storage order.
#[test]
fn extrema_use_value_order_never_storage_order() {
    // The minimum of the integer 100 and the double 1.0 is the double,
    // although integers sort before doubles in storage order.
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

/// Minimum and maximum of strings compare their text, not their order in the dictionary.
#[test]
fn extrema_over_strings_compare_content() {
    let mut dictionary = Dictionary::new();
    let encode = |dictionary: &mut Dictionary, name: &str| {
        dictionary
            .encode(DataValue::string(name).as_ref())
            .expect("strings are representable")
    };
    // The order of insertion into the dictionary differs from the lexicographic order.
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

/// A sum beyond `i64` becomes a wide integer, without an error.
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

/// Sums and averages over wide numbers, and over decimals that outgrow the `i128` workspace,
/// are exact.
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
        // A wide operand in the sum.
        vec![StorageValue::integer(1), big("123456789012345678901")],
        vec![StorageValue::integer(1), StorageValue::integer(1)],
        vec![
            StorageValue::integer(1),
            big("0.0000000000000000000012345678901234567890123"),
        ],
        // Aligning far-apart decimals outgrows the i128 workspace.
        vec![StorageValue::integer(2), decimal(1, -1)],
        vec![StorageValue::integer(2), decimal(1, -40)],
        // An average divides a wide sum.
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

/// A float or a double beside a wide number makes the group's sum and average
/// approximate, in that floating-point type, as beside any exact number.
#[test]
fn floats_promote_big_sums() {
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

/// A group whose aggregate fails yields no row, and the error is counted.
#[test]
fn errored_groups_are_dropped_and_counted() {
    // Group 1 sums a string, and group 2 sums an integer.
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

/// A NaN among the values makes the minimum NaN.
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

/// A malformed aggregation shape fails validation, and building an aggregator over it panics.
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

    // Building the aggregator panics on a malformed shape.
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
