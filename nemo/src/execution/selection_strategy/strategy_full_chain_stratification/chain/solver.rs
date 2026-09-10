use std::collections::VecDeque;

use super::bitset::BitSet;
use super::constraint::Constraint;
use super::domain::Domain;
use super::hypergraph::Hypergraph;
use super::relation::Relation;
use super::trail::{Trail, TrailEntry, TrailTarget};

pub struct Solver<'a> {
    /// Domains of source vertices.
    pub domains: Box<[Domain]>,

    /// One relation per edge color.
    pub relations: Box<[Relation]>,

    /// One constraint per edge of H.
    pub constraints: Box<[Constraint<'a>]>,

    /// incident[v] lists all constraints containing vertex v.
    pub incident: Box<[Vec<usize>]>,

    //pub source: &'a Hypergraph,
    scratch: BitSet,

    pub(crate) queued: Box<[bool]>,
    pub(crate) queue: VecDeque<usize>,

    pub(super) trail: Trail,
}

impl<'a> Solver<'a> {
    pub fn from_hypergraphs(h: &'a Hypergraph, g: &Hypergraph) -> Self {
        let target_vertices = g.vertex_count();

        let relations: Box<[_]> = g
            .tuples()
            .iter()
            .map(|tuples| Relation::from_flat_tuples(tuples, target_vertices))
            .collect();

        let variable_count = h.vertex_count();

        let domains = (0..variable_count)
            .map(|_| Domain::Unassigned(BitSet::filled(target_vertices)))
            .collect();

        let mut color_offsets = vec![0; relations.len()];

        let constraints: Box<[_]> = h
            .tuples()
            .iter()
            .enumerate()
            .flat_map(|(color, color_edges)| {
                if color + 1 < color_offsets.len() {
                    color_offsets[color + 1] = color_offsets[color] + color_edges.len();
                }
                let tuple_count = relations[color].tuple_count;
                color_edges.iter().map(move |vars| Constraint {
                    relation: color,
                    vars,
                    alive: BitSet::filled(tuple_count),
                })
            })
            .collect();

        let incident = h
            .incidence
            .iter()
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .map(|edge_id| color_offsets[edge_id.color] + edge_id.idx)
                    .collect()
            })
            .collect();

        Self {
            domains,
            incident,
            queue: VecDeque::from_iter(0..constraints.len()),
            queued: vec![true; constraints.len()].into_boxed_slice(),
            constraints,
            scratch: BitSet::new(relations.iter().map(|r| r.tuple_count).max().unwrap_or(0)),
            relations,
            trail: Trail {
                entries: Vec::new(),
                levels: Vec::new(),
            },
        }
    }

    /// Returns:
    /// - Ok(true): propagation succeeded and changed at least one domain
    /// - Ok(false): propagation succeeded and changed nothing
    /// - Err(()): contradiction (some domain became empty)
    fn propagate_constraint(&mut self, cid: usize) -> Result<bool, ()> {
        let mut changed = false;

        let constraint = &mut self.constraints[cid];

        let relation_id = constraint.relation;

        let relation = &self.relations[relation_id];
        let vars = &constraint.vars;

        /*
         * Phase 1:
         *
         * Remove tuples that are incompatible with the current domains.
         *
         * For each local variable position:
         *
         * allowed =
         *     union of supports[local_position][possible_value]
         *
         * Then:
         *
         * alive &= allowed
         */

        for local_var in 0..relation.arity {
            let global_var = vars[local_var];

            self.scratch.clear();

            match &self.domains[global_var] {
                Domain::Unassigned(bitset) => {
                    for value in bitset.iter_ones() {
                        self.scratch
                            .union_with(&relation.supports[local_var][value]);
                    }
                }
                Domain::Assigned(value) => {
                    self.scratch
                        .union_with(&relation.supports[local_var][*value]);
                }
            }

            changed |= constraint.alive.intersect_with_trail(
                &self.scratch,
                TrailTarget::Constraint(cid),
                &mut self.trail,
            );

            if constraint.alive.is_empty() {
                return Err(());
            }
        }

        /*
         * Phase 2:
         *
         * Remove domain values that have no supporting tuple left.
         */

        for local_var in 0..relation.arity {
            let global_var = vars[local_var];

            match &mut self.domains[global_var] {
                Domain::Unassigned(bitset) => {
                    changed |= bitset.remove_if_trail(
                        |value| {
                            !constraint
                                .alive
                                .intersects(&relation.supports[local_var][value])
                        },
                        TrailTarget::Domain(global_var),
                        &mut self.trail,
                    );

                    if bitset.is_empty() {
                        return Err(());
                    }
                }
                Domain::Assigned(value) => {
                    if !constraint
                        .alive
                        .intersects(&relation.supports[local_var][*value])
                    {
                        return Err(());
                    }
                }
            }
        }

        Ok(changed)
    }

    fn enqueue_incident(&mut self, cid: usize) {
        let vars = self.constraints[cid].vars;
        let incident = &self.incident;

        let queued = &mut self.queued;
        let queue = &mut self.queue;

        for &var in vars {
            for &other_cid in &incident[var] {
                // important: a constraint can also invalidate itself (by shrinking domains), so can't guard this with other_cid != cid
                Self::enqueue(queued, queue, other_cid);
            }
        }
    }

    pub(crate) fn enqueue(queued: &mut [bool], queue: &mut VecDeque<usize>, cid: usize) {
        if !queued[cid] {
            queued[cid] = true;
            queue.push_back(cid);
        }
    }

    pub fn propagate_all(&mut self) -> Result<(), ()> {
        while let Some(cid) = self.queue.pop_front() {
            self.queued[cid] = false;

            let changed = self.propagate_constraint(cid)?;

            if changed {
                self.enqueue_incident(cid);
            }
        }

        Ok(())
    }

    pub fn solved(&self) -> bool {
        self.domains.iter().all(|d| d.count_ones() == 1)
    }

    pub(super) fn choose_variable_aux<'b>(
        &self,
        it: impl Iterator<Item = (usize, &'b Domain)>,
    ) -> Option<usize> {
        it.filter(|(_, d)| d.count_ones() > 1)
            .min_by_key(|(var, d)| {
                let tightest = self.incident[*var]
                    .iter()
                    .map(|&cid| self.constraints[cid].alive.count_ones())
                    .min()
                    .unwrap_or(usize::MAX);
                (
                    d.count_ones(),                         // minimize domain size
                    tightest,                               // prefer tighter constraints
                    usize::MAX - self.incident[*var].len(), // maximize degree
                )
            })
            .map(|(v, _)| v)
    }

    // branching heuristic: minimum remaining values (MRV)
    pub(super) fn choose_variable(&self) -> Option<usize> {
        self.choose_variable_aux(self.domains.iter().enumerate())
    }

    pub(crate) fn assign(&mut self, var: usize, value: usize) {
        self.trail.entries.push(TrailEntry::Domain {
            var,
            old: std::mem::replace(&mut self.domains[var], Domain::Assigned(value))
                .as_bitset()
                .expect("cannot reassign a variable"),
        });

        for &cid in &self.incident[var] {
            Self::enqueue(&mut self.queued, &mut self.queue, cid);
        }
    }

    // assumes that no domain is empty (should really only be called if solve() gave true)
    pub fn solution(&self) -> Vec<usize> {
        self.domains
            .iter()
            .map(|d| d.first_set().unwrap())
            .collect()
    }

    pub(super) fn undo(&mut self) {
        let marker = self.trail.levels.pop().expect("no levels left in undo");

        while self.trail.entries.len() > marker {
            let entry = self.trail.entries.pop().unwrap();

            match entry {
                TrailEntry::Word { target, word, old } => match target {
                    TrailTarget::Domain(var) => {
                        self.domains[var]
                            .as_bitset_mut()
                            .expect("cannot restore word on assigned domain")
                            .restore_word(word, old);
                    }
                    TrailTarget::Constraint(cid) => {
                        self.constraints[cid].alive.restore_word(word, old);
                    }
                },

                TrailEntry::Domain { var, old } => {
                    self.domains[var] = Domain::Unassigned(old);
                }
            }
        }

        self.queue.clear();
        self.queued.fill(false);
    }

    // MAC solver (MAC: Maintaining Arc Consistency)
    pub fn solve(&mut self) -> bool {
        if self.propagate_all().is_err() {
            return false;
        }

        if self.solved() {
            return true;
        }

        let Some(var) = self.choose_variable() else {
            return false;
        };

        for value in self.domains[var]
            .as_bitset_ref()
            .expect("chosen variable must be unassigned")
            .iter_ones_snapshot()
        {
            self.trail.push_level();

            self.assign(var, value);

            if self.solve() {
                return true;
            }

            self.undo();
        }

        false
    }
}
