use std::collections::{HashMap, HashSet};

use super::hypergraph::Hypergraph;
use super::atoms::{Predicate, Constant};
use super::core_solver::CoreSolver;
use super::tuples::Tuples;

pub struct ChainRuleBody {
    graph: Hypergraph,
    /// maps predicates to colors
    preds: HashMap<Predicate, usize>,
    /// maps some variables to constants
    consts: HashMap<usize, Constant>,
}

impl ChainRuleBody {
    fn remove_vertices(&mut self, vertices_to_delete: &HashSet<usize>) {
        if let Some(color_map) = self.graph.remove_vertices(vertices_to_delete) {
            // and remap preds accordingly
            for v in self.preds.values_mut() {
                *v = color_map[*v];
            }
        }
    }

    fn truncate(&mut self, dist: usize, head_vars: &HashSet<usize>) {
        let mut keep = head_vars.clone();
        let mut last = keep.iter().copied().collect();
        for _ in 0..dist {
            let mut next = Vec::new();
            for k in last {
                for e in self.graph.incidence(k) {
                    for &v in self.graph.get_edge(e) {
                        if keep.insert(v) {
                            next.push(v);
                        }
                    }
                }
            }
            last = next;
        }
        // Delete all vertices that are not in keep.
        self.remove_vertices(
            &(0..self.graph.vertex_count())
                .filter(|x| !keep.contains(x))
                .collect(),
        );
    }

    fn core(&mut self) {
        let mut solver = CoreSolver::from_hypergraph(&self.graph);
        loop {
            assert!(
                solver.solve_retraction(),
                "should always have identity retraction"
            );

            let collapsed_nodes = solver.collapsed_nodes();
            if collapsed_nodes.is_empty() {
                break;
            }

            solver.apply_retraction(&collapsed_nodes);
        }
        self.remove_vertices(&invert(
            solver.alive_vertices.iter_ones(),
            self.graph.vertex_count(),
        ));
    }
}

fn invert<I>(iter: I, max: usize) -> HashSet<usize>
where
    I: IntoIterator<Item = usize>,
{
    let mut result = HashSet::new();
    let mut next = 0;

    for x in iter {
        result.extend(next..x);
        next = x + 1;
    }

    result.extend(next..max);
    result
}

pub struct ChainRuleHead {
    tuples: Vec<Tuples>,
}

impl ChainRuleHead {
    fn vars(&self) -> HashSet<usize> {
        self.tuples
            .iter()
            .flat_map(|t| t.data.iter().copied())
            .collect()
    }
}

pub struct ChainRule {
    body: ChainRuleBody,
    head: ChainRuleHead,
}

static EDGE_COUNT_THRESHOLD: usize = 5;

impl ChainRule {
    fn canonize(&mut self, dist: usize) {
        if self.body.graph.edge_count() > EDGE_COUNT_THRESHOLD {
            self.body.truncate(dist, &self.head.vars());
            self.body.core();
        }
    }
}
