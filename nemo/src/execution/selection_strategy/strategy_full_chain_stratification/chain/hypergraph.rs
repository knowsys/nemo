use std::fmt;
use std::collections::{HashMap, HashSet};

pub(super) use super::atoms::EdgeId;
use super::tuples::Tuples;

pub struct Hypergraph {
    /// tuples per color
    tuples: Vec<Tuples>,
    /// list of incident edges per vertex
    pub incidence: Vec<Vec<EdgeId>>,
}

impl fmt::Display for Hypergraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}",
            self.tuples
                .iter()
                .map(|t| t.iter().collect::<Vec<_>>())
                .collect::<Vec<_>>()
        )
    }
}


impl Hypergraph {
    pub fn new(tuples: Vec<Tuples>, incidence: Vec<Vec<EdgeId>>) -> Self {
        Self { tuples, incidence }
    }

    /// Build from tuples and the total vertex count, computing incidence automatically.
    pub(crate) fn from_tuples(tuples: Vec<Tuples>, vertex_count: usize) -> Self {
        let mut incidence = vec![Vec::new(); vertex_count];
        for (edge_id, vars) in tuples.iter().enumerate().flat_map(|(color, t)| {
            t.iter()
                .enumerate()
                .map(move |(idx, tt)| (EdgeId { color, idx }, tt))
        }) {
            for &v in vars {
                incidence[v].push(edge_id);
            }
        }
        Self::new(tuples, incidence)
    }

    pub fn incidence(&self, v: usize) -> &Vec<EdgeId> {
        &self.incidence[v]
    }

    fn densify(&mut self) {
        let mut n: usize = 0;
        let mut mapping = HashMap::new();
        for rel in &mut self.tuples {
            for var in &mut rel.data {
                *var = *mapping.entry(*var).or_insert_with(|| {
                    let next = n;
                    n += 1;
                    next
                });
            }
        }
        let mut old_incidence = std::mem::take(&mut self.incidence);

        let mut incidence: Vec<Vec<EdgeId>> = (0..n).map(|_| Vec::new()).collect();

        for (from, to) in mapping {
            std::mem::swap(&mut incidence[to], &mut old_incidence[from]);
        }

        self.incidence = incidence;
    }

    fn get_incident_edges(&self, vertices: &HashSet<usize>) -> Vec<Vec<usize>> {
        // For each color, the indices of edges that need to be removed.
        // These are indices into the ORIGINAL relation, so they must remain unchanged until we have finished collecting them.
        let mut deleted: Vec<Vec<usize>> = (0..self.tuples.len()).map(|_| Vec::new()).collect();

        // Find all edges incident to the vertices being deleted.
        for &vertex in vertices {
            for edge in &self.incidence[vertex] {
                deleted[edge.color].push(edge.idx);
            }
        }

        // Sort and deduplicate the edge indices within each color.
        for indices in &mut deleted {
            indices.sort_unstable();
            indices.dedup();
        }

        deleted
    }

    fn get_touched_vertices(&self, edges_per_color: &Vec<Vec<usize>>) -> HashSet<usize> {
        let mut touched_vertices = HashSet::new();
        // Collect the vertices touched by the edges we are going to delete.
        for (color, indices) in edges_per_color.iter().enumerate() {
            for &idx in indices {
                touched_vertices.extend(self.tuples[color][idx].iter().copied());
            }
        }
        touched_vertices
    }

    fn remove_edges(&mut self, deleted: &Vec<Vec<usize>>) -> bool {
        // Remove edges from each relation.
        let mut empty_colors = false;
        for (relation, deleted) in self.tuples.iter_mut().zip(deleted) {
            if deleted.is_empty() {
                continue;
            }

            let mut deleted_pos = 0;

            relation.retain(|old_idx, _| {
                let is_deleted = deleted_pos < deleted.len() && deleted[deleted_pos] == old_idx;

                if is_deleted {
                    deleted_pos += 1;
                }

                !is_deleted
            });

            if relation.is_empty() {
                empty_colors = true;
            }
        }
        empty_colors
    }

    fn remap_incidence(
        &mut self,
        vertices_to_delete: &HashSet<usize>,
        deleted: &Vec<Vec<usize>>,
        touched_vertices: &HashSet<usize>,
    ) {
        // Vertices that themselves are being deleted no longer need incidence information.
        // Every other touched vertex may have EdgeIds whose indices shifted.
        for &vertex in touched_vertices {
            if vertices_to_delete.contains(&vertex) {
                self.incidence[vertex].clear();
                continue;
            }

            let mut deleted_pos = vec![0usize; self.tuples.len()]; // per color

            self.incidence[vertex].retain_mut(|edge_id| {
                let deleted_indices = &deleted[edge_id.color];
                let pos = &mut deleted_pos[edge_id.color];

                while *pos < deleted_indices.len() && deleted_indices[*pos] < edge_id.idx {
                    *pos += 1;
                }

                if *pos < deleted_indices.len() && deleted_indices[*pos] == edge_id.idx {
                    *pos += 1; // This edge itself was deleted.
                    false
                } else {
                    edge_id.idx -= *pos; // *pos deleted edges occurred before this edge.
                    true
                }
            });
        }
    }

    /// Clean up empty relations
    fn remove_empty_colors(&mut self) -> Vec<usize> {
        let mut color_map = vec![0; self.tuples.len()];
        let mut new_color = 0;

        for (old_color, relation) in self.tuples.iter().enumerate() {
            if !relation.data.is_empty() {
                color_map[old_color] = new_color;
                new_color += 1;
            }
        }

        self.tuples.retain(|relation| !relation.data.is_empty());

        for incidence in &mut self.incidence {
            for edge_id in incidence {
                edge_id.color = color_map[edge_id.color];
            }
        }

        color_map
    }

    pub fn remove_vertices(&mut self, vertices_to_delete: &HashSet<usize>) -> Option<Vec<usize>> {
        if vertices_to_delete.is_empty() {
            return None;
        }

        let deleted = self.get_incident_edges(vertices_to_delete);
        let touched_vertices = self.get_touched_vertices(&deleted);

        let empty_colors = self.remove_edges(&deleted);

        self.remap_incidence(vertices_to_delete, &deleted, &touched_vertices);

        let color_map = if empty_colors {
            Some(self.remove_empty_colors())
        } else {
            None
        };

        self.densify();

        color_map
    }

    pub fn get_edge(&self, edge_id: &EdgeId) -> &[usize] {
        &self.tuples[edge_id.color][edge_id.idx]
    }

    fn get_edge_mut(&mut self, edge_id: &EdgeId) -> &mut [usize] {
        &mut self.tuples[edge_id.color][edge_id.idx]
    }

    pub fn vertex_count(&self) -> usize {
        self.incidence.len()
    }

    pub fn edge_count(&self) -> usize {
        self.tuples.iter().map(Tuples::len).sum()
    }

    pub fn tuples(&self) -> &Vec<Tuples> {
        &self.tuples
    }
}
