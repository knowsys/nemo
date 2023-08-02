//! Contains structures that represent graphs.

use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::{collections::HashMap, fmt::Debug};

use petgraph::graph::NodeIndex;
use petgraph::{EdgeType, Graph};

/// Graph with labeled nodes.
///
/// Internally, it uses the [`Graph`] implementation from petgraph.
/// Additionally, it maintains a [`HashMap`] which associates
/// each label with a [`NodeIndex`].
///
/// A [`NodeIndex`] is invalidated once a node is removed from the graph,
/// hence the interface only permits adding new nodes.
#[derive(Debug)]
pub struct LabeledGraph<NodeLabel, EdgeLabel, Type>
where
    NodeLabel: Debug + Clone + Eq + PartialEq + Hash,
    EdgeLabel: Clone + Eq + PartialEq,
    Type: EdgeType,
{
    graph: Graph<NodeLabel, EdgeLabel, Type>,
    label_map: HashMap<NodeLabel, NodeIndex>,
}

impl<NodeLabel, EdgeLabel, Type> LabeledGraph<NodeLabel, EdgeLabel, Type>
where
    NodeLabel: Debug + Clone + Eq + PartialEq + Hash,
    EdgeLabel: Clone + Eq + PartialEq,
    Type: EdgeType,
{
    /// Add a single node to the graph under a new label.
    /// Returns the [`NodeIndex`] of the new node.
    pub fn add_node(&mut self, node: NodeLabel) -> NodeIndex {
        match self.label_map.entry(node) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let new_index = self.graph.add_node(entry.key().clone());
                entry.insert(new_index);

                new_index
            }
        }
    }

    /// Return a [`NodeIndex`] for a given node label or `None`
    /// if there is no node in the graph associated with that label.
    pub fn get_node(&self, node: &NodeLabel) -> Option<NodeIndex> {
        self.label_map.get(node).cloned()
    }

    /// Add a new edge to the graph.
    pub fn add_edge(&mut self, from: NodeLabel, to: NodeLabel, edge_label: EdgeLabel) {
        let node_from = self.add_node(from);
        let node_to = self.add_node(to);

        self.graph.add_edge(node_from, node_to, edge_label);
    }

    /// Return a reference to the underlying [`Graph`].
    pub fn graph(&self) -> &Graph<NodeLabel, EdgeLabel, Type> {
        &self.graph
    }

    /// Remove every edge that connects a node to itself.
    /// If the function detects a self cycle with a label contained in `by_edge_labels`
    /// it will abort the computation and return true.
    /// Otherwise it will return false.
    fn remove_self_cycles<T: Clone>(
        graph: &mut Graph<T, EdgeLabel, Type>,
        by_edge_labels: &[EdgeLabel],
    ) -> bool {
        for node in graph.node_indices() {
            for edge in graph.edges_connecting(node, node) {
                if by_edge_labels.contains(edge.weight()) {
                    return true;
                }
            }
        }

        graph.retain_edges(|g, e| {
            let (start, end) = g.edge_endpoints(e).unwrap();
            start != end
        });

        false
    }

    /// Given a map, reverses the keys and values.
    /// Since multiple keys can map to the same value this results in
    /// map from the values to a vector of keys.
    fn reverse_map(map: HashMap<NodeIndex, usize>) -> HashMap<usize, Vec<NodeIndex>> {
        let mut result = HashMap::<usize, Vec<NodeIndex>>::new();

        for (key, value) in map {
            let vec = result.entry(value).or_default();
            vec.push(key);
        }

        result
    }

    /// Divides the nodes of the graph into a series of strata
    /// such that there is no edge of label contained in `by_edge_labels`
    /// from a node in a higher strata to a node in a lower strata.
    pub fn stratify(&self, by_edge_labels: &[EdgeLabel]) -> Option<Vec<Vec<NodeLabel>>> {
        let mut graph_scc = petgraph::algo::condensation(self.graph.clone(), false);
        if Self::remove_self_cycles(&mut graph_scc, by_edge_labels) {
            return None;
        }

        let scc_sorted = petgraph::algo::toposort(&graph_scc, None)
            .expect("Previous call to remove_cycles should have removed all cycles");
        let scc_count = scc_sorted.len();

        graph_scc.reverse();

        let mut scc_to_stratum = HashMap::<NodeIndex, usize>::new();
        for scc in scc_sorted {
            let mut stratum: usize = 0;
            for neighbor in graph_scc.neighbors(scc) {
                for edge in graph_scc.edges_connecting(scc, neighbor) {
                    if scc == neighbor {
                        continue;
                    }

                    if by_edge_labels.contains(edge.weight()) {
                        stratum = stratum.max(
                            1 + *scc_to_stratum
                                .get(&neighbor)
                                .expect("Topolical sorting should ensure that there is an entry."),
                        );
                    } else {
                        stratum = stratum.max(
                            *scc_to_stratum
                                .get(&neighbor)
                                .expect("Topolical sorting should ensure that there is an entry."),
                        );
                    }
                }
            }

            scc_to_stratum.insert(scc, stratum);
        }

        let stratum_to_sccs = Self::reverse_map(scc_to_stratum);

        let mut result = Vec::new();
        for stratum in 0..scc_count {
            if let Some(sccs) = stratum_to_sccs.get(&stratum) {
                result.push(
                    sccs.iter()
                        .flat_map(|&i| graph_scc.node_weight(i).unwrap().clone())
                        .collect(),
                )
            } else {
                break;
            }
        }

        Some(result)
    }
}

impl<NodeLabel, EdgeLabel, Type> Default for LabeledGraph<NodeLabel, EdgeLabel, Type>
where
    NodeLabel: Debug + Clone + Eq + PartialEq + Hash,
    EdgeLabel: Clone + Eq + PartialEq,
    Type: EdgeType,
{
    fn default() -> Self {
        Self {
            graph: Default::default(),
            label_map: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use petgraph::Directed;

    use super::LabeledGraph;

    #[derive(Debug, Eq, PartialEq, Hash, Clone)]
    enum EdgeLabel {
        Positive,
        Negative,
    }

    #[test]
    fn stratification() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");
        let node_c = String::from("C");
        let node_d = String::from("D");

        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Negative);
        graph.add_edge(node_b.clone(), node_c.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_b.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_c.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_d.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_d.clone(), EdgeLabel::Negative);

        let mut stratums = graph.stratify(&[EdgeLabel::Negative]).unwrap();
        for stratum in &mut stratums {
            stratum.sort();
        }

        assert_eq!(
            stratums,
            vec![vec![node_a], vec![node_b, node_c], vec![node_d]]
        );
    }

    #[test]
    fn stratification_2() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");
        let node_c = String::from("C");
        let node_d = String::from("D");

        graph.add_edge(node_a.clone(), node_c.clone(), EdgeLabel::Negative);
        graph.add_edge(node_b.clone(), node_c.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_d.clone(), EdgeLabel::Positive);

        let mut stratums = graph.stratify(&[EdgeLabel::Negative]).unwrap();

        for stratum in &mut stratums {
            stratum.sort();
        }

        assert_eq!(stratums, vec![vec![node_a, node_b], vec![node_c, node_d]]);
    }

    #[test]
    fn not_stratified() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");

        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Negative);
        graph.add_edge(node_b, node_a, EdgeLabel::Positive);

        let stratums = graph.stratify(&[EdgeLabel::Negative]);
        assert!(stratums.is_none());
    }
}
