//! Contains structures that represent graphs.

use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::{collections::HashMap, fmt::Debug};

use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::{EdgeType, Graph};

/// Graph with labeled nodes.
///
/// Internally, it uses the [Graph] implementation from petgraph.
/// Additionally, it maintains a [HashMap] which associates
/// each label with a [NodeIndex].
///
/// A [NodeIndex] is invalidated once a node is removed from the graph,
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

/// An edge label which can be special.
pub trait SpecialEdgeLabel {
    /// Predicate to decide whether an edge is special.
    fn is_special(&self) -> bool;
}

impl<NodeLabel, EdgeLabel, Type> LabeledGraph<NodeLabel, EdgeLabel, Type>
where
    NodeLabel: Debug + Clone + Eq + PartialEq + Hash,
    EdgeLabel: Clone + Eq + PartialEq,
    Type: EdgeType,
{
    /// Add a single node to the graph under a new label.
    /// Returns the [NodeIndex] of the new node.
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

    /// Return a [NodeIndex] for a given node label or `None`
    /// if there is no node in the graph associated with that label.
    pub fn node(&self, label: &NodeLabel) -> Option<NodeIndex> {
        self.label_map.get(label).copied()
    }
    /// Return a [NodeIndex] for a given node label, assuming it is present.
    pub fn node_unchecked(&self, label: &NodeLabel) -> NodeIndex {
        self.node(label).expect("node with label should be present")
    }

    /// Return a [NodeLabel] for a given node or `None`.
    pub fn label(&self, node: NodeIndex) -> Option<&NodeLabel> {
        self.graph.node_weight(node)
    }
    /// Return a [NodeLabel] for a given node, assuming it is present.
    pub fn label_unchecked(&self, node: NodeIndex) -> &NodeLabel {
        self.label(node).expect("every node should have a label")
    }

    /// Add a new edge to the graph.
    pub fn add_edge(&mut self, from: NodeLabel, to: NodeLabel, edge_label: EdgeLabel) {
        let node_from = self.add_node(from);
        let node_to = self.add_node(to);

        self.graph.add_edge(node_from, node_to, edge_label);
    }

    /// Return a reference to the underlying [Graph].
    pub fn graph(&self) -> &Graph<NodeLabel, EdgeLabel, Type> {
        &self.graph
    }

    /// Check whether the graph has a special-edge path from source to destination.
    /// Like `petgraph::has_path_connecting` but only uses special edges.
    pub fn has_path_via_special(&self, src: NodeIndex, dst: NodeIndex) -> bool
    where
        EdgeLabel: SpecialEdgeLabel,
    {
        let mut seen = std::collections::HashSet::new();
        let mut q = std::collections::VecDeque::from([src]);

        while let Some(u) = q.pop_front() {
            if u == dst {
                return true;
            }

            if !seen.insert(u) {
                continue;
            }

            for e in self.graph.edges(u) {
                if e.weight().is_special() {
                    q.push_back(e.target());
                }
            }
        }

        false
    }

    /// Decompose the strongly-connected components of the graph using Tajan's algorithm and refine those with special edges.
    pub fn decompose_and_refine<F, E>(
        &self,
        strata: &mut Vec<Vec<NodeLabel>>,
        tarjan: &mut petgraph::algo::TarjanScc<NodeIndex>,
        mut refine: F,
    ) -> Result<(), E>
    where
        F: FnMut(Vec<NodeLabel>, &mut Vec<Vec<NodeLabel>>) -> Result<(), E>,
        EdgeLabel: SpecialEdgeLabel,
    {
        // identifier for current (sub-)SCC
        let mut stratum_index = 0;
        // prepare hash map of nodes to SCC identifiers
        let mut mark = vec![0usize; self.graph.node_count()];

        let mut result = Ok(());

        tarjan.run(&self.graph, |scc| {
            if result.is_err() {
                return;
            }

            for &n in scc {
                mark[n.index()] = stratum_index;
            }

            // determine whether the current SCC of the graph contains a preference edge
            let has_special_edge = scc.iter().any(|&u| {
                self.graph
                    .edges(u)
                    .any(|e| mark[e.target().index()] == stratum_index && e.weight().is_special())
            });

            stratum_index += 1;

            // translate petgraph NodeIndices of dependency graph back to rule indices
            let scc_labels = scc
                .iter()
                .map(|&a| self.label_unchecked(a).clone())
                .collect();

            if has_special_edge {
                result = refine(scc_labels, strata);
            } else {
                // no special edges, so just add the SCC to the list
                strata.push(scc_labels);
            }
        });

        result
    }

    /// Return an iterator over outgoing edges of a node.
    pub fn edges_outgoing(
        &self,
        start_label: &NodeLabel,
    ) -> impl Iterator<Item = (&EdgeLabel, &NodeLabel)> {
        self.graph
            .edges_directed(
                self.node_unchecked(start_label),
                petgraph::Direction::Outgoing,
            )
            .map(|e| (e.weight(), self.label_unchecked(e.target())))
    }

    /// Find topological layering of the graph if it is acyclic.
    pub fn layer(
        &self,
    ) -> Result<impl Iterator<Item = Vec<NodeLabel>>, petgraph::algo::Cycle<NodeIndex>> {
        // sort the verices of the graph topologically
        let order = petgraph::algo::toposort(&self.graph, None)?;

        // longest-path layering (sources get layer 0, other vertices get maximum layer of a predecessor plus one)
        let mut layer = vec![0usize; self.graph.node_count()];
        for v in order {
            let l = self
                .graph
                .edges_directed(v, petgraph::Direction::Incoming)
                .map(|e| layer[e.source().index()] + 1)
                .max()
                .unwrap_or(0);

            layer[v.index()] = l;
        }

        // form strata based on the layers of the graph
        let mut strata = HashMap::<usize, Vec<_>>::new();
        for (i, &l) in layer.iter().enumerate() {
            let vec = strata.entry(l).or_default();
            vec.push(self.label_unchecked(NodeIndex::new(i)).clone());
        }

        Ok(strata.into_values())
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
