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
pub struct NodeLabeledGraph<Label, Type>
where
    Label: Debug + Clone + Eq + PartialEq + Hash,
    Type: EdgeType,
{
    graph: Graph<Label, (), Type>,
    label_map: HashMap<Label, NodeIndex>,
}

impl<Label, Type> NodeLabeledGraph<Label, Type>
where
    Label: Debug + Clone + Eq + PartialEq + Hash,
    Type: EdgeType,
{
    /// Add a single node to the graph under a new label.
    /// Returns the [`NodeIndex`] of the new node.
    fn add_node(&mut self, node: Label) -> NodeIndex {
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
    pub fn get_node(&self, node: &Label) -> Option<NodeIndex> {
        self.label_map.get(node).cloned()
    }

    /// Add a new edge to the graph.
    pub fn add_edge(&mut self, from: Label, to: Label) {
        let node_from = self.add_node(from);
        let node_to = self.add_node(to);

        self.graph.add_edge(node_from, node_to, ());
    }

    /// Return a reference to the underlying [`Graph`].
    pub fn graph(&self) -> &Graph<Label, (), Type> {
        &self.graph
    }
}

impl<Label, Type> Default for NodeLabeledGraph<Label, Type>
where
    Label: Debug + Clone + Eq + PartialEq + Hash,
    Type: EdgeType,
{
    fn default() -> Self {
        Self {
            graph: Default::default(),
            label_map: Default::default(),
        }
    }
}
