//! Contains structures that represent graphs.

use std::{
    collections::{BTreeSet, HashMap, hash_map::Entry},
    fmt::Debug,
    hash::Hash,
};

use petgraph::{
    EdgeType, Graph,
    graph::NodeIndex,
    visit::{EdgeFiltered, EdgeRef},
};

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
    #[allow(dead_code)]
    pub fn get_node(&self, node: &NodeLabel) -> Option<NodeIndex> {
        self.label_map.get(node).cloned()
    }

    /// Add a new edge to the graph.
    pub fn add_edge(&mut self, from: NodeLabel, to: NodeLabel, edge_label: EdgeLabel) {
        let node_from = self.add_node(from);
        let node_to = self.add_node(to);

        self.graph.add_edge(node_from, node_to, edge_label);
    }

    /// Return a reference to the underlying [Graph].
    #[allow(dead_code)]
    pub fn graph(&self) -> &Graph<NodeLabel, EdgeLabel, Type> {
        &self.graph
    }

    /// Divides the nodes of the graph into its strongly connected components
    /// and returns them in topological order,
    /// meaning that every component is preceded by the components it depends on.
    ///
    /// We distinguish between strong and weak edges.
    /// Strong edges must be respected, while weak edges merely express a preference.
    ///
    /// Components that do not depend on each other are ordered by the node
    /// that was added to the graph first.
    ///
    /// Returns `None` if some component contains an edge
    /// with a label contained in `strong_edge_labels`.
    pub fn topological_components(
        &self,
        strong_edge_labels: &[EdgeLabel],
        weak_edge_labels: &[EdgeLabel],
    ) -> Option<Vec<Vec<NodeLabel>>> {
        // Nodes only belong to the same component
        // if they lie on a cycle that does not use a weak edge
        let strong_graph = EdgeFiltered::from_fn(&self.graph, |edge| {
            !weak_edge_labels.contains(edge.weight())
        });
        let components = petgraph::algo::tarjan_scc(&strong_graph);

        // Assign each node to its component index
        let mut node_to_component = vec![0; self.graph.node_count()];
        for (component_index, component) in components.iter().enumerate() {
            for node in component {
                node_to_component[node.index()] = component_index;
            }
        }

        // Contract every component into a single node, counting separately
        // how many components it has to be preceded by and how many it should be
        let mut dependents = vec![BTreeSet::<(usize, bool)>::new(); components.len()];
        let mut required_count = vec![0; components.len()];
        let mut preferred_count = vec![0; components.len()];

        for edge in self.graph.edge_references() {
            let source = node_to_component[edge.source().index()];
            let target = node_to_component[edge.target().index()];

            if source == target {
                if strong_edge_labels.contains(edge.weight()) {
                    return None;
                }

                continue;
            }

            let weak = weak_edge_labels.contains(edge.weight());

            if dependents[source].insert((target, weak)) {
                if weak {
                    preferred_count[target] += 1;
                } else {
                    required_count[target] += 1;
                }
            }
        }

        // The position of each component among the ones that may be emitted
        let priority = components
            .iter()
            .enumerate()
            .map(|(component_index, component)| {
                let position = component
                    .iter()
                    .map(|node| node.index())
                    .min()
                    .expect("components are not empty");

                (position, component_index)
            })
            .collect::<Vec<_>>();

        // Components all of whose dependencies have been emitted,
        let mut ready = BTreeSet::new();
        // and those that only wait for a component they merely prefer to follow
        let mut delayed = BTreeSet::new();

        for component_index in 0..components.len() {
            if required_count[component_index] == 0 {
                if preferred_count[component_index] == 0 {
                    ready.insert(priority[component_index]);
                } else {
                    delayed.insert(priority[component_index]);
                }
            }
        }

        let mut emitted = vec![false; components.len()];
        let mut result = Vec::with_capacity(components.len());

        while result.len() < components.len() {
            // Falling back to a delayed component gives up on its preferences,
            // which is what happens if they demand a cyclic order
            let (_, component_index) = ready
                .pop_first()
                .or_else(|| delayed.pop_first())
                .expect("ignoring the weak edges leaves an acyclic graph");

            emitted[component_index] = true;

            for &(dependent, weak) in &dependents[component_index] {
                if emitted[dependent] {
                    continue;
                }

                ready.remove(&priority[dependent]);
                delayed.remove(&priority[dependent]);

                if weak {
                    preferred_count[dependent] -= 1;
                } else {
                    required_count[dependent] -= 1;
                }

                if required_count[dependent] == 0 {
                    if preferred_count[dependent] == 0 {
                        ready.insert(priority[dependent]);
                    } else {
                        delayed.insert(priority[dependent]);
                    }
                }
            }

            result.push(
                components[component_index]
                    .iter()
                    .map(|&node| self.graph[node].clone())
                    .collect(),
            );
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
        Weak,
    }

    /// Compute the components of `graph`, sorting the nodes within each of them.
    ///
    /// The order of the nodes within a component is unspecified,
    /// while the order of the components themselves is what is under test.
    fn components(graph: &LabeledGraph<String, EdgeLabel, Directed>) -> Option<Vec<Vec<String>>> {
        graph
            .topological_components(&[EdgeLabel::Negative], &[EdgeLabel::Weak])
            .map(|components| {
                components
                    .into_iter()
                    .map(|mut component| {
                        component.sort();
                        component
                    })
                    .collect()
            })
    }

    #[test]
    fn topological_order() {
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

        // `B` and `C` lie on a common cycle and form a single component,
        // which the positive self loop on `C` does not split.
        assert_eq!(
            components(&graph),
            Some(vec![vec![node_a], vec![node_b, node_c], vec![node_d]])
        );
    }

    #[test]
    fn topological_order_independent_components() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");
        let node_c = String::from("C");
        let node_d = String::from("D");

        graph.add_edge(node_a.clone(), node_c.clone(), EdgeLabel::Negative);
        graph.add_edge(node_b.clone(), node_c.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_d.clone(), EdgeLabel::Positive);

        // `A` and `B` do not depend on each other,
        // so they are ordered by the node that was added first.
        assert_eq!(
            components(&graph),
            Some(vec![vec![node_a], vec![node_b], vec![node_c], vec![node_d]])
        );
    }

    #[test]
    fn topological_order_isolated_node() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");
        let node_c = String::from("C");

        graph.add_node(node_a.clone());
        graph.add_edge(node_b.clone(), node_c.clone(), EdgeLabel::Negative);

        // A node without any edge forms a component of its own,
        // which nothing constrains beyond having been added first.
        assert_eq!(
            components(&graph),
            Some(vec![vec![node_a], vec![node_b], vec![node_c]])
        );
    }

    #[test]
    fn weak_edge_orders_components() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");
        let node_c = String::from("C");

        graph.add_node(node_a.clone());
        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Positive);
        graph.add_edge(node_c.clone(), node_a.clone(), EdgeLabel::Weak);

        // `C` is placed before `A` even though `A` was added first,
        // while the dependency of `B` on `A` is respected as usual.
        assert_eq!(
            components(&graph),
            Some(vec![vec![node_c], vec![node_a], vec![node_b]])
        );
    }

    #[test]
    fn weak_edge_yields_to_dependency() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");

        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Positive);
        graph.add_edge(node_b.clone(), node_a.clone(), EdgeLabel::Weak);

        // The weak edge neither puts both nodes into one component
        // nor outweighs the dependency pointing the other way.
        assert_eq!(components(&graph), Some(vec![vec![node_a], vec![node_b]]));
    }

    #[test]
    fn weak_edge_cycle_is_ignored() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");

        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Weak);
        graph.add_edge(node_b.clone(), node_a.clone(), EdgeLabel::Weak);

        // Only one of the two preferences can be honoured,
        // and giving up on the other one is not an error.
        assert_eq!(components(&graph), Some(vec![vec![node_a], vec![node_b]]));
    }

    #[test]
    fn not_stratified() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");
        let node_b = String::from("B");

        graph.add_edge(node_a.clone(), node_b.clone(), EdgeLabel::Negative);
        graph.add_edge(node_b, node_a, EdgeLabel::Positive);

        assert_eq!(components(&graph), None);
    }

    #[test]
    fn not_stratified_self_loop() {
        let mut graph = LabeledGraph::<String, EdgeLabel, Directed>::default();

        let node_a = String::from("A");

        graph.add_edge(node_a.clone(), node_a, EdgeLabel::Negative);

        assert_eq!(components(&graph), None);
    }
}
