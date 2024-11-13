use crate::rule_model::components::tag::Tag;
use crate::static_checks::{positions::Positions, rule_set::RuleSet};
use petgraph::{graphmap::GraphMap, Directed};
use std::collections::{
    hash_set::{IntoIter, Iter},
    HashSet,
};

#[derive(Clone, Copy, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct Position<'a>(&'a Tag, usize);

struct ExtendedPositions<'a>(HashSet<Position<'a>>);

impl<'a> ExtendedPositions<'a> {
    pub fn new() -> Self {
        ExtendedPositions(HashSet::<Position>::new())
    }

    fn into_iter(self) -> IntoIter<Position<'a>> {
        self.0.into_iter()
    }

    // fn iter(&self) -> Iter<Position> {
    //     self.0.iter()
    // }
}

enum WeaklyAcyclicityGraphEdgeType {
    CommonEdge,
    SpeciaEdge,
}

type WeaklyAcyclicityGraph<'a> = GraphMap<Position<'a>, WeaklyAcyclicityGraphEdgeType, Directed>;

trait WeaklyAcyclicityGraphBuilder<'a> {
    fn add_edges(&mut self, rule_set: &RuleSet);
    fn add_nodes(&'a mut self, all_positive_extended_positions: ExtendedPositions<'a>);
}

impl<'a> WeaklyAcyclicityGraphBuilder<'a> for WeaklyAcyclicityGraph<'a> {
    fn add_edges(&mut self, rule_set: &RuleSet) {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn add_nodes(&'a mut self, all_positive_extended_positions: ExtendedPositions<'a>) {
        all_positive_extended_positions.into_iter().for_each(|pos| {
            self.add_node(pos);
        });
    }
}

trait WeaklyAcyclicityGraphConstructor {
    fn build_graph(&self) -> WeaklyAcyclicityGraph;
}

impl WeaklyAcyclicityGraphConstructor for RuleSet {
    fn build_graph(&self) -> WeaklyAcyclicityGraph {
        let all_positive_positions: Positions = self.all_positive_positions();
        let all_positions_extended_positions: ExtendedPositions =
            ExtendedPositions::from(&all_positive_positions);
        let mut we_ac_graph: WeaklyAcyclicityGraph = WeaklyAcyclicityGraph::new();
        // we_ac_graph.add_nodes(all_positions_extended_positions);
        // we_ac_graph.add_edges(self);
        we_ac_graph
    }
}

// TODO: MAYBE SHORTEN FUNCTION
impl<'a> From<&'a Positions> for ExtendedPositions<'a> {
    fn from(positions: &'a Positions) -> Self {
        ExtendedPositions(positions.iter().fold(
            HashSet::<Position>::new(),
            |ex_pos, (pred, indices): (&'a Tag, _)| {
                ex_pos
                    .union(&indices.iter().map(|index| Position(pred, *index)).collect())
                    .cloned()
                    .collect()
            },
        ))
    }
}
