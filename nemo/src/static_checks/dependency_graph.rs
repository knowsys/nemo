use crate::rule_model::components::tag::Tag;
use crate::static_checks::{positions::Positions, rule_set::RuleSet};
use petgraph::graph::Graph;
use std::collections::HashSet;

#[derive(Clone, Eq, Hash, PartialEq)]
struct Position<'a>(&'a Tag, usize);

struct ExtendedPositions<'a>(HashSet<Position<'a>>);

impl<'a> ExtendedPositions<'a> {
    pub fn new() -> Self {
        ExtendedPositions(HashSet::<Position>::new())
    }
}

// FIXME: EDGETYPECHECKS CAN BE CONFUSED WITH PETGRAPH::EDGETYPE
enum EdgeTypeChecks {
    CommonEdge,
    SpeciaEdge,
}

// FIXME: DEPENDENCYGRAPHCHECKS CAN BE CONFUSED WITH
// CRATE::EXECUTION::SELECTION_STRATEGY::DEPENDENCY_GRAPH::GRAPH_CONSTRUCTOR::DEPENDENCYGRAPH
type DependencyGraphChecks<'a> = Graph<Position<'a>, EdgeTypeChecks>;

trait DependencyGraphChecksConstructor {
    fn build_graph(&self) -> DependencyGraphChecks;
}

impl DependencyGraphChecksConstructor for RuleSet {
    fn build_graph(&self) -> DependencyGraphChecks {
        let all_positions: ExtendedPositions =
            ExtendedPositions::from(&self.all_positive_positions());
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
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
