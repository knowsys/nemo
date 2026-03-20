//! This module contains basic data structures for tracing the origins of derived facts.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::Hash,
};

use ascii_tree::write_tree;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph_graphml::GraphMl;
use serde::Serialize;

use crate::{
    execution::{
        execution_engine::tracing::simple::storage::{
            ExecutionTrace, TraceDerivation, TraceFactHandle, TraceStatus,
        },
        planning::normalization::atom::ground::GroundAtom,
        tracing::resolve_origin::tracing_resolve_origin,
    },
    rule_model::{
        components::{fact::Fact, rule::Rule},
        substitution::Substitution,
    },
};

/// Trait implemented by tree structure of traces
/// that can be converted into the GraphML format
trait ToGraphMl {
    /// Return graphml representation of the content.
    fn to_graphml(&self) -> String;
}

impl ToGraphMl for DiGraph<TracePetGraphNodeLabel, ()> {
    fn to_graphml(&self) -> String {
        GraphMl::new(&self)
            .export_node_weights(Box::new(|node_label| match node_label {
                TracePetGraphNodeLabel::Fact(chase_fact) => vec![
                    (Cow::from("type"), Cow::from("axiom")),
                    (Cow::from("element"), Cow::from(chase_fact.to_string())),
                ],
                TracePetGraphNodeLabel::Rule(rule) => vec![
                    (Cow::from("type"), Cow::from("DLRule")),
                    (Cow::from("element"), Cow::from(rule.to_string())),
                ],
            }))
            .to_string()
    }
}

/// Represents the application of a rule to derive a specific fact
#[derive(Debug, Clone)]
pub struct TraceTreeRuleApplication {
    /// Rule that was applied
    pub rule: Rule,
    /// Index of the head atom which produced the fact under consideration
    pub head_index: usize,
    /// Variable assignment used during the rule application
    pub assignment: Substitution,
}

impl TraceTreeRuleApplication {
    /// Get the [Fact] that was produced by this rule application.
    fn to_derived_atom(&self) -> Fact {
        let mut fact = self.rule.head()[self.head_index].clone();
        self.assignment.apply(&mut fact);

        Fact::from(fact)
    }

    /// Get a string representation of the Instantiated rule.
    fn to_instantiated_string(&self) -> String {
        if let Some(display_string) = self.rule.instantiated_display(&self.assignment) {
            return display_string;
        }

        let mut rule_name_prefix = String::from("");
        if let Some(name) = &self.rule.name() {
            rule_name_prefix = format!("{}: ", name.clone());
        }

        let mut rule_instantiated = self.rule.clone();
        self.assignment.apply(&mut rule_instantiated);

        format!("{rule_name_prefix}{rule_instantiated}")
    }
}

/// Tree representation of an `ExecutionTrace` from a given start node
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ExecutionTraceTree {
    /// Node represent a fact in the initial data base
    Fact(GroundAtom),
    /// Node represents a derived fact
    Rule(TraceTreeRuleApplication, Vec<ExecutionTraceTree>),
}

impl ExecutionTraceTree {
    /// Return the number of nodes in this tree.
    pub fn node_count(&self) -> usize {
        match self {
            ExecutionTraceTree::Fact(_) => 1,
            ExecutionTraceTree::Rule(_, trees) => {
                1 + trees.iter().map(|tree| tree.node_count()).sum::<usize>()
            }
        }
    }
}

/// Type of labels for [DiGraph] representation of [ExecutionTrace]
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum TracePetGraphNodeLabel {
    /// Fact
    Fact(Fact),
    /// Application of a rule
    Rule(Rule),
}

impl ExecutionTraceTree {
    /// Create an ascii tree representation.
    fn to_ascii_tree(&self) -> ascii_tree::Tree {
        match self {
            Self::Fact(chase_fact) => ascii_tree::Tree::Leaf(vec![chase_fact.to_string()]),
            Self::Rule(trace_tree_rule_application, subtrees) => ascii_tree::Tree::Node(
                trace_tree_rule_application.to_instantiated_string(),
                subtrees
                    .iter()
                    .map(ExecutionTraceTree::to_ascii_tree)
                    .collect(),
            ),
        }
    }

    /// Create an ascii tree representation.
    pub fn to_ascii_art(&self) -> String {
        let mut result = String::new();
        let _ = write_tree(&mut result, &self.to_ascii_tree());

        result
    }

    /// Create a petgraph representation.
    fn to_petgraph(&self) -> DiGraph<TracePetGraphNodeLabel, ()> {
        let mut graph = DiGraph::new();

        let mut node_stack: Vec<(Option<NodeIndex>, Self)> = vec![(None, self.clone())];
        while let Some((parent_node_index_opt, next_node)) = node_stack.pop() {
            let next_node_index = match next_node {
                Self::Fact(ref chase_fact) => {
                    let next_node_index = graph
                        .add_node(TracePetGraphNodeLabel::Fact(Fact::from(chase_fact.clone())));
                    if let Some(parent_node_index) = parent_node_index_opt {
                        graph.add_edge(next_node_index, parent_node_index, ());
                    }
                    next_node_index
                }
                Self::Rule(ref trace_tree_rule_application, _) => {
                    let fact = trace_tree_rule_application.to_derived_atom();
                    let rule = trace_tree_rule_application.rule.clone();

                    let fact_node_index = graph.add_node(TracePetGraphNodeLabel::Fact(fact));
                    let next_node_index = graph.add_node(TracePetGraphNodeLabel::Rule(rule));
                    graph.add_edge(next_node_index, fact_node_index, ());

                    if let Some(parent_node_index) = parent_node_index_opt {
                        graph.add_edge(fact_node_index, parent_node_index, ());
                    }

                    next_node_index
                }
            };

            if let Self::Rule(_, subtrees) = next_node {
                let new_parent_node_index_opt = Some(next_node_index);
                let mut entries_to_append: Vec<(Option<NodeIndex>, Self)> = subtrees
                    .iter()
                    .cloned()
                    .map(|st| (new_parent_node_index_opt, st))
                    .collect();
                node_stack.append(&mut entries_to_append)
            }
        }

        graph
    }

    /// Return [ExecutionTraceTree] in [GraphML](http://graphml.graphdrawing.org/) format (for [Evonne](https://github.com/imldresden/evonne) integration)
    pub fn to_graphml(&self) -> String {
        self.to_petgraph().to_graphml()
    }
}

impl ExecutionTrace {
    /// Return a [ExecutionTraceTree] representation of an [ExecutionTrace]
    /// starting from a given fact.
    ///
    /// Return `None` if there is no trace for the given fact.
    pub fn tree(&self, fact_handle: TraceFactHandle) -> Option<ExecutionTraceTree> {
        let traced_fact = self.get_fact(fact_handle);

        if let TraceStatus::Success(derivation) = &traced_fact.status() {
            match derivation {
                TraceDerivation::Input => {
                    Some(ExecutionTraceTree::Fact(traced_fact.fact().clone()))
                }
                TraceDerivation::Derived(application, subderivations) => {
                    let mut subtrees = Vec::new();
                    for &derivation in subderivations {
                        if let Some(tree) = self.tree(derivation) {
                            subtrees.push(tree);
                        } else {
                            return None;
                        }
                    }

                    let rule = tracing_resolve_origin(self.program(), application.rule());

                    let tree_application = TraceTreeRuleApplication {
                        rule,
                        head_index: application.head_index(),
                        assignment: application.assignment().clone(),
                    };

                    Some(ExecutionTraceTree::Rule(tree_application, subtrees))
                }
            }
        } else {
            None
        }
    }
}

/// Inference in [ExecutionTraceListOfInferences]
#[derive(Debug)]
struct ExecutionTraceInference {
    /// Pair of [Rule] and [Substitution]
    trigger: Option<(Rule, Substitution)>,
    /// Result of the rule application
    conclusion: GroundAtom,
    /// Input facts to the rule
    premises: Vec<GroundAtom>,
}

/// Serializable version of [ExecutionTraceInference]
#[derive(Debug, Serialize)]
struct ExecutionTraceInferenceJSON {
    /// String representation of the logical rule
    #[serde(rename = "rule")]
    rule_string: String,
    /// Optionally provided name of the rule
    #[serde(rename = "ruleName", skip_serializing_if = "Option::is_none")]
    rule_name: Option<String>,
    /// Optional human readable string representation of a rule instance
    #[serde(rename = "ruleDisplay", skip_serializing_if = "Option::is_none")]
    rule_display: Option<String>,
    /// Instantiated result of the rule application
    conclusion: String,
    /// Premises enabling the rule application
    premises: Vec<String>,
}

const DEFAULT_DERIVATION_ANNOTATION: &str = "Asserted";

impl From<ExecutionTraceInference> for ExecutionTraceInferenceJSON {
    fn from(value: ExecutionTraceInference) -> Self {
        Self {
            rule_string: value
                .trigger
                .as_ref()
                .map(|(rule, _)| rule.to_string())
                .unwrap_or(DEFAULT_DERIVATION_ANNOTATION.to_string()),
            rule_name: value.trigger.as_ref().and_then(|(rule, _)| rule.name()),
            rule_display: value
                .trigger
                .as_ref()
                .and_then(|(rule, substitution)| rule.instantiated_display(substitution)),
            conclusion: value.conclusion.to_string(),
            premises: value
                .premises
                .into_iter()
                .map(|premise| premise.to_string())
                .collect(),
        }
    }
}

impl ExecutionTraceInference {
    /// Create a new [ExecutionTraceInference]
    pub fn new(
        trigger: Option<(Rule, Substitution)>,
        conclusion: GroundAtom,
        premises: Vec<GroundAtom>,
    ) -> Self {
        Self {
            trigger,
            conclusion,
            premises,
        }
    }
}

/// Representation of an [ExecutionTrace] as a list of inferences
#[derive(Debug)]
struct ExecutionTraceListOfInferences {
    /// The list of conclusions for which the trace is computed
    final_conclusions: Vec<GroundAtom>,

    /// List of inferences which result in the `final_conclusions`
    inferences: Vec<ExecutionTraceInference>,
}

/// Serializable representation of an `ExecutionTrace` as a list of inferences
#[derive(Debug, Serialize, Default)]
pub struct ExecutionTraceListOfInferencesJSON {
    /// The list of conclusions for which the trace is computed
    #[serde(rename = "finalConclusion")]
    final_conclusions: Vec<String>,

    /// List of inferences which result in the `final_conclusions`
    inferences: Vec<ExecutionTraceInferenceJSON>,
}

impl From<ExecutionTraceListOfInferences> for ExecutionTraceListOfInferencesJSON {
    fn from(value: ExecutionTraceListOfInferences) -> Self {
        Self {
            final_conclusions: value
                .final_conclusions
                .into_iter()
                .map(|concl| concl.to_string())
                .collect(),
            inferences: value
                .inferences
                .into_iter()
                .map(ExecutionTraceInferenceJSON::from)
                .collect(),
        }
    }
}

impl ExecutionTrace {
    /// Translate an [TraceDerivation] into an [ExecutionTraceInference].
    fn inference_from_derivation(
        &self,
        derivation: &TraceDerivation,
        conclusion: &GroundAtom,
    ) -> ExecutionTraceInference {
        match derivation {
            TraceDerivation::Input => {
                ExecutionTraceInference::new(None, conclusion.clone(), vec![])
            }
            TraceDerivation::Derived(application, premises_handles) => {
                let rule = tracing_resolve_origin(self.program(), application.rule());

                let premises = premises_handles
                    .iter()
                    .map(|&handle| self.get_fact(handle).fact().clone())
                    .collect();

                ExecutionTraceInference::new(
                    Some((rule, application.assignment().clone())),
                    conclusion.clone(),
                    premises,
                )
            }
        }
    }

    /// Represent the [ExecutionTrace] as a list of inferences.
    fn list_of_inferences(
        &self,
        fact_handles: &[TraceFactHandle],
    ) -> ExecutionTraceListOfInferences {
        let mut final_conclusions = Vec::<GroundAtom>::new();
        let mut fact_stack = fact_handles.to_vec();
        let mut inferences = Vec::<ExecutionTraceInference>::new();
        let mut inferred_conclusions = HashSet::<TraceFactHandle>::new();

        for &final_handle in fact_handles {
            let mut successful_derivation = true;
            fact_stack.push(final_handle);

            while let Some(current_fact) = fact_stack.pop() {
                if inferred_conclusions.contains(&current_fact) {
                    continue;
                }
                inferred_conclusions.insert(current_fact);

                let traced_fact = self.get_fact(current_fact);

                if let TraceStatus::Success(derivation) = &traced_fact.status() {
                    let inference = self.inference_from_derivation(derivation, traced_fact.fact());
                    inferences.push(inference);

                    if let TraceDerivation::Derived(_, handles_derived) = derivation {
                        fact_stack.extend(handles_derived);
                    }
                } else {
                    successful_derivation = false;
                    break;
                }
            }

            if successful_derivation {
                final_conclusions.push(self.get_fact(final_handle).fact().clone());
            }
        }

        ExecutionTraceListOfInferences {
            final_conclusions,
            inferences,
        }
    }

    /// Create a json representation of the trace.
    pub fn json(&self, fact_handles: &[TraceFactHandle]) -> ExecutionTraceListOfInferencesJSON {
        ExecutionTraceListOfInferencesJSON::from(self.list_of_inferences(fact_handles))
    }

    /// Return [`ExecutionTrace`] in [GraphML](http://graphml.graphdrawing.org/) format as DAG (for [Evonne](https://github.com/imldresden/evonne) integration)
    pub fn graphml(&self, fact_handles: &[TraceFactHandle]) -> String {
        self.list_of_inferences(fact_handles).to_graphml()
    }
}

impl ExecutionTraceListOfInferences {
    fn to_petgraph(&self) -> DiGraph<TracePetGraphNodeLabel, ()> {
        let mut graph = DiGraph::new();
        let mut label_to_node_index: HashMap<TracePetGraphNodeLabel, NodeIndex> = HashMap::new();

        self.inferences.iter().for_each(|inference| {
            let conclusion = &inference.conclusion;
            let trigger_opt = &inference.trigger;
            let premises = &inference.premises;

            let conclusion_label = TracePetGraphNodeLabel::Fact(Fact::from(conclusion.clone()));
            let conclusion_node_index = label_to_node_index
                .entry(conclusion_label.clone())
                .or_insert_with(|| graph.add_node(conclusion_label));

            if let Some((rule, _substitution)) = trigger_opt {
                let rule_node_index = graph.add_node(TracePetGraphNodeLabel::Rule(rule.clone()));
                graph.add_edge(rule_node_index, *conclusion_node_index, ());

                premises.iter().for_each(|premise| {
                    let premise_label = TracePetGraphNodeLabel::Fact(Fact::from(premise.clone()));
                    let premise_node_index = label_to_node_index
                        .entry(premise_label.clone())
                        .or_insert_with(|| graph.add_node(premise_label));

                    graph.add_edge(*premise_node_index, rule_node_index, ());
                })
            }
        });

        graph
    }

    fn to_graphml(&self) -> String {
        self.to_petgraph().to_graphml()
    }
}

#[cfg(test)]
mod test {
    use nemo_physical::datavalues::AnyDataValue;

    use crate::{
        execution::{
            execution_engine::tracing::simple::storage::TraceRuleApplication,
            planning::normalization::atom::ground::GroundAtom,
            tracing::trace::{TraceDerivation, TraceStatus},
        },
        rule_model::{
            components::{
                ComponentIdentity,
                atom::Atom,
                rule::Rule,
                term::primitive::{Primitive, variable::Variable},
            },
            error::ValidationReport,
            pipeline::{ProgramPipeline, commit::ProgramCommit},
            programs::{ProgramRead, ProgramWrite},
            substitution::Substitution,
            translation::TranslationComponent,
        },
    };

    use super::ExecutionTrace;

    macro_rules! variable_assignment {
        ($($k:expr => $v:expr),*) => {{
            let terms = [$(($k, $v)),*]
            .into_iter()
            .map(|(k, v)| {
                (
                    Primitive::from(Variable::universal(k)),
                    Primitive::from(AnyDataValue::new_iri(v.to_string())),
                )
            });

            Substitution::new(terms)
        }};
    }

    fn test_trace() -> ExecutionTrace {
        let rule_1 = Rule::parse("P(?x, ?y) :- Q(?y, ?x)").unwrap();
        let rule_1_assignment = variable_assignment!("x" => "b", "y" => "a");

        let rule_2 = Rule::parse("S(?x) :- T(?x)").unwrap();
        let rule_2_assignment = variable_assignment!("x" => "a");

        let rule_3 = Rule::parse("R(?x, ?y) :- P(?x, ?y), S(?y)").unwrap();
        let rule_3_assignment = variable_assignment!("x" => "b", "y" => "a");

        let q_ab = GroundAtom::try_from(Atom::parse("Q(a,b)").unwrap()).unwrap();
        let p_ba = GroundAtom::try_from(Atom::parse("P(b,a)").unwrap()).unwrap();
        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();
        let s_a = GroundAtom::try_from(Atom::parse("S(a)").unwrap()).unwrap();
        let t_a = GroundAtom::try_from(Atom::parse("T(a)").unwrap()).unwrap();

        let mut commit = ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());
        commit.add_rule(rule_1);
        commit.add_rule(rule_2);
        commit.add_rule(rule_3);

        let program = commit.submit().unwrap();
        let mut program_rules = program.rules();

        let rule_1_id = program_rules.next().unwrap().id();
        let rule_2_id = program_rules.next().unwrap().id();
        let rule_3_id = program_rules.next().unwrap().id();

        drop(program_rules);

        let mut trace = ExecutionTrace::new(program);

        let trace_s_a = trace.register_fact(s_a);
        let trace_t_a = trace.register_fact(t_a);
        let trace_q_ab = trace.register_fact(q_ab);
        let trace_p_ba = trace.register_fact(p_ba);
        let trace_r_ba = trace.register_fact(r_ba);

        trace.update_status(trace_t_a, TraceStatus::Success(TraceDerivation::Input));
        trace.update_status(
            trace_s_a,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_2_id, rule_2_assignment, 0),
                vec![trace_t_a],
            )),
        );
        trace.update_status(trace_q_ab, TraceStatus::Success(TraceDerivation::Input));
        trace.update_status(
            trace_p_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_1_id, rule_1_assignment, 0),
                vec![trace_q_ab],
            )),
        );
        trace.update_status(
            trace_r_ba,
            TraceStatus::Success(TraceDerivation::Derived(
                TraceRuleApplication::new(rule_3_id, rule_3_assignment, 0),
                vec![trace_p_ba, trace_s_a],
            )),
        );

        trace
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn trace_ascii() {
        let trace = test_trace();
        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();

        let trace_r_ba = trace.find_fact(&r_ba).unwrap();

        let trace_string = r#" R(b, a) :- P(b, a), S(a) .
 ├─ P(b, a) :- Q(a, b) .
 │  └─ Q(a, b)
 └─ S(a) :- T(a) .
    └─ T(a)
"#;

        assert_eq!(
            trace.tree(trace_r_ba).unwrap().to_ascii_art(),
            trace_string.to_string()
        )
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn trace_json() {
        let trace = test_trace();

        let r_ba = GroundAtom::try_from(Atom::parse("R(b,a)").unwrap()).unwrap();
        let p_ba = GroundAtom::try_from(Atom::parse("P(b,a)").unwrap()).unwrap();

        let trace_r_ba = trace.find_fact(&r_ba).unwrap();
        let trace_p_ba = trace.find_fact(&p_ba).unwrap();
        let trace_handles = vec![trace_r_ba, trace_p_ba];

        let expected_json = r#"{"finalConclusion":["R(b, a)","P(b, a)"],"inferences":[{"rule":"R(?x, ?y) :- P(?x, ?y), S(?y) .","conclusion":"R(b, a)","premises":["P(b, a)","S(a)"]},{"rule":"S(?x) :- T(?x) .","conclusion":"S(a)","premises":["T(a)"]},{"rule":"Asserted","conclusion":"T(a)","premises":[]},{"rule":"P(?x, ?y) :- Q(?y, ?x) .","conclusion":"P(b, a)","premises":["Q(a, b)"]},{"rule":"Asserted","conclusion":"Q(a, b)","premises":[]}]}"#.to_string();
        let computed_json = serde_json::to_string(&trace.json(&trace_handles)).unwrap();

        assert_eq!(expected_json, computed_json);
    }
}
